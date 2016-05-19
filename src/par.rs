use std::io;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::collections::BinaryHeap;
use num_cpus;
use super::{Executor, ExecutorNewError, ExecutorJobError, Job, JobExecuteError};
use super::{Reducer, LocalContextBuilder};

#[derive(Debug)]
pub enum Error {
    NotInitialized,
    AlreadyInitialized,
    SlaveError(io::Error),
    UnexpectedSlaveReport,
    UnexpectedReduceResult,
}

enum Command<LC> {
    Job(Box<Fn(&mut LC) + Send>),
    Stop,
}

enum Report {
    JobComplete,
    Stopped,
}

struct Slave<LC> {
    thread: Option<thread::JoinHandle<()>>,
    tx: Sender<Command<LC>>,
    rx: Receiver<Report>,
}

impl<LC> Slave<LC> where LC: Send + 'static {
    fn spawn(slave_id: usize, local_context: LC) -> Result<Slave<LC>, io::Error> {
        let (master_tx, slave_rx) = channel();
        let (slave_tx, master_rx) = channel();
        let maybe_thread = thread::Builder::new()
            .name(format!("gen_lsb parallel executor slave #{}", slave_id))
            .spawn(move || slave_loop(local_context, slave_rx, slave_tx));
        Ok(Slave {
            thread: Some(try!(maybe_thread)),
            tx: master_tx,
            rx: master_rx,
        })
    }
}

impl<LC> Drop for Slave<LC> {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.tx.send(Command::Stop).unwrap();
            loop {
                match self.rx.recv().unwrap() {
                    Report::Stopped => {
                        thread.join().unwrap();
                        break;
                    },
                    _ =>
                        unreachable!(),
                }
            }
        }
    }
}

fn slave_loop<LC>(mut local_context: LC, rx: Receiver<Command<LC>>, tx: Sender<Report>) {
    loop {
        match rx.recv().unwrap() {
            Command::Stop => {
                tx.send(Report::Stopped).unwrap();
                break;
            },
            Command::Job(job) => {
                job(&mut local_context);
                tx.send(Report::JobComplete).unwrap();
            },
        }
    }
}

struct SyncIter {
    counter: Arc<AtomicUsize>,
    limit: usize,
}

impl SyncIter {
    fn new(counter: Arc<AtomicUsize>, limit: usize) -> SyncIter {
        SyncIter {
            counter: counter,
            limit: limit,
        }
    }
}

impl Iterator for SyncIter {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        let current = self.counter.fetch_add(1, Ordering::Relaxed);
        if current < self.limit {
            Some(current)
        } else {
            None
        }
    }
}

pub struct ParallelExecutor<LC> {
    slaves_count: usize,
    slaves: Vec<Slave<LC>>,
}

impl<LC> ParallelExecutor<LC> {
    pub fn new(slaves_count: usize) -> ParallelExecutor<LC> {
        ParallelExecutor {
            slaves_count: slaves_count,
            slaves: Vec::new(),
        }
    }
}

impl<LC> Default for ParallelExecutor<LC> {
    fn default() -> ParallelExecutor<LC> {
        ParallelExecutor::new(num_cpus::get())
    }
}

impl<LC> Executor for ParallelExecutor<LC> where LC: Send + 'static {
    type LC = LC;
    type E = Error;

    fn start<LCB, LCBE>(mut self, mut local_context_builder: LCB) -> Result<Self, ExecutorNewError<Self::E, LCBE>>
        where LCB: LocalContextBuilder<LC = Self::LC, E = LCBE>
    {
        if !self.slaves.is_empty() {
            return Err(ExecutorNewError::Executor(Error::AlreadyInitialized));
        }

        for i in 0 .. self.slaves_count {
            let local_context =
                try!(local_context_builder.make_local_context().map_err(|e| ExecutorNewError::LocalContextBuilder(e)));
            let slave =
                try!(Slave::spawn(i, local_context).map_err(|e| ExecutorNewError::Executor(Error::SlaveError(e))));
            self.slaves.push(slave);
        }
        Ok(self)
    }

    fn execute_job<J, JR, JRR, JE, JRE>(&mut self, input_size: usize, job: J) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, JRE>>> where
        J: Job<LC = Self::LC, R = JR, RR = JRR, E = JE> + Sync + Send + 'static,
        JRR: Reducer<R = JR, E = JRE>,
        JR: Send + 'static,
        JE: Send + 'static,
        JRE: Send + 'static
    {
        struct ReduceItem<JR>(JR, Option<usize>);

        impl<JR> Eq for ReduceItem<JR> { }

        impl<JR> PartialEq for ReduceItem<JR> {
            fn eq(&self, other: &Self) -> bool {
                self.1.eq(&other.1)
            }
        }

        impl<JR> Ord for ReduceItem<JR> {
            fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
                other.1.cmp(&self.1)
            }
        }

        impl<JR> PartialOrd for ReduceItem<JR> {
            fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        let reduce_result = Arc::new(Mutex::new(BinaryHeap::new()));
        let errors: Arc<Mutex<Vec<ExecutorJobError<Error, JobExecuteError<JE, JRE>>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let arc_job = Arc::new(job);
        let sync_iter = Arc::new(AtomicUsize::new(0));

        for slave in self.slaves.iter() {
            let local_job = arc_job.clone();
            let local_reduce_result = reduce_result.clone();
            let local_errors = errors.clone();
            let local_sync_iter = sync_iter.clone();
            slave.tx.send(Command::Job(Box::new(move |local_context| {
                // execute job
                match local_job.execute(local_context, SyncIter::new(local_sync_iter.clone(), input_size)) {
                    Ok(mut current_result) => {
                        // reduce result
                        let mut reducer = match local_job.reducer(local_context) {
                            Ok(r) => r,
                            Err(e) => {
                                local_errors.lock().unwrap().push(ExecutorJobError::Job(JobExecuteError::Job(e)));
                                return;
                            },
                        };
                        loop {
                            let current_result_len = reducer.len(&current_result);
                            local_reduce_result.lock().unwrap().push(ReduceItem(current_result, current_result_len));
                            let (ReduceItem(result_a, _), ReduceItem(result_b, _)) = {
                                let mut result_lock = local_reduce_result.lock().unwrap();
                                if result_lock.len() >= 2 {
                                    (result_lock.pop().unwrap(), result_lock.pop().unwrap())
                                } else {
                                    break
                                }
                            };
                            match reducer.reduce(result_a, result_b) {
                                Ok(result) =>
                                    current_result = result,
                                Err(reduce_err) => {
                                    local_errors.lock().unwrap().push(ExecutorJobError::Job(JobExecuteError::Reducer(reduce_err)));
                                    break;
                                }
                            }
                        }
                    },
                    Err(job_err) =>
                        local_errors.lock().unwrap().push(ExecutorJobError::Job(job_err)),
                }
            }))).unwrap();
        }

        for slave in self.slaves.iter() {
            match slave.rx.recv().unwrap() {
                Report::JobComplete =>
                    (),
                Report::Stopped =>
                    errors.lock().unwrap().push(ExecutorJobError::Executor(Error::UnexpectedSlaveReport)),
            }
        }

        let mut errors_lock = errors.lock().unwrap();
        if errors_lock.len() == 1 {
            return Err(errors_lock.pop().unwrap());
        } else if errors_lock.len() > 1 {
            return Err(ExecutorJobError::Several(errors_lock.drain(..).collect()));
        }

        let mut result_lock = reduce_result.lock().unwrap();
        if result_lock.len() == 0 {
            Ok(None)
        } else if result_lock.len() == 1 {
            if let Some(ReduceItem(result, _)) = result_lock.pop() {
                Ok(Some(result))
            } else {
                Err(ExecutorJobError::Executor(Error::UnexpectedReduceResult))
            }
        } else {
            Err(ExecutorJobError::Executor(Error::UnexpectedReduceResult))
        }
    }
}
