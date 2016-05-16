use std::io;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::collections::BinaryHeap;
use num_cpus;
use super::{Executor, ExecutorNewError, ExecutorJobError, Job, JobExecuteError, Reduce, ThreadContextBuilder};

#[derive(Debug)]
pub enum Error {
    NotInitialized,
    SlaveError(io::Error),
    UnexpectedSlaveReport,
    UnexpectedReduceResult,
}

enum Command<TC> {
    Job(Box<Fn(&mut TC) + Send>),
    Stop,
}

enum Report {
    JobComplete,
    Stopped,
}

struct Slave<TC> {
    thread: Option<thread::JoinHandle<()>>,
    tx: Sender<Command<TC>>,
    rx: Receiver<Report>,
}

impl<TC> Slave<TC> where TC: Send + 'static {
    fn spawn(slave_id: usize, thread_context: TC) -> Result<Slave<TC>, io::Error> {
        let (master_tx, slave_rx) = channel();
        let (slave_tx, master_rx) = channel();
        let maybe_thread = thread::Builder::new()
            .name(format!("gen_lsb parallel executor slave #{}", slave_id))
            .spawn(move || slave_loop(thread_context, slave_rx, slave_tx));
        Ok(Slave {
            thread: Some(try!(maybe_thread)),
            tx: master_tx,
            rx: master_rx,
        })
    }
}

impl<TC> Drop for Slave<TC> {
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

fn slave_loop<TC>(mut thread_context: TC, rx: Receiver<Command<TC>>, tx: Sender<Report>) {
    loop {
        match rx.recv().unwrap() {
            Command::Stop => {
                tx.send(Report::Stopped).unwrap();
                break;
            },
            Command::Job(job) => {
                job(&mut thread_context);
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

pub struct ParallelExecutor<TC> {
    slaves_count: usize,
    slaves: Vec<Slave<TC>>,
}

impl<TC> ParallelExecutor<TC> {
    pub fn new(slaves_count: usize) -> ParallelExecutor<TC> {
        ParallelExecutor {
            slaves_count: slaves_count,
            slaves: Vec::new(),
        }
    }
}

impl<TC> Default for ParallelExecutor<TC> {
    fn default() -> ParallelExecutor<TC> {
        ParallelExecutor::new(num_cpus::get())
    }
}

impl<TC> Executor for ParallelExecutor<TC> where TC: Send + 'static {
    type TC = TC;
    type E = Error;

    fn run<TCB, TCBE>(mut self, thread_context_builder: TCB) -> Result<Self, ExecutorNewError<Self::E, TCBE>>
        where TCB: ThreadContextBuilder<TC = Self::TC, E = TCBE>
    {
        for i in 0 .. self.slaves_count {
            let thread_context =
                try!(thread_context_builder.make_thread_context().map_err(|e| ExecutorNewError::ThreadContextBuilder(e)));
            let slave =
                try!(Slave::spawn(i, thread_context).map_err(|e| ExecutorNewError::Executor(Error::SlaveError(e))));
            self.slaves.push(slave);
        }
        Ok(self)
    }

    fn execute_job<J, JR, JE>(&mut self, input_size: usize, job: J) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, JR::E>>>
        where J: Job<TC = Self::TC, R = JR, E = JE>, JR: Reduce, JE: Send + 'static
    {
        struct ReduceItem<JR>(JR);

        impl<JR> Eq for ReduceItem<JR> where JR: Reduce { }

        impl<JR> PartialEq for ReduceItem<JR> where JR: Reduce {
            fn eq(&self, other: &Self) -> bool {
                self.0.len().eq(&other.0.len())
            }
        }

        impl<JR> Ord for ReduceItem<JR> where JR: Reduce {
            fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
                other.0.len().cmp(&self.0.len())
            }
        }

        impl<JR> PartialOrd for ReduceItem<JR> where JR: Reduce {
            fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        let reduce_result = Arc::new(Mutex::new(BinaryHeap::new()));
        let errors: Arc<Mutex<Vec<ExecutorJobError<Error, JobExecuteError<JE, JR::E>>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let arc_job = Arc::new(job);
        let sync_iter = Arc::new(AtomicUsize::new(0));

        for slave in self.slaves.iter() {
            let local_job = arc_job.clone();
            let local_reduce_result = reduce_result.clone();
            let local_errors = errors.clone();
            let local_sync_iter = sync_iter.clone();
            slave.tx.send(Command::Job(Box::new(move |thread_context| {
                // execute job
                match local_job.execute(thread_context, SyncIter::new(local_sync_iter.clone(), input_size)) {
                    Ok(mut current_result) =>
                        // reduce result
                        loop {
                            local_reduce_result.lock().unwrap().push(ReduceItem(current_result));
                            let (ReduceItem(result_a), ReduceItem(result_b)) = {
                                let mut result_lock = local_reduce_result.lock().unwrap();
                                if result_lock.len() >= 2 {
                                    (result_lock.pop().unwrap(), result_lock.pop().unwrap())
                                } else {
                                    break
                                }
                            };
                            match result_b.reduce(result_a) {
                                Ok(result) =>
                                    current_result = result,
                                Err(reduce_err) => {
                                    local_errors.lock().unwrap().push(ExecutorJobError::Job(JobExecuteError::Result(reduce_err)));
                                    break;
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
            if let Some(ReduceItem(result)) = result_lock.pop() {
                Ok(Some(result))
            } else {
                Err(ExecutorJobError::Executor(Error::UnexpectedReduceResult))
            }
        } else {
            Err(ExecutorJobError::Executor(Error::UnexpectedReduceResult))
        }
    }
}
