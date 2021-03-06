use std::io;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use num_cpus;
use super::{Executor, LocalContextBuilder, ExecutorNewError, ExecutorJobError, JobExecuteError, WorkAmount, JobIterBuild};

#[derive(Debug)]
pub enum Error {
    NotInitialized,
    AlreadyInitialized,
    SlaveError(io::Error),
    UnexpectedSlaveReport,
    UnexpectedReduceResult,
}

enum Command<LC> {
    Job(Arc<Box<Fn(&mut LC) + Sync + Send + 'static>>),
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

pub struct IterBuild {
    slaves_count: usize,
    sync: Arc<AtomicUsize>,
}

impl IterBuild {
    fn new(slaves_count: usize) -> IterBuild {
        IterBuild {
            slaves_count: slaves_count,
            sync: Arc::new(AtomicUsize::new(0)),
        }
    }
}

pub struct Alternately(usize);

impl WorkAmount for Alternately {
    type IT = SyncIter;

    fn new(work_amount: usize) -> Alternately {
        Alternately(work_amount)
    }
}

impl JobIterBuild<Alternately> for IterBuild {
    fn build(&self, &Alternately(work_amount): &Alternately) -> SyncIter {
        SyncIter {
            counter: self.sync.clone(),
            limit: work_amount,
        }
    }
}

pub struct SyncIter {
    counter: Arc<AtomicUsize>,
    limit: usize,
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

pub struct ByEqualChunks(usize);

impl WorkAmount for ByEqualChunks {
    type IT = ::std::ops::Range<usize>;

    fn new(work_amount: usize) -> ByEqualChunks {
        ByEqualChunks(work_amount)
    }
}

impl JobIterBuild<ByEqualChunks> for IterBuild {
    fn build(&self, &ByEqualChunks(work_amount): &ByEqualChunks) -> <ByEqualChunks as WorkAmount>::IT {
        let chunk = work_amount / self.slaves_count;
        let current_slave = self.sync.fetch_add(1, Ordering::Relaxed);
        let start = current_slave * chunk;
        let end = if current_slave < self.slaves_count - 1 {
            start + chunk
        } else {
            work_amount
        };
        start .. end
    }
}

impl<LC> Executor for ParallelExecutor<LC> where LC: Send + 'static {
    type LC = LC;
    type E = Error;
    type JIB = IterBuild;

    fn try_start<LCB>(mut self, mut local_context_builder: LCB) -> Result<Self, ExecutorNewError<Self::E, LCB::E>>
        where LCB: LocalContextBuilder<LC = Self::LC>
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

    fn try_execute_job<WA, JF, JR, RF, JE, RE>(&mut self, work_amount: WA, map: JF, reduce: RF) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, RE>>> where
        WA: WorkAmount,
        Self::JIB: JobIterBuild<WA>,
        JF: Fn(&mut Self::LC, WA::IT) -> Result<JR, JE> + Sync + Send + 'static,
        RF: Fn(&mut Self::LC, JR, JR) -> Result<JR, RE> + Sync + Send + 'static,
        JR: Send + 'static,
        JE: Send + 'static,
        RE: Send + 'static
    {
        let reduce_result = Arc::new(Mutex::new(None));
        let errors: Arc<Mutex<Vec<ExecutorJobError<Error, JobExecuteError<JE, RE>>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let iter_build = IterBuild::new(self.slaves_count);

        let local_reduce_result = reduce_result.clone();
        let local_errors = errors.clone();
        let worker_fn: Arc<Box<Fn(&mut LC) + Sync + Send + 'static>> = Arc::new(Box::new(move |ref mut local_context| {
            // map
            match map(local_context, iter_build.build(&work_amount)) {
                Ok(mut current_result) =>
                    // reduce
                    loop {
                        let existing_result = {
                            let mut result_lock = local_reduce_result.lock().unwrap();
                            if let Some(arg) = result_lock.take() {
                                arg
                            } else {
                                *result_lock = Some(current_result);
                                break;
                            }
                        };
                        match reduce(local_context, current_result, existing_result) {
                            Ok(result) =>
                                current_result = result,
                            Err(reduce_err) => {
                                local_errors.lock().unwrap().push(ExecutorJobError::Job(JobExecuteError::Reducer(reduce_err)));
                                break;
                            }
                        }
                    },
                Err(job_err) =>
                    local_errors.lock().unwrap().push(ExecutorJobError::Job(JobExecuteError::Job(job_err))),
            }
        }));

        for slave in self.slaves.iter() {
            slave.tx.send(Command::Job(worker_fn.clone())).unwrap();
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
        Ok(result_lock.take())
    }
}
