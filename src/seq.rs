use super::{Executor, ExecutorNewError, ExecutorJobError, Job, JobExecuteError, Reduce, ThreadContextBuilder};

#[derive(Debug)]
pub enum Error {
    NotInitialized,
}

pub struct SequentalExecutor<TC> {
    thread_context: Option<TC>,
}

impl<TC> SequentalExecutor<TC> {
    pub fn new() -> SequentalExecutor<TC> {
        SequentalExecutor {
            thread_context: None,
        }
    }
}

impl<TC> Executor for SequentalExecutor<TC> {
    type TC = TC;
    type E = Error;

    fn run<TCB, TCBE>(self, mut thread_context_builder: TCB) -> Result<Self, ExecutorNewError<Self::E, TCBE>>
        where TCB: ThreadContextBuilder<TC = Self::TC, E = TCBE>
    {
        let maybe_thread_context = thread_context_builder
            .make_thread_context()
            .map_err(|e| ExecutorNewError::ThreadContextBuilder(e));
        Ok(SequentalExecutor {
            thread_context: Some(try!(maybe_thread_context)),
        })
    }

    fn execute_job<J, JR, JE>(&mut self, input_size: usize, job: J) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, JR::E>>>
        where J: Job<TC = Self::TC, R = JR, E = JE>, JR: Reduce, JE: Send + 'static
    {
        if let Some(thread_context) = self.thread_context.as_mut() {
            job.execute(thread_context, 0 .. input_size)
                .map(|v| Some(v))
                .map_err(|e| ExecutorJobError::Job(e))
        } else {
            Err(ExecutorJobError::Executor(Error::NotInitialized))
        }
    }
}
