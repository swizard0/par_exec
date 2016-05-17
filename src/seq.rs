use super::{Executor, ExecutorNewError, ExecutorJobError, Job, JobExecuteError};
use super::{Reducer, ReducerRetrieve, LocalContextBuilder};

#[derive(Debug)]
pub enum Error {
    NotInitialized,
}

pub struct SequentalExecutor<LC> {
    local_context: Option<LC>,
}

impl<LC> SequentalExecutor<LC> {
    pub fn new() -> SequentalExecutor<LC> {
        SequentalExecutor {
            local_context: None,
        }
    }
}

impl<LC> Executor for SequentalExecutor<LC> {
    type LC = LC;
    type E = Error;

    fn run<LCB, LCBE>(self, mut local_context_builder: LCB) -> Result<Self, ExecutorNewError<Self::E, LCBE>>
        where LCB: LocalContextBuilder<LC = Self::LC, E = LCBE>
    {
        let maybe_local_context = local_context_builder
            .make_local_context()
            .map_err(|e| ExecutorNewError::LocalContextBuilder(e));
        Ok(SequentalExecutor {
            local_context: Some(try!(maybe_local_context)),
        })
    }

    fn execute_job<J, JR, JRR, JE, JRE>(&mut self, input_size: usize, job: J) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, JRE>>> where
        J: Job<LC = Self::LC, R = JR, RR = JRR, E = JE> + Sync + Send + 'static,
        JRR: Reducer<R = JR, E = JRE> + ReducerRetrieve<LC = Self::LC>,
        JR: Send + 'static,
        JE: Send + 'static,
        JRE: Send + 'static
    {
        if let Some(local_context) = self.local_context.as_mut() {
            job.execute(local_context, 0 .. input_size)
                .map(|v| Some(v))
                .map_err(|e| ExecutorJobError::Job(e))
        } else {
            Err(ExecutorJobError::Executor(Error::NotInitialized))
        }
    }
}
