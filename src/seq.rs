use super::{Executor, ExecutorNewError, ExecutorJobError, JobExecuteError};

#[derive(Debug)]
pub enum Error {
    NotInitialized,
    AlreadyInitialized,
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
    type IT = ::std::ops::Range<usize>;

    fn try_start<LCBF, LCBE>(self, mut local_context_builder: LCBF) -> Result<Self, ExecutorNewError<Self::E, LCBE>>
        where LCBF: FnMut() -> Result<Self::LC, LCBE>
    {
        if self.local_context.is_some() {
            return Err(ExecutorNewError::Executor(Error::AlreadyInitialized));
        }

        let maybe_local_context = local_context_builder()
            .map_err(|e| ExecutorNewError::LocalContextBuilder(e));
        Ok(SequentalExecutor {
            local_context: Some(try!(maybe_local_context)),
        })
    }

    fn try_execute_job<JF, JR, RF, JE, RE>(&mut self, input_size: usize, map: JF, _reduce: RF) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, RE>>> where
        JF: Fn(&mut Self::LC, Self::IT) -> Result<JR, JE> + Sync + Send + 'static,
        RF: Fn(&mut Self::LC, JR, JR) -> Result<JR, RE> + Sync + Send + 'static,
        JR: Send + 'static,
        JE: Send + 'static,
        RE: Send + 'static
    {
        if let Some(local_context) = self.local_context.as_mut() {
            map(local_context, 0 .. input_size)
                .map(|v| Some(v))
                .map_err(|e| ExecutorJobError::Job(JobExecuteError::Job(e)))
        } else {
            Err(ExecutorJobError::Executor(Error::NotInitialized))
        }
    }
}
