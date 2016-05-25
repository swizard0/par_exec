use super::{Executor, LocalContextBuilder, ExecutorNewError, ExecutorJobError, JobExecuteError, WorkAmount, JobIterBuild};

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

pub struct Sequentially(usize);

impl WorkAmount for Sequentially {
    type IT = ::std::ops::Range<usize>;

    fn new(work_amount: usize) -> Sequentially {
        Sequentially(work_amount)
    }
}

pub struct IterBuild;

impl JobIterBuild<Sequentially> for IterBuild {
    fn build(&self, work_amount: &Sequentially) -> <Sequentially as WorkAmount>::IT {
        0 .. work_amount.0
    }
}

impl<LC> Executor for SequentalExecutor<LC> {
    type LC = LC;
    type E = Error;
    type JIB = IterBuild;

    fn try_start<LCB>(self, mut local_context_builder: LCB) -> Result<Self, ExecutorNewError<Self::E, LCB::E>>
        where LCB: LocalContextBuilder<LC = Self::LC>
    {
        if self.local_context.is_some() {
            return Err(ExecutorNewError::Executor(Error::AlreadyInitialized));
        }

        let maybe_local_context = local_context_builder
            .make_local_context()
            .map_err(|e| ExecutorNewError::LocalContextBuilder(e));
        Ok(SequentalExecutor {
            local_context: Some(try!(maybe_local_context)),
        })
    }

    fn try_execute_job<WA, JF, JR, RF, JE, RE>(&mut self, work_amount: WA, map: JF, _reduce: RF) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, RE>>> where
        WA: WorkAmount,
        Self::JIB: JobIterBuild<WA>,
        JF: Fn(&mut Self::LC, WA::IT) -> Result<JR, JE> + Sync + Send + 'static,
        RF: Fn(&mut Self::LC, JR, JR) -> Result<JR, RE> + Sync + Send + 'static,
        JR: Send + 'static,
        JE: Send + 'static,
        RE: Send + 'static
    {
        let iter_build = IterBuild;
        if let Some(local_context) = self.local_context.as_mut() {
            map(local_context, iter_build.build(&work_amount))
                .map(|v| Some(v))
                .map_err(|e| ExecutorJobError::Job(JobExecuteError::Job(e)))
        } else {
            Err(ExecutorJobError::Executor(Error::NotInitialized))
        }
    }
}
