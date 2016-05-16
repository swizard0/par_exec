pub mod seq;
pub mod par;

pub trait Reduce: Sized + Send + 'static {
    type E: Send + 'static;

    fn len(&self) -> Option<usize>;
    fn reduce(self, other_result: Self) -> Result<Self, Self::E>;
}

pub trait ThreadContextBuilder {
    type TC;
    type E;

    fn make_thread_context(&self) -> Result<Self::TC, Self::E>;
}

pub enum JobExecuteError<JE, RE> {
    Job(JE),
    Result(RE),
}

pub trait Job: Sync + Send + 'static {
    type TC;
    type R: Reduce;
    type E;

    fn execute<IS>(&self, thread_context: &mut Self::TC, input_indices: IS) ->
        Result<Self::R, JobExecuteError<Self::E, <Self::R as Reduce>::E>>
        where IS: Iterator<Item = usize>;
}

pub enum ExecutorNewError<EE, TCBE> {
    Executor(EE),
    ThreadContextBuilder(TCBE),
}

pub enum ExecutorJobError<EE, JE> {
    Executor(EE),
    Job(JE),
    Several(Vec<ExecutorJobError<EE, JE>>),
}

pub trait Executor: Sized {
    type TC;
    type E;

    fn run<TCB, TCBE>(self, thread_context_builder: TCB) -> Result<Self, ExecutorNewError<Self::E, TCBE>>
        where TCB: ThreadContextBuilder<TC = Self::TC, E = TCBE>;

    fn execute_job<J, JR, JE>(&mut self, input_size: usize, job: J) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, JR::E>>>
        where J: Job<TC = Self::TC, R = JR, E = JE>, JR: Reduce, JE: Send + 'static;
}
