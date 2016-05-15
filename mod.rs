use super::set::Set;

pub mod seq;
pub mod par;

pub trait UnionResult: Sized + Sync + Send + 'static {
    type E: Sync + Send + 'static;

    fn len(&self) -> Option<usize>;
    fn union(self, other_result: Self) -> Result<Self, Self::E>;
}

pub trait ThreadContextBuilder {
    type TC;
    type E;

    fn make_thread_context(&self) -> Result<Self::TC, Self::E>;
}

pub enum JobExecuteError<JE, SE, RE> {
    Job(JE),
    Set(SE),
    Result(RE),
}

pub trait Job: Sync + Send + 'static {
    type TC;
    type T;
    type S: Set<T = Self::T>;
    type R: UnionResult;
    type E;

    fn execute<IS>(&self, thread_context: &mut Self::TC, input: &Self::S, indices: IS) ->
        Result<Self::R, JobExecuteError<Self::E, <Self::S as Set>::E, <Self::R as UnionResult>::E>>
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

    fn execute_job<J, S, T, JR, JE>(&mut self, input: S, job: J) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, S::E, JR::E>>>
        where J: Job<TC = Self::TC, T = T, S = S, R = JR, E = JE>, S: Set<T = T> + 'static, JR: UnionResult, JE: Sync + Send + 'static;
}
