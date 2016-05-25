extern crate num_cpus;

pub mod seq;
pub mod par;

#[derive(Debug)]
pub enum JobExecuteError<JE, RE> {
    Job(JE),
    Reducer(RE),
}

#[derive(Debug)]
pub enum ExecutorNewError<EE, LCBE> {
    Executor(EE),
    LocalContextBuilder(LCBE),
}

#[derive(Debug)]
pub enum ExecutorJobError<EE, JE> {
    Executor(EE),
    Job(JE),
    Several(Vec<ExecutorJobError<EE, JE>>),
}

pub trait LocalContextBuilder {
    type LC;
    type E;

    fn make_local_context(&mut self) -> Result<Self::LC, Self::E>;
}

impl<F, LC, E> LocalContextBuilder for F where F: FnMut() -> Result<LC, E> {
    type LC = LC;
    type E = E;

    fn make_local_context(&mut self) -> Result<Self::LC, Self::E> {
        self()
    }
}

pub trait WorkAmount: Sync + Send + 'static {
    type IT: Iterator<Item = usize>;
}

pub trait JobIterBuild<WA> where WA: WorkAmount {
    fn build(&self, work_amount: &WA) -> WA::IT;
}

pub trait Executor: Sized {
    type LC;
    type E;
    type JIB;

    fn try_start<LCB>(self, local_context_builder: LCB) -> Result<Self, ExecutorNewError<Self::E, LCB::E>>
        where LCB: LocalContextBuilder<LC = Self::LC>;

    fn start<LCBF>(self, mut local_context_builder: LCBF) -> Result<Self, ExecutorNewError<Self::E, ()>> where LCBF: FnMut() -> Self::LC {
        self.try_start(|| Ok(local_context_builder()))
    }

    fn try_execute_job<WA, JF, JR, RF, JE, RE>(&mut self, work_amount: WA, map: JF, reduce: RF) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, RE>>> where
        WA: WorkAmount,
        Self::JIB: JobIterBuild<WA>,
        JF: Fn(&mut Self::LC, WA::IT) -> Result<JR, JE> + Sync + Send + 'static,
        RF: Fn(&mut Self::LC, JR, JR) -> Result<JR, RE> + Sync + Send + 'static,
        JR: Send + 'static,
        JE: Send + 'static,
        RE: Send + 'static;

    fn execute_job<WA, JF, JR, RF>(&mut self, work_amount: WA, map: JF, reduce: RF) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<(), ()>>> where
        WA: WorkAmount,
        Self::JIB: JobIterBuild<WA>,
        JF: Fn(&mut Self::LC, WA::IT) -> JR + Sync + Send + 'static,
        RF: Fn(&mut Self::LC, JR, JR) -> JR + Sync + Send + 'static,
        JR: Send + 'static
    {
        self.try_execute_job(work_amount, move |lc, it| Ok(map(lc, it)), move |lc, a, b| Ok(reduce(lc, a, b)))
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    extern crate itertools;
    use self::rand::Rng;
    use self::itertools::Itertools;

    use std::sync::Arc;
    use std::collections::BinaryHeap;
    use super::{Executor, ExecutorJobError, JobExecuteError, WorkAmount, JobIterBuild};

    use super::{seq, par};

    struct SorterLocalContext;

    fn mergesort<Exec, ExecE, WA, F>(mut executor: Exec, make_wa: F) where
        Exec: Executor<LC = SorterLocalContext, E = ExecE>,
        ExecE: ::std::fmt::Debug,
        WA: WorkAmount,
        Exec::JIB: JobIterBuild<WA>,
        F: Fn(usize) -> WA
    {
        let mut rng = rand::thread_rng();
        let data: Arc<Vec<u64>> = Arc::new((0 .. 262144).map(|_| rng.gen()).collect());
        let mut sample = (*data).clone();
        sample.sort();

        let local_data = data.clone();
        let result =
            executor.execute_job(
                make_wa(data.len()),
                move |_, indices| BinaryHeap::from(indices.map(|i| local_data[i]).collect::<Vec<_>>()).into_sorted_vec(),
                |_, vec_a, vec_b| vec_a.into_iter().merge_by(vec_b.into_iter(), |a, b| a < b).collect())
            .unwrap()
            .unwrap();
        assert_eq!(sample, result);
    }

    #[test]
    fn mergesort_seq() {
        mergesort(seq::SequentalExecutor::new().start(|| SorterLocalContext).unwrap(), seq::Sequentially);
    }

    #[test]
    fn mergesort_par_alt() {
        let exec: par::ParallelExecutor<_> = Default::default();
        mergesort(exec.start(|| SorterLocalContext).unwrap(), par::Alternately);
    }

    #[test]
    fn mergesort_par_chunks() {
        let exec: par::ParallelExecutor<_> = Default::default();
        mergesort(exec.start(|| SorterLocalContext).unwrap(), par::ByEqualChunks);
    }

    #[test]
    fn par_errors() {
        #[derive(Debug)]
        struct LooserError(usize);

        let mut counter = 0;
        let mut executor = par::ParallelExecutor::new(5).start(|| { counter += 1; counter }).unwrap();
        match executor.try_execute_job(par::Alternately(10), |c, _indices| Err::<(), _>(LooserError(*c)), |_, _, _| Err(())) {
            Ok(_) =>
                panic!("Unexpected successfull result"),
            Err(ExecutorJobError::Several(errs)) => {
                let mut slaves_report: Vec<usize> = errs
                    .into_iter()
                    .map(|e| match e {
                        ExecutorJobError::Job(JobExecuteError::Job(LooserError(i))) => i,
                        other => panic!("Unexpected set member error: {:?}", other),
                    })
                    .collect();
                slaves_report.sort();
                assert_eq!(slaves_report, vec![1, 2, 3, 4, 5]);
            },
            Err(other) =>
                panic!("Unexpected error result: {:?}", other),
        }
    }
}
