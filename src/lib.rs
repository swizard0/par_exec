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

pub trait Executor: Sized {
    type LC;
    type E;
    type IT: Iterator<Item = usize>;

    fn try_start<LCBF, LCBE>(self, local_context_builder: LCBF) -> Result<Self, ExecutorNewError<Self::E, LCBE>>
        where LCBF: FnMut() -> Result<Self::LC, LCBE>;

    fn start<LCBF>(self, mut local_context_builder: LCBF) -> Result<Self, ExecutorNewError<Self::E, ()>> where LCBF: FnMut() -> Self::LC {
        self.try_start(|| Ok(local_context_builder()))
    }

    fn try_execute_job<JF, JR, RF, JE, RE>(&mut self, input_size: usize, map: JF, reduce: RF) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, RE>>> where
        JF: Fn(&mut Self::LC, Self::IT) -> Result<JR, JE> + Sync + Send + 'static,
        RF: Fn(&mut Self::LC, JR, JR) -> Result<JR, RE> + Sync + Send + 'static,
        JR: Send + 'static,
        JE: Send + 'static,
        RE: Send + 'static;

    fn execute_job<JF, JR, RF>(&mut self, input_size: usize, map: JF, reduce: RF) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<(), ()>>> where
        JF: Fn(&mut Self::LC, Self::IT) -> JR + Sync + Send + 'static,
        RF: Fn(&mut Self::LC, JR, JR) -> JR + Sync + Send + 'static,
        JR: Send + 'static
    {
        self.try_execute_job(input_size, move |lc, it| Ok(map(lc, it)), move |lc, a, b| Ok(reduce(lc, a, b)))
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
    use super::{Executor, ExecutorJobError, JobExecuteError};

    use super::{seq, par};

    struct SorterLocalContext;

    fn mergesort<Exec, ExecE>(mut executor: Exec) where Exec: Executor<LC = SorterLocalContext, E = ExecE>, ExecE: ::std::fmt::Debug {
        let mut rng = rand::thread_rng();
        let data: Arc<Vec<u64>> = Arc::new((0 .. 262144).map(|_| rng.gen()).collect());
        let mut sample = (*data).clone();
        sample.sort();

        let local_data = data.clone();
        let result =
            executor.execute_job(
                data.len(),
                move |_, indices| BinaryHeap::from(indices.map(|i| local_data[i]).collect::<Vec<_>>()).into_sorted_vec(),
                |_, vec_a, vec_b| vec_a.into_iter().merge_by(vec_b.into_iter(), |a, b| a < b).collect())
            .unwrap()
            .unwrap();
        assert_eq!(sample, result);
    }

    #[test]
    fn mergesort_seq() {
        mergesort(seq::SequentalExecutor::new().start(|| SorterLocalContext).unwrap());
    }

    #[test]
    fn mergesort_par() {
        let exec: par::ParallelExecutor<_> = Default::default();
        mergesort(exec.start(|| SorterLocalContext).unwrap());
    }

    #[test]
    fn par_errors() {
        #[derive(Debug)]
        struct LooserError(usize);

        let mut counter = 0;
        let mut executor = par::ParallelExecutor::new(5).start(|| { counter += 1; counter }).unwrap();
        match executor.try_execute_job(10, |c, _indices| Err::<(), _>(LooserError(*c)), |_, _, _| Err(())) {
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
