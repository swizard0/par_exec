extern crate num_cpus;

pub mod seq;
pub mod par;
pub mod empty;

pub trait Reduce: Sized + Send + 'static {
    type RC;
    type E: Send + 'static;

    fn len(&self) -> Option<usize>;
    fn reduce(self, other: Self, reduce_context: &mut Self::RC) -> Result<Self, Self::E>;
}

pub trait LocalContextBuilder {
    type LC;
    type E;

    fn make_local_context(&mut self) -> Result<Self::LC, Self::E>;
}

pub trait ReduceContextRetrieve {
    type LC;

    fn get(local_context: &mut Self::LC) -> &mut Self;
}

#[derive(Debug)]
pub enum JobExecuteError<JE, RE> {
    Job(JE),
    Result(RE),
}

pub trait Job: Sync + Send + 'static {
    type LC;
    type RC: ReduceContextRetrieve<LC = Self::LC>;
    type R: Reduce<RC = Self::RC>;
    type E;

    fn execute<IS>(&self, local_context: &mut Self::LC, input_indices: IS) ->
        Result<Self::R, JobExecuteError<Self::E, <Self::R as Reduce>::E>>
        where IS: Iterator<Item = usize>;
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

    fn run<LCB, LCBE>(self, local_context_builder: LCB) -> Result<Self, ExecutorNewError<Self::E, LCBE>>
        where LCB: LocalContextBuilder<LC = Self::LC, E = LCBE>;

    fn execute_job<J, JRC, JR, JE>(&mut self, input_size: usize, job: J) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, JR::E>>> where
        J: Job<LC = Self::LC, RC = JRC, R = JR, E = JE>,
        JRC: ReduceContextRetrieve<LC = Self::LC>,
        JR: Reduce<RC = JRC>,
        JE: Send + 'static;
}

#[cfg(test)]
mod tests {
    extern crate rand;
    extern crate itertools;
    use self::rand::Rng;
    use self::itertools::Itertools;

    use std::sync::Arc;
    use std::collections::BinaryHeap;
    use super::{Executor, Job, ExecutorJobError, JobExecuteError, LocalContextBuilder, Reduce, ReduceContextRetrieve};

    use super::{seq, par, empty};

    struct Reducer(Vec<u64>);

    impl Reduce for Reducer {
        type RC = empty::EmptyReduceContext;
        type E = empty::EmptyError;

        fn len(&self) -> Option<usize> {
            Some(self.0.len())
        }

        fn reduce(self, other: Self, _reduce_context: &mut Self::RC) -> Result<Self, Self::E> {
            Ok(Reducer(self.0.into_iter().merge_by(other.0.into_iter(), |a, b| a < b).collect()))
        }
    }

    struct Sorter(Arc<Vec<u64>>);

    impl Job for Sorter {
        type LC = empty::EmptyLocalContext;
        type RC = empty::EmptyReduceContext;
        type R = Reducer;
        type E = empty::EmptyError;

        fn execute<IS>(&self, _local_context: &mut Self::LC, input_indices: IS) ->
            Result<Self::R, JobExecuteError<Self::E, <Self::R as Reduce>::E>>
            where IS: Iterator<Item = usize>
        {
            let heap = BinaryHeap::from(input_indices.map(|i| self.0[i]).collect::<Vec<_>>());
            Ok(Reducer(heap.into_sorted_vec()))
        }
    }

    fn mergesort<E, EE>(mut executor: E) where E: Executor<LC = empty::EmptyLocalContext, E = EE>, EE: ::std::fmt::Debug {
        let mut rng = rand::thread_rng();
        let data: Arc<Vec<u64>> = Arc::new((0 .. 262144).map(|_| rng.gen()).collect());
        let mut sample = (*data).clone();
        sample.sort();

        let Reducer(result) =
            executor.execute_job(data.len(), Sorter(data.clone())).unwrap().unwrap();
        assert_eq!(sample, result);
    }

    #[test]
    fn mergesort_seq() {
        mergesort(seq::SequentalExecutor::new().run(empty::EmptyLocalContextBuilder).unwrap());
    }

    #[test]
    fn mergesort_par() {
        let exec: par::ParallelExecutor<_> = Default::default();
        mergesort(exec.run(empty::EmptyLocalContextBuilder).unwrap());
    }

    #[test]
    fn par_errors() {
        #[derive(Debug)]
        struct LooserError(usize);
        struct LooserReducer;
        struct LooserReduceContext;
        struct LooserLocalContext(usize, LooserReduceContext);
        struct LooserLocalContextBuilder(usize);
        struct LooserJob;

        impl Reduce for LooserReducer {
            type RC = LooserReduceContext;
            type E = empty::EmptyError;

            fn len(&self) -> Option<usize> {
                None
            }

            fn reduce(self, _other: Self, _reduce_context: &mut Self::RC) -> Result<Self, Self::E> {
                Err(empty::EmptyError)
            }
        }

        impl ReduceContextRetrieve for LooserReduceContext {
            type LC = LooserLocalContext;

            fn get(local_context: &mut Self::LC) -> &mut Self {
                &mut local_context.1
            }
        }

        impl LocalContextBuilder for LooserLocalContextBuilder {
            type LC = LooserLocalContext;
            type E = empty::EmptyError;

            fn make_local_context(&mut self) -> Result<Self::LC, Self::E> {
                self.0 += 1;
                Ok(LooserLocalContext(self.0, LooserReduceContext))
            }
        }

        impl Job for LooserJob {
            type LC = LooserLocalContext;
            type RC = LooserReduceContext;
            type R = LooserReducer;
            type E = LooserError;

            fn execute<IS>(&self, local_context: &mut Self::LC, _input_indices: IS) ->
                Result<Self::R, JobExecuteError<Self::E, <Self::R as Reduce>::E>>
                where IS: Iterator<Item = usize>
            {
                Err(JobExecuteError::Job(LooserError(local_context.0)))
            }
        }

        let mut executor = par::ParallelExecutor::new(5).run(LooserLocalContextBuilder(0)).unwrap();
        match executor.execute_job(10, LooserJob) {
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
