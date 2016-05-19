extern crate num_cpus;

pub mod seq;
pub mod par;
pub mod empty;

pub trait Reducer {
    type R;
    type E;

    fn len(&self, item: &Self::R) -> Option<usize>;
    fn reduce(&mut self, item_a: Self::R, item_b: Self::R) -> Result<Self::R, Self::E>;
}

pub trait LocalContextBuilder {
    type LC;
    type E;

    fn make_local_context(&mut self) -> Result<Self::LC, Self::E>;
}

#[derive(Debug)]
pub enum JobExecuteError<JE, RE> {
    Job(JE),
    Reducer(RE),
}

pub trait Job {
    type R;
    type RR: Reducer<R = Self::R>;
    type LC: AsMut<Self::RR>;
    type E;

    fn execute<IS>(&self, local_context: &mut Self::LC, input_indices: IS) ->
        Result<Self::R, JobExecuteError<Self::E, <Self::RR as Reducer>::E>>
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

    fn start<LCB, LCBE>(self, local_context_builder: LCB) -> Result<Self, ExecutorNewError<Self::E, LCBE>>
        where LCB: LocalContextBuilder<LC = Self::LC, E = LCBE>;

    fn execute_job<J, JR, JRR, JE, JRE>(&mut self, input_size: usize, job: J) ->
        Result<Option<JR>, ExecutorJobError<Self::E, JobExecuteError<JE, JRE>>> where
        J: Job<LC = Self::LC, R = JR, RR = JRR, E = JE> + Sync + Send + 'static,
        JRR: Reducer<R = JR, E = JRE>,
        Self::LC: AsMut<JRR>,
        JR: Send + 'static,
        JE: Send + 'static,
        JRE: Send + 'static;
}

#[cfg(test)]
mod tests {
    extern crate rand;
    extern crate itertools;
    use self::rand::Rng;
    use self::itertools::Itertools;

    use std::sync::Arc;
    use std::collections::BinaryHeap;
    use super::{Executor, Job, ExecutorJobError, JobExecuteError, LocalContextBuilder, Reducer};

    use super::{seq, par, empty};

    struct SorterReducer;
    struct SorterLocalContext(SorterReducer);
    struct SorterLocalContextBuilder;
    struct SorterJob(Arc<Vec<u64>>);

    impl Reducer for SorterReducer {
        type R = Vec<u64>;
        type E = empty::EmptyError;

        fn len(&self, item: &Self::R) -> Option<usize> {
            Some(item.len())
        }

        fn reduce(&mut self, item_a: Self::R, item_b: Self::R) -> Result<Self::R, Self::E> {
            Ok(item_a.into_iter().merge_by(item_b.into_iter(), |a, b| a < b).collect())
        }
    }

    impl AsMut<SorterReducer> for SorterLocalContext {
        fn as_mut(&mut self) -> &mut SorterReducer {
            &mut self.0
        }
    }

    impl LocalContextBuilder for SorterLocalContextBuilder {
        type LC = SorterLocalContext;
        type E = empty::EmptyError;

        fn make_local_context(&mut self) -> Result<Self::LC, Self::E> {
            Ok(SorterLocalContext(SorterReducer))
        }
    }

    impl Job for SorterJob {
        type LC = SorterLocalContext;
        type R = Vec<u64>;
        type RR = SorterReducer;
        type E = empty::EmptyError;

        fn execute<IS>(&self, _local_context: &mut Self::LC, input_indices: IS) ->
            Result<Self::R, JobExecuteError<Self::E, <Self::RR as Reducer>::E>>
            where IS: Iterator<Item = usize>
        {
            let heap = BinaryHeap::from(input_indices.map(|i| self.0[i]).collect::<Vec<_>>());
            Ok(heap.into_sorted_vec())
        }
    }

    fn mergesort<E, EE>(mut executor: E) where E: Executor<LC = SorterLocalContext, E = EE>, EE: ::std::fmt::Debug {
        let mut rng = rand::thread_rng();
        let data: Arc<Vec<u64>> = Arc::new((0 .. 262144).map(|_| rng.gen()).collect());
        let mut sample = (*data).clone();
        sample.sort();

        let result =
            executor.execute_job(data.len(), SorterJob(data.clone())).unwrap().unwrap();
        assert_eq!(sample, result);
    }

    #[test]
    fn mergesort_seq() {
        mergesort(seq::SequentalExecutor::new().start(SorterLocalContextBuilder).unwrap());
    }

    #[test]
    fn mergesort_par() {
        let exec: par::ParallelExecutor<_> = Default::default();
        mergesort(exec.start(SorterLocalContextBuilder).unwrap());
    }

    #[test]
    fn par_errors() {
        #[derive(Debug)]
        struct LooserError(usize);
        struct LooserReducer;
        struct LooserLocalContext(usize, LooserReducer);
        struct LooserLocalContextBuilder(usize);
        struct LooserJob;

        impl Reducer for LooserReducer {
            type R = ();
            type E = empty::EmptyError;

            fn len(&self, _item: &Self::R) -> Option<usize> {
                None
            }

            fn reduce(&mut self, _item_a: Self::R, _item_b: Self::R) -> Result<Self::R, Self::E> {
                Err(empty::EmptyError)
            }
        }

        impl AsMut<LooserReducer> for LooserLocalContext {
            fn as_mut(&mut self) -> &mut LooserReducer {
                &mut self.1
            }
        }

        impl LocalContextBuilder for LooserLocalContextBuilder {
            type LC = LooserLocalContext;
            type E = empty::EmptyError;

            fn make_local_context(&mut self) -> Result<Self::LC, Self::E> {
                self.0 += 1;
                Ok(LooserLocalContext(self.0, LooserReducer))
            }
        }

        impl Job for LooserJob {
            type LC = LooserLocalContext;
            type R = ();
            type RR = LooserReducer;
            type E = LooserError;

            fn execute<IS>(&self, local_context: &mut Self::LC, _input_indices: IS) ->
                Result<Self::R, JobExecuteError<Self::E, <Self::RR as Reducer>::E>>
                where IS: Iterator<Item = usize>
            {
                Err(JobExecuteError::Job(LooserError(local_context.0)))
            }
        }

        let mut executor = par::ParallelExecutor::new(5).start(LooserLocalContextBuilder(0)).unwrap();
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
