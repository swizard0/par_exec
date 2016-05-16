extern crate num_cpus;

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

    fn make_thread_context(&mut self) -> Result<Self::TC, Self::E>;
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum ExecutorNewError<EE, TCBE> {
    Executor(EE),
    ThreadContextBuilder(TCBE),
}

#[derive(Debug)]
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

#[cfg(test)]
mod tests {
    extern crate rand;
    extern crate itertools;
    use self::rand::Rng;
    use self::itertools::Itertools;

    use std::sync::Arc;
    use std::collections::BinaryHeap;
    use super::{Executor, Job, ExecutorJobError, JobExecuteError, ThreadContextBuilder, Reduce};

    use super::{seq, par};

    struct EmptyThreadContextBuilder;

    impl ThreadContextBuilder for EmptyThreadContextBuilder {
        type TC = ();
        type E = ();

        fn make_thread_context(&mut self) -> Result<Self::TC, Self::E> {
            Ok(())
        }
    }

    struct Reducer(Vec<u64>);

    impl Reduce for Reducer {
        type E = ();

        fn len(&self) -> Option<usize> {
            Some(self.0.len())
        }

        fn reduce(self, other_result: Self) -> Result<Self, Self::E> {
            Ok(Reducer(self.0.into_iter().merge_by(other_result.0.into_iter(), |a, b| a < b).collect()))
        }
    }

    struct Sorter(Arc<Vec<u64>>);

    impl Job for Sorter {
        type TC = ();
        type R = Reducer;
        type E = ();

        fn execute<IS>(&self, _thread_context: &mut Self::TC, input_indices: IS) ->
            Result<Self::R, JobExecuteError<Self::E, <Self::R as Reduce>::E>>
            where IS: Iterator<Item = usize>
        {
            let heap = BinaryHeap::from(input_indices.map(|i| self.0[i]).collect::<Vec<_>>());
            Ok(Reducer(heap.into_sorted_vec()))
        }
    }

    fn mergesort<E, EE>(mut executor: E) where E: Executor<TC = (), E = EE>, EE: ::std::fmt::Debug {
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
        mergesort(seq::SequentalExecutor::new().run(EmptyThreadContextBuilder).unwrap());
    }

    #[test]
    fn mergesort_par() {
        let exec: par::ParallelExecutor<_> = Default::default();
        mergesort(exec.run(EmptyThreadContextBuilder).unwrap());
    }

    #[test]
    fn par_errors() {
        #[derive(Debug)]
        struct Error(usize);

        struct Looser(Arc<Vec<u64>>);

        impl Job for Looser {
            type TC = usize;
            type R = Reducer;
            type E = Error;

            fn execute<IS>(&self, thread_context: &mut Self::TC, _input_indices: IS) ->
                Result<Self::R, JobExecuteError<Self::E, <Self::R as Reduce>::E>>
                where IS: Iterator<Item = usize>
            {
                Err(JobExecuteError::Job(Error(*thread_context)))
            }
        }

        struct SlaveIndex(usize);

        impl ThreadContextBuilder for SlaveIndex {
            type TC = usize;
            type E = ();

            fn make_thread_context(&mut self) -> Result<Self::TC, Self::E> {
                self.0 += 1;
                Ok(self.0)
            }
        }

        let mut executor = par::ParallelExecutor::new(5).run(SlaveIndex(0)).unwrap();
        match executor.execute_job(10, Looser(Arc::new(vec![]))) {
            Ok(_) =>
                panic!("Unexpected successfull result"),
            Err(ExecutorJobError::Several(errs)) => {
                let mut slaves_report: Vec<usize> = errs
                    .into_iter()
                    .map(|e| match e {
                        ExecutorJobError::Job(JobExecuteError::Job(Error(i))) => i,
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
