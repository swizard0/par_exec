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

    fn make_thread_context(&self) -> Result<Self::TC, Self::E>;
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
    use super::{Executor, Job, JobExecuteError, ThreadContextBuilder, Reduce};

    use super::seq::SequentalExecutor;
    use super::par::ParallelExecutor;

    struct EmptyThreadContextBuilder;

    impl ThreadContextBuilder for EmptyThreadContextBuilder {
        type TC = ();
        type E = ();

        fn make_thread_context(&self) -> Result<Self::TC, Self::E> {
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

        let Reducer(seq_result) =
            executor.execute_job(data.len(), Sorter(data.clone())).unwrap().unwrap();
        assert_eq!(sample, seq_result);
    }

    #[test]
    fn mergesort_seq() {
        mergesort(SequentalExecutor::new().run(EmptyThreadContextBuilder).unwrap());
    }

    #[test]
    fn mergesort_par() {
        let exec: ParallelExecutor<_> = Default::default();
        mergesort(exec.run(EmptyThreadContextBuilder).unwrap());
    }
}
