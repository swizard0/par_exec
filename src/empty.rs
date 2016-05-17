use super::{ReduceContextRetrieve, LocalContextBuilder};

#[derive(Debug)]
pub struct EmptyError;
pub struct EmptyReduceContext;
pub struct EmptyLocalContext(EmptyReduceContext);
pub struct EmptyLocalContextBuilder;

impl ReduceContextRetrieve for EmptyReduceContext {
    type LC = EmptyLocalContext;

    fn get(local_context: &mut Self::LC) -> &mut Self {
        &mut local_context.0
    }
}

impl LocalContextBuilder for EmptyLocalContextBuilder {
    type LC = EmptyLocalContext;
    type E = EmptyError;

    fn make_local_context(&mut self) -> Result<Self::LC, Self::E> {
        Ok(EmptyLocalContext(EmptyReduceContext))
    }
}
