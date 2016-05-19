use super::LocalContextBuilder;

#[derive(Debug)]
pub struct EmptyError;
pub struct EmptyReducer;
pub struct EmptyLocalContext(EmptyReducer);
pub struct EmptyLocalContextBuilder;

impl LocalContextBuilder for EmptyLocalContextBuilder {
    type LC = EmptyLocalContext;
    type E = EmptyError;

    fn make_local_context(&mut self) -> Result<Self::LC, Self::E> {
        Ok(EmptyLocalContext(EmptyReducer))
    }
}

impl AsMut<EmptyReducer> for EmptyLocalContext {
    fn as_mut(&mut self) -> &mut EmptyReducer {
        &mut self.0
    }
}
