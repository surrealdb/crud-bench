pub(super) type Task = Box<dyn BoxedFn + Send + 'static>;

pub(super) trait BoxedFn {
	fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> BoxedFn for F {
	fn call_box(self: Box<F>) {
		(*self)()
	}
}
