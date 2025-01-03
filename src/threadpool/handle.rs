use std::future::Future;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread;
use tokio::sync::oneshot::Receiver;

/// Async handle for a blocking task running in the thread pool.
///
/// If the spawned task panics, `poll()` will propagate the panic.
#[must_use]
#[derive(Debug)]
pub struct Handle<T> {
	pub(crate) rx: Receiver<thread::Result<T>>,
}

impl<T> Future for Handle<T> {
	type Output = T;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let rx = Pin::new(&mut self.rx);
		rx.poll(cx).map(|result| {
			result
				.expect("Unreachable error: Tokio channel closed")
				.unwrap_or_else(|err| resume_unwind(err))
		})
	}
}
