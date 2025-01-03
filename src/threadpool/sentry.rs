use super::Threadpool;
use super::ThreadpoolData;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub(super) struct Sentry<'a> {
	active: bool,
	data: &'a Arc<ThreadpoolData>,
}

impl<'a> Sentry<'a> {
	/// Create a new sentry tracker
	pub fn new(data: &'a Arc<ThreadpoolData>) -> Sentry<'a> {
		Sentry {
			data,
			active: true,
		}
	}
	/// Cancel and destroy this sentry
	pub fn cancel(mut self) {
		self.active = false;
	}
}

impl<'a> Drop for Sentry<'a> {
	fn drop(&mut self) {
		// If this sentry was still active,
		// then the task panicked without
		// properly cancelling the sentry,
		// so we should start a new thread.
		if self.active {
			// Reduce the active job count
			self.data.active_count.fetch_sub(1, Ordering::SeqCst);
			// Spawn another new thread
			Threadpool::spawn(None, self.data.clone());
		}
	}
}
