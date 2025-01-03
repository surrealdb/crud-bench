use super::Threadpool;
use std::sync::OnceLock;

pub(super) static THREADPOOL: OnceLock<Threadpool> = OnceLock::new();
