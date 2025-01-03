use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{KeyType, Scan};
use anyhow::Result;
use serde_json::Value;
use std::hint::black_box;
use std::time::Duration;

pub(crate) struct DryClientProvider {}

impl BenchmarkEngine<DryClient> for DryClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, _endpoint: Option<&str>) -> Result<Self> {
		Ok(Self {})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<DryClient> {
		Ok(DryClient {})
	}
}

pub(crate) struct DryClient {}

impl BenchmarkClient for DryClient {
	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		black_box(key);
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		black_box(key);
		Ok(())
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		black_box(key);
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		black_box(key);
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		black_box(scan);
		Ok(scan.expect.unwrap_or(0))
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		black_box(scan);
		Ok(scan.expect.unwrap_or(0))
	}
}
