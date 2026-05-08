use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Scan};
use anyhow::Result;
use std::hint::black_box;
use std::time::Duration;

pub(crate) struct DryClientProvider {}

impl BenchmarkEngine<DryClient> for DryClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, _options: &Benchmark) -> Result<Self> {
		Ok(Self {})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<DryClient> {
		Ok(DryClient {})
	}
}

pub(crate) struct DryClient {}

impl BenchmarkClient for DryClient {
	// The return type when reading a row
	type ReadRow = BenchValue;

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		Ok(black_box(BenchValue::Object(vec![("id".into(), BenchValue::UInt(key as u64))])))
	}

	async fn read_string(&self, key: String) -> Result<BenchValue> {
		Ok(black_box(BenchValue::Object(vec![("id".into(), BenchValue::String(key))])))
	}

	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
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

	async fn scan_u32(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		black_box(scan);
		Ok(scan.expect.unwrap_or(0))
	}

	async fn scan_string(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		black_box(scan);
		Ok(scan.expect.unwrap_or(0))
	}

	async fn batch_create_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		for (key, val) in key_vals {
			black_box((key, val));
		}
		Ok(())
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		for (key, val) in key_vals {
			black_box((key, val));
		}
		Ok(())
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		for key in keys {
			black_box(key);
		}
		Ok(())
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		for key in keys {
			black_box(key);
		}
		Ok(())
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		for (key, val) in key_vals {
			black_box((key, val));
		}
		Ok(())
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		for (key, val) in key_vals {
			black_box((key, val));
		}
		Ok(())
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		for key in keys {
			black_box(key);
		}
		Ok(())
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		for key in keys {
			black_box(key);
		}
		Ok(())
	}
}
