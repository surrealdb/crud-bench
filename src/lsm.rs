#![cfg(feature = "lsm")]
use std::path::PathBuf;

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{bail, Result};
use lsm::Options;
use lsm::Tree as Database;
use serde_json::Value;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const DATABASE_DIR: &str = "test-lsm";

pub(crate) struct LSMClientProvider(Arc<Database>);

impl BenchmarkEngine<LSMClient> for LSMClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_: KeyType, _columns: Columns, _options: &Benchmark) -> Result<Self> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();

		// Create the store
		let opts = Options::default()
			.with_path(PathBuf::from(DATABASE_DIR))
			.with_block_size(16 * 1024)
			.with_max_memtable_size(256 * 1024 * 1024)
			.with_block_cache_capacity(1 << 28) // 256 MiB
			.with_vlog_value_threshold(100)
			.with_filter_policy(None);
		let opts = Arc::new(opts);
		Ok(Self(Arc::new(Database::new(opts)?)))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<LSMClient> {
		Ok(LSMClient {
			db: self.0.clone(),
		})
	}
}

pub(crate) struct LSMClient {
	db: Arc<Database>,
}

impl BenchmarkClient for LSMClient {
	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Ok
		Ok(())
	}

	async fn compact(&self) -> Result<()> {
		// Compact the database
		self.db.flush()?;
		// Ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create_bytes(&key.to_ne_bytes(), val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create_bytes(&key.into_bytes(), val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read_bytes(&key.to_ne_bytes()).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read_bytes(&key.into_bytes()).await
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		self.update_bytes(&key.to_ne_bytes(), val).await
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		self.update_bytes(&key.into_bytes(), val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete_bytes(&key.to_ne_bytes()).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete_bytes(&key.into_bytes()).await
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan_bytes(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan_bytes(scan).await
	}
}

impl LSMClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.db.begin()?;
		// Process the data
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin()?;
		// Process the data
		let res = txn.get(key)?;
		// Check the value exists
		assert!(res.is_some());
		// Deserialise the value
		black_box(res.unwrap());
		// All ok
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.db.begin()?;
		// Process the data
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin()?;
		// Process the data
		txn.delete(key)?;
		txn.commit().await?;
		Ok(())
	}

	async fn scan_bytes(&self, scan: &Scan) -> Result<usize> {
		// Contional scans are not supported
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(usize::MAX);
		let t = scan.limit.map(|l| s + l);
		let p = scan.projection()?;
		// Create a new transaction
		let txn = self.db.begin()?;
		let beg = [0u8].to_vec();
		let end = [255u8].to_vec();
		let iter = txn.range(beg, end, t)?;
		// Create an iterator starting at the beginning
		let iter = iter.into_iter();

		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.0);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				// Scan the desired range of keys
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.1);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.0);
					count += 1;
				}
				Ok(count)
			}
		}
	}
}
