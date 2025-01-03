#![cfg(feature = "lmdb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
use heed::types::Bytes;
use heed::{Database, EnvOpenOptions};
use heed::{Env, EnvFlags};
use serde_json::Value;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

const DATABASE_DIR: &str = "lmdb";

const DEFAULT_SIZE: usize = 1_073_741_824;

static DATABASE_SIZE: LazyLock<usize> = LazyLock::new(|| {
	std::env::var("CRUD_BENCH_LMDB_DATABASE_SIZE")
		.map(|s| s.parse::<usize>().unwrap_or(DEFAULT_SIZE))
		.unwrap_or(DEFAULT_SIZE)
});

pub(crate) struct LmDBClientProvider(Arc<(Env, Database<Bytes, Bytes>)>);

impl BenchmarkEngine<LmDBClient> for LmDBClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, _endpoint: Option<&str>) -> Result<Self> {
		// Cleanup the data directory
		tokio::fs::remove_dir_all(DATABASE_DIR).await.ok();
		// Recreate the database directory
		tokio::fs::create_dir(DATABASE_DIR).await?;
		// Create a new environment
		println!("{}", *DATABASE_SIZE);
		let env = unsafe {
			EnvOpenOptions::new()
				.flags(
					EnvFlags::NO_TLS
						| EnvFlags::MAP_ASYNC
						| EnvFlags::NO_SYNC
						| EnvFlags::NO_META_SYNC,
				)
				.map_size(*DATABASE_SIZE)
				.open(DATABASE_DIR)
		}?;
		// Creaye the database
		let db = {
			// Open a new transaction
			let mut txn = env.write_txn()?;
			// Initiate the database
			env.create_database::<Bytes, Bytes>(&mut txn, None)?
		};
		// Create the store
		Ok(Self(Arc::new((env, db))))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<LmDBClient> {
		Ok(LmDBClient(self.0.clone()))
	}
}

pub(crate) struct LmDBClient(Arc<(Env, Database<Bytes, Bytes>)>);

impl BenchmarkClient for LmDBClient {
	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		tokio::fs::remove_dir_all(DATABASE_DIR).await.ok();
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

impl LmDBClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.0 .0.write_txn()?;
		// Process the data
		self.0 .1.put(&mut txn, key, val.as_ref())?;
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.0 .0.read_txn()?;
		// Process the data
		let res: Option<_> = self.0 .1.get(&txn, key.as_ref())?;
		assert!(res.is_some());
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.0 .0.write_txn()?;
		// Process the data
		self.0 .1.put(&mut txn, key.as_ref(), &val)?;
		txn.commit()?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.0 .0.write_txn()?;
		// Process the data
		self.0 .1.delete(&mut txn, key.as_ref())?;
		txn.commit()?;
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
		// Create a new transaction
		let txn = self.0 .0.read_txn()?;
		// Create an iterator starting at the beginning
		let iter = self.0 .1.iter(&txn)?;
		// Perform the relevant projection scan type
		match scan.projection()? {
			Projection::Id => {
				// Skip `offset` entries, then collect `limit` entries
				Ok(iter
					.skip(s) // Skip the first `offset` entries
					.take(l) // Take the next `limit` entries
					.map(|v| -> Result<_> { Ok(black_box(v?.0)) })
					.collect::<Result<Vec<_>>>()?
					.len())
			}
			Projection::Full => {
				// Skip `offset` entries, then collect `limit` entries
				Ok(iter
					.skip(s) // Skip the first `offset` entries
					.take(l) // Take the next `limit` entries
					.map(|v| -> Result<_> { Ok(black_box(v?)) })
					.collect::<Result<Vec<_>>>()?
					.len())
			}
			Projection::Count => {
				// Skip `offset` entries, then collect `limit` entries
				Ok(iter
					.skip(s) // Skip the first `offset` entries
					.take(l) // Take the next `limit` entries
					.count())
			}
		}
	}
}
