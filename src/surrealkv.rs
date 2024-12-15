#![cfg(feature = "surrealkv")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, NOT_SUPPORTED_ERROR};
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
use serde_json::Value;
use std::hint::black_box;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::Arc;
use surrealkv::Mode::{ReadOnly, ReadWrite};
use surrealkv::Options;
use surrealkv::Store;

pub(crate) struct SurrealKVClientProvider(Arc<Store>);

impl BenchmarkEngine<SurrealKVClient> for SurrealKVClientProvider {
	async fn setup(_: KeyType, _columns: Columns) -> Result<Self> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("surrealkv");
		// Configure custom options
		let mut opts = Options::new();
		// Disable versioning
		opts.enable_versions = false;
		// Enable disk persistence
		opts.disk_persistence = true;
		// Set the directory location
		opts.dir = PathBuf::from("surrealkv");
		// Create the store
		Ok(Self(Arc::new(Store::new(opts)?)))
	}

	async fn create_client(&self, _: Option<String>) -> Result<SurrealKVClient> {
		Ok(SurrealKVClient {
			db: self.0.clone(),
		})
	}
}

pub(crate) struct SurrealKVClient {
	db: Arc<Store>,
}

impl BenchmarkClient for SurrealKVClient {
	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("surrealkv");
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

impl SurrealKVClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Process the data
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadOnly)?;
		// Process the data
		let res = txn.get(key)?;
		assert!(res.is_some());
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Process the data
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
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
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadOnly)?;
		let beg = [0u8].as_slice();
		let end = [255u8].as_slice();
		// Perform the relevant projection scan type
		match scan.projection()? {
			Projection::Id => {
				// Skip `offset` entries, then collect `limit` entries
				Ok(txn
					.scan(beg..end, Some(s + l))?
					.into_iter()
					.skip(s)
					.take(l)
					.map(|v| black_box(v.0))
					.collect::<Vec<_>>()
					.len())
			}
			Projection::Full => {
				// Skip `offset` entries, then collect `limit` entries
				Ok(txn
					.scan(beg..end, Some(s + l))?
					.into_iter()
					.skip(s)
					.take(l)
					.map(|v| black_box((v.0, v.1)))
					.collect::<Vec<_>>()
					.len())
			}
			Projection::Count => {
				// Skip `offset` entries, then collect `limit` entries
				Ok(txn
					.scan(beg..end, None)?
					.into_iter()
					.skip(s)
					.take(l)
					.map(|_| true)
					.collect::<Vec<_>>()
					.len())
			}
		}
	}
}
