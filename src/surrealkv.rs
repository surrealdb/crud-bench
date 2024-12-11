#![cfg(feature = "surrealkv")]

use anyhow::{bail, Result};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use surrealkv::Options;
use surrealkv::Store;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, NOT_SUPPORTED_ERROR};
use crate::valueprovider::Columns;
use crate::{KeyType, Scan};

pub(crate) struct SurrealKVClientProvider(Arc<Store>);

impl BenchmarkEngine<SurrealKVClient> for SurrealKVClientProvider {
	async fn setup(_: KeyType, _columns: Columns) -> Result<Self> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("surrealkv");
		// Configure custom options
		let mut opts = Options::new();
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
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(&val)?;
		let mut txn = self.db.begin()?;
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		let key = key.into_bytes();
		let val = bincode::serialize(&val)?;
		let mut txn = self.db.begin()?;
		txn.set(&key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let key = &key.to_ne_bytes();
		let mut txn = self.db.begin()?;
		let read: Option<Vec<u8>> = txn.get(key)?;
		assert!(read.is_some());
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let key = key.into_bytes();
		let mut txn = self.db.begin()?;
		let read: Option<Vec<u8>> = txn.get(&key)?;
		assert!(read.is_some());
		Ok(())
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}
	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(&val)?;
		let mut txn = self.db.begin()?;
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let key = key.into_bytes();
		let val = bincode::serialize(&val)?;
		let mut txn = self.db.begin()?;
		txn.set(&key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let key = &key.to_ne_bytes();
		let mut txn = self.db.begin()?;
		txn.delete(key)?;
		txn.commit().await?;
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		let key = key.into_bytes();
		let mut txn = self.db.begin()?;
		txn.delete(&key)?;
		txn.commit().await?;
		Ok(())
	}
}

impl SurrealKVClient {
	async fn scan(&self, scan: &Scan) -> Result<usize> {
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}

		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(0);
		let k = scan.keys_only.unwrap_or(false);
		if k {
			bail!(NOT_SUPPORTED_ERROR);
		}

		let mut txn = self.db.begin()?;
		let start = [0u8].as_slice();
		let end = [255u8].as_slice();
		let res = txn.scan(start..end, Some(s + l))?;
		Ok(res.len())
	}
}
