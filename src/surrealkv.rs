#![cfg(feature = "surrealkv")]

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use surrealkv::Mode;
use surrealkv::Options;
use surrealkv::Store;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::KeyType;

pub(crate) struct SurrealKVClientProvider(Arc<Store>);

impl BenchmarkEngine<SurrealKVClient> for SurrealKVClientProvider {
	async fn setup(_: KeyType) -> Result<Self> {
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

	async fn create_u32(&self, key: u32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(record)?;
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn create_string(&self, key: String, record: &Record) -> Result<()> {
		let key = key.into_bytes();
		let val = bincode::serialize(record)?;
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.set(&key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let key = &key.to_ne_bytes();
		let mut txn = self.db.begin_with_mode(Mode::ReadOnly)?;
		let read: Option<Vec<u8>> = txn.get(key)?;
		assert!(read.is_some());
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let key = key.into_bytes();
		let mut txn = self.db.begin_with_mode(Mode::ReadOnly)?;
		let read: Option<Vec<u8>> = txn.get(&key)?;
		assert!(read.is_some());
		Ok(())
	}

	async fn update_u32(&self, key: u32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(record)?;
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn update_string(&self, key: String, record: &Record) -> Result<()> {
		let key = key.into_bytes();
		let val = bincode::serialize(record)?;
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.set(&key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let key = &key.to_ne_bytes();
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.delete(key)?;
		txn.commit().await?;
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		let key = key.into_bytes();
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.delete(&key)?;
		txn.commit().await?;
		Ok(())
	}
}
