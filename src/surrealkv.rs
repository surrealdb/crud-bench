#![cfg(feature = "surrealkv")]

use anyhow::Error;
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use surrealkv::Mode;
use surrealkv::Options;
use surrealkv::Store;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};

pub(crate) struct SurrealKVClientProvider {
	db: Arc<Store>,
}

impl SurrealKVClientProvider {
	pub(crate) async fn setup() -> Result<Self, Error> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("surrealkv");
		// Configure custom options
		let mut opts = Options::new();
		// Set the directory location
		opts.dir = PathBuf::from("surrealkv");
		// Create the store
		Ok(SurrealKVClientProvider {
			db: Arc::new(Store::new(opts)?),
		})
	}
}

impl BenchmarkEngine<SurrealKVClient> for SurrealKVClientProvider {
	async fn create_client(&self, _: Option<String>) -> Result<SurrealKVClient> {
		Ok(SurrealKVClient {
			db: self.db.clone(),
		})
	}
}

pub(crate) struct SurrealKVClient {
	db: Arc<Store>,
}

impl BenchmarkClient for SurrealKVClient {
	async fn shutdown(&mut self) -> Result<()> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("surrealkv");
		// Ok
		Ok(())
	}

	async fn create(&mut self, key: i32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(record)?;
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn read(&mut self, key: i32) -> Result<()> {
		let key = &key.to_ne_bytes();
		let txn = self.db.begin_with_mode(Mode::ReadOnly)?;
		let read: Option<Vec<u8>> = txn.get(key)?;
		assert!(read.is_some());
		Ok(())
	}

	async fn update(&mut self, key: i32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(record)?;
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn delete(&mut self, key: i32) -> Result<()> {
		let key = &key.to_ne_bytes();
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.delete(key)?;
		txn.commit().await?;
		Ok(())
	}
}
