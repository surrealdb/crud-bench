use anyhow::Result;
use std::path::PathBuf;
use surrealkv::Mode;
use surrealkv::Options;
use surrealkv::Store;

use crate::benchmark::{BenchmarkClient, BenchmarkClientProvider, Record};

#[derive(Default)]
pub(crate) struct SurrealKVClientProvider {}

impl BenchmarkClientProvider<SurrealKVClient> for SurrealKVClientProvider {
	async fn create_client(&self) -> Result<SurrealKVClient> {
		let mut opts = Options::new();
		opts.dir = PathBuf::from("surrealkv");
		Ok(SurrealKVClient {
			db: Store::new(opts)?,
		})
	}
}

pub(crate) struct SurrealKVClient {
	db: Store,
}

impl BenchmarkClient for SurrealKVClient {
	async fn read(&mut self, key: i32) -> Result<()> {
		let key = &key.to_ne_bytes();
		let txn = self.db.begin_with_mode(Mode::ReadOnly)?;
		let read: Option<Vec<u8>> = txn.get(key)?;
		assert!(read.is_some());
		Ok(())
	}

	async fn create(&mut self, key: i32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = serde_json::to_vec(record)?;
		let mut txn = self.db.begin_with_mode(Mode::WriteOnly)?;
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn update(&mut self, key: i32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = serde_json::to_vec(record)?;
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
