#![cfg(feature = "redb")]

use anyhow::Error;
use anyhow::Result;
use redb::{Database, TableDefinition};
use std::sync::Arc;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};

const TABLE: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new("test");

pub(crate) struct ReDBClientProvider {
	db: Arc<Database>,
}

impl ReDBClientProvider {
	pub(crate) async fn setup() -> Result<Self, Error> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("redb");
		// Create the store
		Ok(Self {
			db: Arc::new(Database::create("redb")?),
		})
	}
}

impl BenchmarkEngine<ReDBClient> for ReDBClientProvider {
	async fn create_client(&self, _: Option<String>) -> Result<ReDBClient> {
		Ok(ReDBClient {
			db: self.db.clone(),
		})
	}
}

pub(crate) struct ReDBClient {
	db: Arc<Database>,
}

impl BenchmarkClient for ReDBClient {
	async fn shutdown(&mut self) -> Result<()> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("redb");
		// Ok
		Ok(())
	}

	async fn create(&mut self, key: i32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = serde_json::to_vec(record)?;
		// Create a new transaction
		let txn = self.db.begin_write()?;
		// Open the database table
		let mut tab = txn.open_table(TABLE)?;
		// Process the data
		tab.insert(key.as_ref(), val)?;
		drop(tab);
		txn.commit()?;
		Ok(())
	}

	async fn read(&mut self, key: i32) -> Result<()> {
		let key = &key.to_ne_bytes();
		// Create a new transaction
		let txn = self.db.begin_read()?;
		// Open the database table
		let tab = txn.open_table(TABLE)?;
		// Process the data
		let read: Option<_> = tab.get(key.as_ref())?;
		assert!(read.is_some());
		Ok(())
	}

	async fn update(&mut self, key: i32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = serde_json::to_vec(record)?;
		// Create a new transaction
		let txn = self.db.begin_write()?;
		// Open the database table
		let mut tab = txn.open_table(TABLE)?;
		// Process the data
		tab.insert(key.as_ref(), val)?;
		drop(tab);
		txn.commit()?;
		Ok(())
	}

	async fn delete(&mut self, key: i32) -> Result<()> {
		let key = &key.to_ne_bytes();
		// Create a new transaction
		let txn = self.db.begin_write()?;
		// Open the database table
		let mut tab = txn.open_table(TABLE)?;
		// Process the data
		tab.remove(key.as_ref())?;
		drop(tab);
		txn.commit()?;
		Ok(())
	}
}
