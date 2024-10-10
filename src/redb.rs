#![cfg(feature = "redb")]

use anyhow::Result;
use redb::{Database, TableDefinition};
use std::sync::Arc;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::KeyType;

const TABLE: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new("test");

pub(crate) struct ReDBClientProvider(Arc<Database>);

impl BenchmarkEngine<ReDBClient> for ReDBClientProvider {
	async fn setup(_: KeyType) -> Result<Self> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("redb");
		// Create the store
		Ok(Self(Arc::new(Database::create("redb")?)))
	}

	async fn create_client(&self, _: Option<String>) -> Result<ReDBClient> {
		Ok(ReDBClient(self.0.clone()))
	}
}

pub(crate) struct ReDBClient(Arc<Database>);

impl BenchmarkClient for ReDBClient {
	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("redb");
		// Ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(record)?;
		// Create a new transaction
		let txn = self.0.begin_write()?;
		// Open the database table
		let mut tab = txn.open_table(TABLE)?;
		// Process the data
		tab.insert(key.as_ref(), val)?;
		drop(tab);
		txn.commit()?;
		Ok(())
	}

	async fn create_string(&self, key: String, record: &Record) -> Result<()> {
		todo!()
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let key = &key.to_ne_bytes();
		// Create a new transaction
		let txn = self.0.begin_read()?;
		// Open the database table
		let tab = txn.open_table(TABLE)?;
		// Process the data
		let read: Option<_> = tab.get(key.as_ref())?;
		assert!(read.is_some());
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		todo!()
	}

	async fn update_u32(&self, key: u32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(record)?;
		// Create a new transaction
		let txn = self.0.begin_write()?;
		// Open the database table
		let mut tab = txn.open_table(TABLE)?;
		// Process the data
		tab.insert(key.as_ref(), val)?;
		drop(tab);
		txn.commit()?;
		Ok(())
	}

	async fn update_string(&self, key: String, record: &Record) -> Result<()> {
		todo!()
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let key = &key.to_ne_bytes();
		// Create a new transaction
		let txn = self.0.begin_write()?;
		// Open the database table
		let mut tab = txn.open_table(TABLE)?;
		// Process the data
		tab.remove(key.as_ref())?;
		drop(tab);
		txn.commit()?;
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		todo!()
	}
}
