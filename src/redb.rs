#![cfg(feature = "redb")]

use anyhow::Result;
use redb::{Database, TableDefinition};
use std::sync::Arc;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Record;
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

	async fn create_u32(&self, key: u32, record: Record) -> Result<()> {
		self.create_bytes(&key.to_ne_bytes(), record).await
	}

	async fn create_string(&self, key: String, record: Record) -> Result<()> {
		self.create_bytes(&key.into_bytes(), record).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read_bytes(&key.to_ne_bytes()).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read_bytes(&key.into_bytes()).await
	}

	async fn update_u32(&self, key: u32, record: Record) -> Result<()> {
		self.update_bytes(&key.to_ne_bytes(), record).await
	}

	async fn update_string(&self, key: String, record: Record) -> Result<()> {
		self.update_bytes(&key.into_bytes(), record).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete_bytes(&key.to_ne_bytes()).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete_bytes(&key.into_bytes()).await
	}
}

impl ReDBClient {
	async fn create_bytes(&self, key: &[u8], record: Record) -> Result<()> {
		let val = bincode::serialize(&record)?;
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

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.0.begin_read()?;
		// Open the database table
		let tab = txn.open_table(TABLE)?;
		// Process the data
		let read: Option<_> = tab.get(key.as_ref())?;
		assert!(read.is_some());
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], record: Record) -> Result<()> {
		let val = bincode::serialize(&record)?;
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

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
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
}
