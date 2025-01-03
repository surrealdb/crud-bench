#![cfg(feature = "redb")]

use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::KeyType;
use anyhow::Result;
use redb::{Database, Durability, TableDefinition};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

const DATABASE_DIR: &str = "redb";

const TABLE: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new("test");

pub(crate) struct ReDBClientProvider(Arc<Database>);

impl BenchmarkEngine<ReDBClient> for ReDBClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, _endpoint: Option<&str>) -> Result<Self> {
		// Cleanup the data directory
		tokio::fs::remove_file(DATABASE_DIR).await.ok();
		// Create the store
		Ok(Self(Arc::new(Database::create(DATABASE_DIR)?)))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<ReDBClient> {
		Ok(ReDBClient(self.0.clone()))
	}
}

pub(crate) struct ReDBClient(Arc<Database>);

impl BenchmarkClient for ReDBClient {
	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		tokio::fs::remove_file(DATABASE_DIR).await.ok();
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
}

impl ReDBClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.0.begin_write()?;
		// Let the OS handle syncing to disk
		txn.set_durability(Durability::Eventual);
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
		let res: Option<_> = tab.get(key.as_ref())?;
		assert!(res.is_some());
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.0.begin_write()?;
		// Let the OS handle syncing to disk
		txn.set_durability(Durability::Eventual);
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
		let mut txn = self.0.begin_write()?;
		// Let the OS handle syncing to disk
		txn.set_durability(Durability::Eventual);
		// Open the database table
		let mut tab = txn.open_table(TABLE)?;
		// Process the data
		tab.remove(key.as_ref())?;
		drop(tab);
		txn.commit()?;
		Ok(())
	}
}
