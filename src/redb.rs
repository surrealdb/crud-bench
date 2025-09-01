#![cfg(feature = "redb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use redb::{Database, Durability, ReadableTable, TableDefinition};
use serde_json::Value;
use std::cmp::max;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;

const DATABASE_DIR: &str = "redb";

const MIN_CACHE_SIZE: u64 = 512 * 1024 * 1024;

const TABLE: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new("test");

pub(crate) struct ReDBClientProvider(Arc<Database>);

impl BenchmarkEngine<ReDBClient> for ReDBClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, _options: &Benchmark) -> Result<Self> {
		// Cleanup the data directory
		std::fs::remove_file(DATABASE_DIR).ok();
		// Load the system attributes
		let system = System::new_all();
		// Get the total system memory
		let memory = system.total_memory();
		// Calculate a good cache memory size
		let memory = max(memory / 2, MIN_CACHE_SIZE);
		// Configure and create the database
		let db = Database::builder()
			// Set the cache size to 512 MiB
			.set_cache_size(memory as usize)
			// Create the database directory
			.create(DATABASE_DIR)?;
		// Create the store
		Ok(Self(Arc::new(db)))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<ReDBClient> {
		Ok(ReDBClient {
			db: self.0.clone(),
		})
	}
}

pub(crate) struct ReDBClient {
	db: Arc<Database>,
}

impl BenchmarkClient for ReDBClient {
	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		std::fs::remove_file(DATABASE_DIR).ok();
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

impl ReDBClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Clone the datastore
		let db = self.db.clone();
		// Execute on the blocking threadpool
		affinitypool::spawn_local(|| -> Result<_> {
			// Serialise the value
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			// Create a new transaction
			let mut txn = db.begin_write()?;
			// Let the OS handle syncing to disk
			txn.set_durability(Durability::Eventual);
			// Open the database table
			let mut tab = txn.open_table(TABLE)?;
			// Process the data
			tab.insert(key, val)?;
			drop(tab);
			txn.commit()?;
			Ok(())
		})
		.await
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Clone the datastore
		let db = self.db.clone();
		// Execute on the blocking threadpool
		affinitypool::spawn_local(|| -> Result<_> {
			// Create a new transaction
			let txn = db.begin_read()?;
			// Open the database table
			let tab = txn.open_table(TABLE)?;
			// Process the data
			let res: Option<_> = tab.get(key)?;
			// Check the value exists
			assert!(res.is_some());
			// Deserialise the value
			black_box(res.unwrap().value());
			// All ok
			Ok(())
		})
		.await
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Clone the datastore
		let db = self.db.clone();
		// Execute on the blocking threadpool
		affinitypool::spawn_local(|| -> Result<_> {
			// Serialise the value
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			// Create a new transaction
			let mut txn = db.begin_write()?;
			// Let the OS handle syncing to disk
			txn.set_durability(Durability::Eventual);
			// Open the database table
			let mut tab = txn.open_table(TABLE)?;
			// Process the data
			tab.insert(key, val)?;
			drop(tab);
			txn.commit()?;
			Ok(())
		})
		.await
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Clone the datastore
		let db = self.db.clone();
		// Execute on the blocking threadpool
		affinitypool::spawn_local(|| -> Result<_> {
			// Create a new transaction
			let mut txn = db.begin_write()?;
			// Let the OS handle syncing to disk
			txn.set_durability(Durability::Eventual);
			// Open the database table
			let mut tab = txn.open_table(TABLE)?;
			// Process the data
			tab.remove(key)?;
			drop(tab);
			txn.commit()?;
			Ok(())
		})
		.await
	}

	async fn scan_bytes(&self, scan: &Scan) -> Result<usize> {
		// Contional scans are not supported
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(usize::MAX);
		let p = scan.projection()?;
		// Clone the datastore
		let db = self.db.clone();
		// Execute on the blocking threadpool
		affinitypool::spawn_local(|| -> Result<_> {
			// Create a new transaction
			let txn = db.begin_read()?;
			// Open the database table
			let tab = txn.open_table(TABLE)?;
			// Create an iterator starting at the beginning
			let iter = tab.iter()?;
			// Perform the relevant projection scan type
			match p {
				Projection::Id => {
					// We use a for loop to iterate over the results, while
					// calling black_box internally. This is necessary as
					// an iterator with `filter_map` or `map` is optimised
					// out by the compiler when calling `count` at the end.
					let mut count = 0;
					for v in iter.skip(s).take(l) {
						black_box(v.unwrap().1.value());
						count += 1;
					}
					Ok(count)
				}
				Projection::Full => {
					// We use a for loop to iterate over the results, while
					// calling black_box internally. This is necessary as
					// an iterator with `filter_map` or `map` is optimised
					// out by the compiler when calling `count` at the end.
					let mut count = 0;
					for v in iter.skip(s).take(l) {
						black_box(v.unwrap().1.value());
						count += 1;
					}
					Ok(count)
				}
				Projection::Count => {
					Ok(iter
						.skip(s) // Skip the first `offset` entries
						.take(l) // Take the next `limit` entries
						.count())
				}
			}
		})
		.await
	}
}
