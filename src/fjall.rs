#![cfg(feature = "fjall")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{bail, Result};
use fjall::{
	Config, KvSeparationOptions, PartitionCreateOptions, PersistMode, TransactionalKeyspace,
	TxPartitionHandle,
};
use serde_json::Value;
use std::cmp::max;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;

const DATABASE_DIR: &str = "fjall";

const MIN_CACHE_SIZE: u64 = 256 * 1024 * 1024;

const DURABILITY: Option<PersistMode> = Some(PersistMode::Buffer);

pub(crate) struct FjallClientProvider {
	keyspace: Arc<TransactionalKeyspace>,
	partition: Arc<TxPartitionHandle>,
}

impl BenchmarkEngine<FjallClient> for FjallClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, _options: &Benchmark) -> Result<Self> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Load the system attributes
		let system = System::new_all();
		// Get the total system memory
		let memory = system.total_memory();
		// Calculate a good cache memory size
		let memory = max(memory / 4, MIN_CACHE_SIZE);
		// Configure the key-value separation
		let blobopts = KvSeparationOptions::default()
			// Separate values if larger than 1 KiB
			.separation_threshold(1024);
		// Configure and create the keyspace
		let keyspace = Config::new(DATABASE_DIR)
			// Fsync data every 100 milliseconds
			.fsync_ms(Some(100))
			// Handle transaction flushed automatically
			.manual_journal_persist(false)
			// Set the amount of data to build up in memory
			.max_write_buffer_size(u64::MAX)
			// Set the cache size to 512 MiB
			.cache_size(memory)
			// Open a transactional keyspace
			.open_transactional()?;
		// Configure and create the partition
		let options = PartitionCreateOptions::default()
			// Set the data block size to 32 KiB
			.block_size(16 * 1_024)
			// Set the max memtable size to 256 MiB
			.max_memtable_size(256 * 1_024 * 1_024)
			// Separate values if larger than 4 KiB
			.with_kv_separation(blobopts);
		// Create a default data partition
		let partition = keyspace.open_partition("default", options)?;
		// Create the store
		Ok(Self {
			keyspace: Arc::new(keyspace),
			partition: Arc::new(partition),
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<FjallClient> {
		Ok(FjallClient {
			keyspace: self.keyspace.clone(),
			partition: self.partition.clone(),
		})
	}
}

pub(crate) struct FjallClient {
	keyspace: Arc<TransactionalKeyspace>,
	partition: Arc<TxPartitionHandle>,
}

impl BenchmarkClient for FjallClient {
	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
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

impl FjallClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.keyspace.write_tx().durability(DURABILITY);
		// Process the data
		txn.insert(&self.partition, key, val);
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.keyspace.read_tx();
		// Process the data
		let res = txn.get(&self.partition, key)?;
		// Check the value exists
		assert!(res.is_some());
		// Deserialise the value
		black_box(res.unwrap());
		// All ok
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.keyspace.write_tx().durability(DURABILITY);
		// Process the data
		txn.insert(&self.partition, key, val);
		txn.commit()?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.keyspace.write_tx().durability(DURABILITY);
		// Process the data
		txn.remove(&self.partition, key);
		txn.commit()?;
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
		let p = scan.projection()?;
		// Create a new transaction
		let txn = self.keyspace.read_tx();
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// Create an iterator starting at the beginning
				let iter = txn.keys(&self.partition);
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.unwrap());
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				// Create an iterator starting at the beginning
				let iter = txn.iter(&self.partition);
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.unwrap().1);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				Ok(txn
					.keys(&self.partition)
					.skip(s) // Skip the first `offset` entries
					.take(l) // Take the next `limit` entries
					.count())
			}
		}
	}
}
