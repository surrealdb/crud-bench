#![cfg(feature = "surrealkv")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use serde_json::Value;
use std::hint::black_box;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use surrealkv::Durability;
use surrealkv::Mode::{ReadOnly, ReadWrite};
use surrealkv::Tree;
use surrealkv::TreeBuilder;

const DATABASE_DIR: &str = "surrealkv";

const BLOCK_SIZE: usize = 64 * 1024;

/// Calculate SurrealKV specific memory allocation
fn calculate_surrealkv_memory() -> u64 {
	// Load the system memory
	let memory = Config::new();
	// Return configuration
	memory.cache_gb * 1024 * 1024 * 1024
}

pub(crate) struct SurrealKVClientProvider {
	store: Arc<Tree>,
	sync: bool,
}

impl BenchmarkEngine<SurrealKVClient> for SurrealKVClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Calculate memory allocation
		let block_cache_bytes = calculate_surrealkv_memory();
		// Configure custom options
		let builder = TreeBuilder::new();
		// Enable max memtable size
		let builder = builder.with_max_memtable_size(256 * 1024 * 1024);
		// Enable the block cache capacity
		let builder = builder.with_block_cache_capacity(block_cache_bytes);
		// Disable versioned queries
		let builder = builder.with_versioning(false, 0);
		// Enable separated keys and values
		let builder = builder.with_enable_vlog(true);
		// Set the block size to 64 KiB
		let builder = builder.with_block_size(BLOCK_SIZE);
		// Set the vlog threshold to 1KB
		let builder = builder.with_vlog_value_threshold(1024);
		// Set the directory location
		let builder = builder.with_path(PathBuf::from(DATABASE_DIR));
		// Create the datastore
		let store = builder.build()?;
		// Create the store
		Ok(Self {
			store: Arc::new(store),
			sync: options.sync,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<SurrealKVClient> {
		Ok(SurrealKVClient {
			db: self.store.clone(),
			sync: self.sync,
		})
	}
}

pub(crate) struct SurrealKVClient {
	db: Arc<Tree>,
	sync: bool,
}

impl BenchmarkClient for SurrealKVClient {
	async fn shutdown(&self) -> Result<()> {
		// Close the database
		self.db.close().await?;
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

	async fn scan_u32(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		self.scan_bytes(scan).await
	}

	async fn scan_string(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		self.scan_bytes(scan).await
	}

	async fn batch_create_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		let pairs_iter = key_vals.map(|(key, val)| {
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			Ok((key.to_ne_bytes().to_vec(), val))
		});
		self.batch_create_bytes(pairs_iter).await
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		let pairs_iter = key_vals.map(|(key, val)| {
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			Ok((key.into_bytes(), val))
		});
		self.batch_create_bytes(pairs_iter).await
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		let keys_iter = keys.map(|key| key.to_ne_bytes().to_vec());
		self.batch_read_bytes(keys_iter).await
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		let keys_iter = keys.map(|key| key.into_bytes());
		self.batch_read_bytes(keys_iter).await
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		let pairs_iter = key_vals.map(|(key, val)| {
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			Ok((key.to_ne_bytes().to_vec(), val))
		});
		self.batch_update_bytes(pairs_iter).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		let pairs_iter = key_vals.map(|(key, val)| {
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			Ok((key.into_bytes(), val))
		});
		self.batch_update_bytes(pairs_iter).await
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		let keys_iter = keys.map(|key| key.to_ne_bytes().to_vec());
		self.batch_delete_bytes(keys_iter).await
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		let keys_iter = keys.map(|key| key.into_bytes());
		self.batch_delete_bytes(keys_iter).await
	}
}

impl SurrealKVClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Set the transaction durability
		txn.set_durability(if self.sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Process the data
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin_with_mode(ReadOnly)?;
		// Process the data
		let res = txn.get(key)?;
		// Check the value exists
		assert!(res.is_some());
		// Deserialise the value
		black_box(res.unwrap());
		// All ok
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Set the transaction durability
		txn.set_durability(if self.sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Process the data
		txn.set(key, &val)?;
		txn.commit().await?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Set the transaction durability
		txn.set_durability(if self.sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Process the data
		txn.delete(key)?;
		txn.commit().await?;
		Ok(())
	}

	async fn batch_create_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Set the transaction durability
		txn.set_durability(if self.sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			txn.set(&key, &val)?;
		}
		// Commit the batch
		txn.commit().await?;
		Ok(())
	}

	async fn batch_read_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin_with_mode(ReadOnly)?;
		// Process the data
		for key in keys {
			// Get the current value
			let res = txn.get(&key)?;
			// Check the value exists
			assert!(res.is_some());
			// Deserialise the value
			black_box(res.unwrap());
		}
		// All ok
		Ok(())
	}

	async fn batch_update_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Set the transaction durability
		txn.set_durability(if self.sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			txn.set(&key, &val)?;
		}
		// Commit the batch
		txn.commit().await?;
		Ok(())
	}

	async fn batch_delete_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Set the transaction durability
		txn.set_durability(if self.sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Process the data
		for key in keys {
			txn.delete(&key)?;
		}
		// Commit the batch
		txn.commit().await?;
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
		let txn = self.db.begin_with_mode(ReadOnly)?;
		let beg = [0u8].as_slice();
		let end = [255u8].as_slice();
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// Create an iterator starting at the beginning
				let mut iter = txn.range(beg, end)?;
				iter.seek_first().unwrap();
				let mut count = 0;
				let mut skipped = 0;
				while iter.next()? {
					if !iter.valid() {
						break;
					}
					if skipped < s {
						skipped += 1;
						continue;
					}
					if count >= l {
						break;
					}
					black_box(iter.key());
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				// Create an iterator starting at the beginning
				let mut iter = txn.range(beg, end)?;
				iter.seek_first().unwrap();
				let mut count = 0;
				let mut skipped = 0;
				while iter.next()? {
					if !iter.valid() {
						break;
					}
					if skipped < s {
						skipped += 1;
						continue;
					}
					if count >= l {
						break;
					}
					let _ = black_box(iter.value());
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				// Create an iterator starting at the beginning
				let mut iter = txn.range(beg, end)?;
				iter.seek_first().unwrap();
				let mut count = 0;
				let mut skipped = 0;
				while iter.next()? {
					if !iter.valid() {
						break;
					}
					if skipped < s {
						skipped += 1;
						continue;
					}
					if count >= l {
						break;
					}
					black_box(iter.key());
					count += 1;
				}
				Ok(count)
			}
		}
	}
}
