#![cfg(feature = "surrealkv")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use serde_json::Value;
use std::cmp::max;
use std::hint::black_box;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use surrealkv::Durability;
use surrealkv::Mode::{ReadOnly, ReadWrite};
use surrealkv::Options;
use surrealkv::Store;
use sysinfo::System;

const DATABASE_DIR: &str = "surrealkv";

const MIN_CACHE_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB

pub(crate) struct SurrealKVClientProvider {
	store: Arc<Store>,
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
		// Load the system attributes
		let system = System::new_all();
		// Get the total system memory
		let memory = system.total_memory();
		// Divide the total memory into half
		let memory = memory.saturating_div(2);
		// Subtract 1 GiB from the memory size
		let memory = memory.saturating_sub(1024 * 1024 * 1024);
		// Divide the total memory by a 4KiB value size
		let cache = memory.saturating_div(4096);
		// Calculate a good cache memory size
		let cache = max(cache, MIN_CACHE_SIZE);
		// Configure custom options
		let mut opts = Options::new();
		// Disable versioning
		opts.enable_versions = false;
		// Enable disk persistence
		opts.disk_persistence = options.persisted;
		// Set the directory location
		opts.dir = PathBuf::from(DATABASE_DIR);
		// Set the cache to 250,000 entries
		opts.max_value_cache_size = cache;
		// Create the store
		Ok(Self {
			store: Arc::new(Store::new(opts)?),
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
	db: Arc<Store>,
	sync: bool,
}

impl BenchmarkClient for SurrealKVClient {
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

	async fn batch_create_u32(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut pairs = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			pairs.push((key.to_ne_bytes().to_vec(), val));
		}
		self.batch_create_bytes(pairs).await
	}

	async fn batch_create_string(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut pairs = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			pairs.push((key.into_bytes(), val));
		}
		self.batch_create_bytes(pairs).await
	}

	async fn batch_read_u32(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = u32> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key.to_ne_bytes().to_vec());
		}
		self.batch_read_bytes(keys_vec).await
	}

	async fn batch_read_string(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = String> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key.into_bytes());
		}
		self.batch_read_bytes(keys_vec).await
	}

	async fn batch_update_u32(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut pairs = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			pairs.push((key.to_ne_bytes().to_vec(), val));
		}
		self.batch_update_bytes(pairs).await
	}

	async fn batch_update_string(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut pairs = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
			pairs.push((key.into_bytes(), val));
		}
		self.batch_update_bytes(pairs).await
	}

	async fn batch_delete_u32(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = u32> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key.to_ne_bytes().to_vec());
		}
		self.batch_delete_bytes(keys_vec).await
	}

	async fn batch_delete_string(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = String> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key.into_bytes());
		}
		self.batch_delete_bytes(keys_vec).await
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
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadOnly)?;
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
		txn.commit()?;
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
		txn.commit()?;
		Ok(())
	}

	async fn batch_create_bytes(&self, key_vals: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Set the transaction durability
		txn.set_durability(if self.sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Process the data
		for (key, val) in key_vals {
			txn.set(&key, &val)?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_read_bytes(&self, keys: Vec<Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadOnly)?;
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

	async fn batch_update_bytes(&self, key_vals: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadWrite)?;
		// Set the transaction durability
		txn.set_durability(if self.sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Process the data
		for (key, val) in key_vals {
			txn.set(&key, &val)?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_delete_bytes(&self, keys: Vec<Vec<u8>>) -> Result<()> {
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
		let t = scan.limit.map(|l| s + l);
		let p = scan.projection()?;
		// Create a new transaction
		let mut txn = self.db.begin_with_mode(ReadOnly)?;
		let beg = [0u8].as_slice();
		let end = [255u8].as_slice();
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// Create an iterator starting at the beginning
				let iter = txn.keys(beg..end, t);
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				// Create an iterator starting at the beginning
				let iter = txn.scan(beg..end, t);
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					assert!(v.is_ok());
					black_box(v.unwrap().1);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				Ok(txn
					.keys(beg..end, t)
					.skip(s) // Skip the first `offset` entries
					.take(l) // Take the next `limit` entries
					.count())
			}
		}
	}
}
