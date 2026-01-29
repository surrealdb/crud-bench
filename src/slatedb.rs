#![cfg(feature = "slatedb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use serde_json::Value;
use slatedb::config::{CompressionCodec, Settings, SstBlockSize, WriteOptions};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::{Db, IsolationLevel};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const DATABASE_DIR: &str = "slatedb";
const DATA_DIR: &str = "slatedb/data";
const WAL_DIR: &str = "slatedb/wal";

/// Calculate SlateDB specific memory allocation
fn calculate_slatedb_memory() -> u64 {
	// Load the system memory
	let memory = Config::new();
	// Return configuration in bytes
	memory.cache_gb * 1024 * 1024 * 1024
}

pub(crate) struct SlateDBClientProvider {
	db: Arc<Db>,
	sync: bool,
}

impl BenchmarkEngine<SlateDBClient> for SlateDBClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Create the database directories
		std::fs::create_dir_all(DATA_DIR)?;
		std::fs::create_dir_all(WAL_DIR)?;
		// Calculate memory allocation
		let memory = calculate_slatedb_memory();
		// Create object store for data
		let data_store = Arc::new(LocalFileSystem::new_with_prefix(DATA_DIR)?);
		// Create object store for WAL
		let wal_store = Arc::new(LocalFileSystem::new_with_prefix(WAL_DIR)?);
		// Create a custom block cache
		let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
			max_capacity: memory,
			shards: num_cpus::get(),
		}));
		// Configure database settings
		let settings = Settings {
			// Flush the WAL regularly
			flush_interval: Some(Duration::from_millis(100)),
			// Set the L0 SST size to 256MB
			l0_sst_size_bytes: 256 * 1024 * 1024,
			// Set max L0 SSTs before compaction
			l0_max_ssts: 8,
			// Set backpressure limit to 512MB
			max_unflushed_bytes: 512 * 1024 * 1024,
			// Enable bloom filters for SSTs with 100+ keys
			min_filter_keys: 100,
			// Set bloom filter bits per key
			filter_bits_per_key: 10,
			// Enable Snappy compression
			compression_codec: Some(CompressionCodec::Snappy),
			// Use other default settings
			..Default::default()
		};
		// Create the database builder
		let builder = Db::builder(DATABASE_DIR, data_store);
		// Apply custom settings
		let builder = builder.with_settings(settings);
		// Setup the separate WAL object store
		let builder = builder.with_wal_object_store(wal_store);
		// Configure custom memory cache
		let builder = builder.with_memory_cache(cache);
		// Use a larger block size for better sequential performance
		let builder = builder.with_sst_block_size(SstBlockSize::Block64Kib);
		// Open the database
		let db = builder.build().await?;
		// Create the store
		Ok(Self {
			db: Arc::new(db),
			sync: options.sync,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<SlateDBClient> {
		Ok(SlateDBClient {
			db: self.db.clone(),
			opts: WriteOptions {
				await_durable: self.sync,
			},
		})
	}
}

pub(crate) struct SlateDBClient {
	db: Arc<Db>,
	opts: WriteOptions,
}

impl BenchmarkClient for SlateDBClient {
	async fn shutdown(&self) -> Result<()> {
		// Close the database
		self.db.close().await?;
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Ok
		Ok(())
	}

	async fn compact(&self) -> Result<()> {
		// SlateDB handles compaction automatically
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

impl SlateDBClient {
	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Get the data
		let res = self.db.get(key).await?;
		// Check the value exists
		assert!(res.is_some());
		// Deserialise the value
		black_box(res.unwrap());
		// All ok
		Ok(())
	}

	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		txn.put(key, val)?;
		txn.commit_with_options(&self.opts).await?;
		//
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		txn.put(key, val)?;
		txn.commit_with_options(&self.opts).await?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		txn.delete(key)?;
		txn.commit_with_options(&self.opts).await?;
		Ok(())
	}

	async fn batch_read_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		for key in keys {
			// Get the current value
			let res = txn.get(&key).await?;
			// Check the value exists
			assert!(res.is_some());
			// Deserialise the value
			black_box(res.unwrap());
		}
		// All ok
		Ok(())
	}

	async fn batch_create_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			txn.put(&key, val)?;
		}
		// Commit the batch
		txn.commit_with_options(&self.opts).await?;
		Ok(())
	}

	async fn batch_update_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			txn.put(&key, val)?;
		}
		// Commit the batch
		txn.commit_with_options(&self.opts).await?;
		Ok(())
	}

	async fn batch_delete_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		for key in keys {
			txn.delete(&key)?;
		}
		// Commit the batch
		txn.commit_with_options(&self.opts).await?;
		Ok(())
	}

	async fn scan_bytes(&self, scan: &Scan) -> Result<usize> {
		// Conditional scans are not supported
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(usize::MAX);
		let p = scan.projection()?;
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Create an iterator
		let mut iter = txn.scan::<Vec<u8>, _>(..).await?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// We use a while loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// otherwise the loop is optimised out by the compiler
				// when calling `count` at the end.
				let mut count = 0;
				let mut scanned = 0;
				while let Ok(Some(item)) = iter.next().await {
					if scanned >= s {
						black_box(item.key);
						count += 1;
						if count >= l {
							break;
						}
					}
					scanned += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				// We use a while loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// otherwise the loop is optimised out by the compiler
				// when calling `count` at the end.
				let mut count = 0;
				let mut scanned = 0;
				while let Ok(Some(item)) = iter.next().await {
					if scanned >= s {
						black_box(item.value);
						count += 1;
						if count >= l {
							break;
						}
					}
					scanned += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				// We use a while loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// otherwise the loop is optimised out by the compiler
				// when calling `count` at the end.
				let mut count = 0;
				let mut scanned = 0;
				while let Ok(Some(_)) = iter.next().await {
					if scanned >= s {
						count += 1;
						if count >= l {
							break;
						}
					}
					scanned += 1;
				}
				Ok(count)
			}
		}
	}
}
