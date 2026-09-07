#![cfg(feature = "slatedb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use slatedb::config::{
	CompactionWorkerOptions, CompactorOptions, FlushOptions, FlushType, ScanOptions, Settings,
	SstBlockSize, WriteOptions,
};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::{CompactorBuilder, Db, DbTransaction, IsolationLevel};
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
		// Create a block cache for SST data blocks
		let block_cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
			max_capacity: memory / 8 * 7,
			shards: num_cpus::get(),
		}));
		// Create a meta cache for SST indexes and bloom filters
		let meta_cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
			max_capacity: memory / 8,
			shards: num_cpus::get(),
		}));
		// Keep data blocks from evicting indexes and filters
		let cache = SplitCache::new()
			.with_block_cache(Some(block_cache as Arc<dyn DbCache>))
			.with_meta_cache(Some(meta_cache as Arc<dyn DbCache>))
			.build();
		// Create a dedicated runtime for background work, so that
		// compaction and garbage collection do not steal CPU time
		// from the runtime executing benchmark operations
		let background = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(num_cpus::get().min(8))
			.thread_name("slatedb-background")
			.enable_all()
			.build()?;
		// Get a handle to the background runtime
		let handle = background.handle().clone();
		// Keep the background runtime alive for the process lifetime,
		// as dropping a runtime within an async context is not allowed
		std::mem::forget(background);
		// Configure the background compactor
		let compactor = CompactorBuilder::new(DATABASE_DIR, data_store.clone())
			.with_runtime(handle.clone())
			.with_options(CompactorOptions {
				// Check for new compaction work regularly
				poll_interval: Duration::from_secs(1),
				// Allow multiple compactions to run concurrently
				max_concurrent_compactions: num_cpus::get().min(8),
				// Configure the embedded compaction worker
				worker: Some(CompactionWorkerOptions {
					// Pick up scheduled compaction jobs quickly
					compactions_poll_interval: Duration::from_millis(500),
					// Allow multiple compactions to run concurrently
					max_concurrent_compactions: num_cpus::get().min(8),
					// Enable bloom filters for SSTs with 100+ keys
					min_filter_keys: 100,
					// Use other default worker settings
					..Default::default()
				}),
				// Use other default compactor settings
				..Default::default()
			});
		// Configure database settings
		let settings = Settings {
			// Flush the WAL buffer and check memtable sizes regularly.
			// Commits which await durability wait for the next flush, so
			// when sync is enabled we flush often, grouping concurrent
			// commits into a single write to the object store.
			flush_interval: Some(match options.sync {
				true => Duration::from_millis(1),
				false => Duration::from_millis(100),
			}),
			// Set the L0 SST size to 256MB
			l0_sst_size_bytes: 256 * 1024 * 1024,
			// Allow more L0 SSTs before memtable flushes stall
			l0_max_ssts: 24,
			// Random keys make every L0 SST span the whole keyspace,
			// so the per-key overlap limit is the gate which actually
			// stalls memtable flushes, and must be raised in tandem
			l0_max_ssts_per_key: 24,
			// Set backpressure limit to 2GB
			max_unflushed_bytes: 2 * 1024 * 1024 * 1024,
			// Enable bloom filters for SSTs with 100+ keys
			min_filter_keys: 100,
			// Store SSTs uncompressed, matching RocksDB which leaves
			// the hot L0 and L1 levels and all blob files uncompressed
			compression_codec: None,
			// The compactor is configured through the builder instead
			compactor_options: None,
			// Use other default settings
			..Default::default()
		};
		// Create the database builder
		let builder = Db::builder(DATABASE_DIR, data_store);
		// Apply custom settings
		let builder = builder.with_settings(settings);
		// Setup the separate WAL object store
		let builder = builder.with_wal_object_store(wal_store);
		// Configure the split block and meta cache
		let builder = builder.with_db_cache(Arc::new(cache));
		// Run the compactor on the background runtime
		let builder = builder.with_compactor_builder(compactor);
		// Run the garbage collector on the background runtime
		let builder = builder.with_gc_runtime(handle);
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
				..Default::default()
			},
		})
	}
}

pub(crate) struct SlateDBClient {
	db: Arc<Db>,
	opts: WriteOptions,
}

impl BenchmarkClient for SlateDBClient {
	// The return type when reading a row
	type ReadRow = BenchValue;

	async fn shutdown(&self) -> Result<()> {
		// Close the database
		self.db.close().await?;
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Ok
		Ok(())
	}

	async fn compact(&self) -> Result<()> {
		// SlateDB does not expose a manual full-compaction API,
		// so this is best-effort: persist all in-memory data and
		// let the background compactor process the L0 SSTs.
		// Flush the WAL to object storage
		self.db.flush().await?;
		// Flush the memtables to L0 object storage
		self.db
			.flush_with_options(FlushOptions {
				flush_type: FlushType::MemTable,
			})
			.await?;
		// Ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.create_bytes(&key.to_ne_bytes(), val).await
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.create_bytes(&key.into_bytes(), val).await
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		self.read_bytes(&key.to_ne_bytes()).await
	}

	async fn read_string(&self, key: String) -> Result<BenchValue> {
		self.read_bytes(&key.into_bytes()).await
	}

	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.update_bytes(&key.to_ne_bytes(), val).await
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
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
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		let pairs_iter = key_vals.map(|(key, val)| {
			let val = val.encode()?;
			Ok((key.to_ne_bytes().to_vec(), val))
		});
		self.batch_create_bytes(pairs_iter).await
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		let pairs_iter = key_vals.map(|(key, val)| {
			let val = val.encode()?;
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
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		let pairs_iter = key_vals.map(|(key, val)| {
			let val = val.encode()?;
			Ok((key.to_ne_bytes().to_vec(), val))
		});
		self.batch_update_bytes(pairs_iter).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		let pairs_iter = key_vals.map(|(key, val)| {
			let val = val.encode()?;
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
	/// Commit a transaction with the configured write options
	async fn commit(&self, txn: DbTransaction) -> Result<()> {
		// Commit the transaction
		txn.commit_with_options(&self.opts).await?;
		Ok(())
	}

	async fn create_bytes(&self, key: &[u8], val: BenchValue) -> Result<()> {
		// Serialise the value
		let val = val.encode()?;
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		txn.put(key, val)?;
		self.commit(txn).await?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<BenchValue> {
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Get the data
		let res = txn.get(key).await?;
		// Check the value exists
		assert!(res.is_some());
		// Deserialise the value
		let val = BenchValue::decode(res.unwrap().as_ref())?;
		// All ok
		Ok(black_box(val))
	}

	async fn update_bytes(&self, key: &[u8], val: BenchValue) -> Result<()> {
		// Serialise the value
		let val = val.encode()?;
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		txn.put(key, val)?;
		self.commit(txn).await?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Process the data
		txn.delete(key)?;
		self.commit(txn).await?;
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
		self.commit(txn).await?;
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
			let val = BenchValue::decode(res.unwrap().as_ref())?;
			// Use the value
			black_box(val);
		}
		// All ok
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
		self.commit(txn).await?;
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
		self.commit(txn).await?;
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
		// Configure scan options
		let opts = ScanOptions {
			// Read ahead by 2MB when fetching blocks
			read_ahead_bytes: 2 * 1024 * 1024,
			// Fetch blocks concurrently when reading ahead
			max_fetch_tasks: 4,
			// Store any fetched blocks in the block cache
			cache_blocks: true,
			// Use other default scan settings
			..Default::default()
		};
		// Create a new transaction
		let txn = self.db.begin(IsolationLevel::Snapshot).await?;
		// Create an iterator over the full range
		let mut iter = txn.scan_with_options(.., &opts).await?;
		// Skip the necessary number of entries
		let mut skipped = 0;
		while skipped < s {
			if iter.next().await?.is_none() {
				return Ok(0);
			}
			skipped += 1;
		}
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// We use a while loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// otherwise the loop is optimised out by the compiler
				// when calling `count` at the end.
				let mut count = 0;
				while let Some(item) = iter.next().await? {
					black_box(item.key);
					count += 1;
					if count >= l {
						break;
					}
				}
				Ok(count)
			}
			Projection::Full => {
				// We use a while loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// otherwise the loop is optimised out by the compiler
				// when calling `count` at the end.
				let mut count = 0;
				while let Some(item) = iter.next().await? {
					black_box(item.value);
					count += 1;
					if count >= l {
						break;
					}
				}
				Ok(count)
			}
			Projection::Count => {
				// Count entries without processing values
				let mut count = 0;
				while iter.next().await?.is_some() {
					count += 1;
					if count >= l {
						break;
					}
				}
				Ok(count)
			}
		}
	}
}
