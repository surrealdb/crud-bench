#![cfg(feature = "rocksdb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use log::error;
use rocksdb::{
	BlockBasedOptions, BottommostLevelCompaction, Cache, CompactOptions, DBCompactionStyle,
	DBCompressionType, FlushOptions, IteratorMode, LogLevel, OptimisticTransactionDB,
	OptimisticTransactionOptions, Options, ReadOptions, WaitForCompactOptions, WriteOptions,
};
use serde_json::Value;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const DATABASE_DIR: &str = "rocksdb";

/// Calculate RocksDB specific memory allocation
fn calculate_rocksdb_memory() -> u64 {
	// Load the system memory
	let memory = Config::new();
	// Calculate total available cache memory in bytes
	let total_cache_bytes = memory.cache_gb * 1024 * 1024 * 1024;
	// Return configuration
	total_cache_bytes
}

pub(crate) struct RocksDBClientProvider {
	db: Arc<OptimisticTransactionDB>,
	sync: bool,
}

impl BenchmarkEngine<RocksDBClient> for RocksDBClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Calculate memory allocation
		let memory = calculate_rocksdb_memory();
		// Configure custom options
		let mut opts = Options::default();
		// Ensure we use fdatasync
		opts.set_use_fsync(false);
		// Set the maximum number of open files
		opts.set_max_open_files(1024);
		// Only use warning log level
		opts.set_log_level(LogLevel::Error);
		// Set the number of log files to keep
		opts.set_keep_log_file_num(20);
		// Create database if missing
		opts.create_if_missing(true);
		// Create column families if missing
		opts.create_missing_column_families(true);
		// Set the datastore compaction style
		opts.set_compaction_style(DBCompactionStyle::Level);
		// Increase the background thread count
		opts.increase_parallelism(num_cpus::get() as i32);
		// Increase the number of background jobs
		opts.set_max_background_jobs(num_cpus::get() as i32 * 2);
		// Set the maximum number of write buffers
		opts.set_max_write_buffer_number(32);
		// Set the amount of data to build up in memory
		opts.set_write_buffer_size(256 * 1024 * 1024);
		// Set the target file size for compaction
		opts.set_target_file_size_base(128 * 1024 * 1024);
		// Set the levelled target file size multipler
		opts.set_target_file_size_multiplier(10);
		// Set minimum number of write buffers to merge
		opts.set_min_write_buffer_number_to_merge(6);
		// Delay compaction until the minimum number of files
		opts.set_level_zero_file_num_compaction_trigger(16);
		// Set the compaction readahead size
		opts.set_compaction_readahead_size(16 * 1024 * 1024);
		// Set the max number of subcompactions
		opts.set_max_subcompactions(4);
		// Allow multiple writers to update memtables
		opts.set_allow_concurrent_memtable_write(true);
		// Improve concurrency from write batch mutex
		opts.set_enable_write_thread_adaptive_yield(true);
		// Avoid unnecessary blocking IO
		opts.set_avoid_unnecessary_blocking_io(true);
		// Use separate write thread queues
		opts.set_enable_pipelined_write(true);
		// Enable separation of keys and values
		opts.set_enable_blob_files(true);
		// Store 4KB values separate from keys
		opts.set_min_blob_size(4 * 1024);
		// Set the write-ahead-log size limit
		opts.set_wal_size_limit_mb(1024);
		// Set specific compression levels
		opts.set_compression_per_level(&[
			DBCompressionType::None,
			DBCompressionType::None,
			DBCompressionType::Snappy,
			DBCompressionType::Snappy,
			DBCompressionType::Snappy,
		]);
		// Create the in-memory LRU cache
		let cache = Cache::new_lru_cache(memory as usize);
		// Configure the block based file options
		let mut block_opts = BlockBasedOptions::default();
		block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
		block_opts.set_pin_top_level_index_and_filter(true);
		block_opts.set_bloom_filter(10.0, false);
		block_opts.set_block_size(64 * 1024);
		block_opts.set_block_cache(&cache);
		// Configure the database with the cache
		opts.set_block_based_table_factory(&block_opts);
		opts.set_blob_cache(&cache);
		opts.set_row_cache(&cache);
		// Allow memory-mapped reads
		opts.set_allow_mmap_reads(true);
		// Configure background WAL flush behaviour
		let db = match std::env::var("ROCKSDB_BACKGROUND_FLUSH").is_ok() {
			// Beckground flush is disabled which
			// means that the WAL will be flushed
			// whenever a transaction is committed.
			false => {
				// Enable manual WAL flush
				opts.set_manual_wal_flush(false);
				// Create the optimistic datastore
				Arc::new(OptimisticTransactionDB::open(&opts, DATABASE_DIR)?)
			}
			// Background flush is enabled so we
			// spawn a background worker thread to
			// flush the WAL to disk periodically.
			true => {
				// Enable manual WAL flush
				opts.set_manual_wal_flush(true);
				// Create the optimistic datastore
				let db = Arc::new(OptimisticTransactionDB::open(&opts, DATABASE_DIR)?);
				// Clone the database reference
				let dbc = db.clone();
				// Create a new background thread
				std::thread::spawn(move || {
					loop {
						// Wait for the specified interval
						std::thread::sleep(Duration::from_millis(200));
						// Flush the WAL to disk periodically
						if let Err(err) = dbc.flush_wal(true) {
							error!("Failed to flush WAL: {err}");
						}
					}
				});
				// Return the datastore
				db
			}
		};
		// Create the store
		Ok(Self {
			db,
			sync: options.sync,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<RocksDBClient> {
		Ok(RocksDBClient {
			db: self.db.clone(),
			sync: self.sync,
		})
	}
}

pub(crate) struct RocksDBClient {
	db: Arc<OptimisticTransactionDB>,
	sync: bool,
}

impl BenchmarkClient for RocksDBClient {
	async fn shutdown(&self) -> Result<()> {
		// No need to run background jobs
		self.db.cancel_all_background_work(true);
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Ok
		Ok(())
	}

	async fn compact(&self) -> Result<()> {
		// Create new flush options
		let mut opts = FlushOptions::default();
		opts.set_wait(true);
		// Flush the WAL to storage
		let _ = self.db.flush_wal(true);
		// Flush the memtables to SST
		let _ = self.db.flush_opt(&opts);
		// Create new wait options
		let mut opts = CompactOptions::default();
		opts.set_change_level(true);
		opts.set_target_level(4);
		opts.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
		// Compact the entire dataset
		self.db.compact_range_opt(Some(&[0u8]), Some(&[255u8]), &opts);
		// Create new wait options
		let mut opts = WaitForCompactOptions::default();
		opts.set_flush(true);
		// Wait for compaction to complete
		self.db.wait_for_compact(&opts)?;
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

impl RocksDBClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
		// Process the data
		txn.put(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
		// Configure read options
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&txn.snapshot());
		ro.set_verify_checksums(false);
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Process the data
		let res = txn.get_pinned_opt(key, &ro)?;
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
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
		// Process the data
		txn.put(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
		// Process the data
		txn.delete(key)?;
		txn.commit()?;
		Ok(())
	}

	async fn batch_create_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			txn.put(&key, val)?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_read_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
		// Configure read options
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&txn.snapshot());
		ro.set_verify_checksums(false);
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Process the data
		for key in keys {
			// Get the current value
			let res = txn.get_pinned_opt(&key, &ro)?;
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
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			txn.put(&key, val)?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_delete_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
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
		let p = scan.projection()?;
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		// Create a new transaction
		let txn = self.db.transaction_opt(&wo, &to);
		// Configure read options
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&txn.snapshot());
		ro.set_readahead_size(2 * 1024 * 1024);
		ro.set_iterate_lower_bound([0u8]);
		ro.set_iterate_upper_bound([255u8]);
		ro.set_verify_checksums(false);
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Create an iterator starting at the beginning
		let iter = txn.iterator_opt(IteratorMode::Start, ro);
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.unwrap().0);
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
					black_box(v.unwrap().1);
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
	}
}
