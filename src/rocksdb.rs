#![cfg(feature = "rocksdb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
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

pub(crate) struct RocksDBClientProvider(Arc<OptimisticTransactionDB>);

impl BenchmarkEngine<RocksDBClient> for RocksDBClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, _endpoint: Option<&str>) -> Result<Self> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Configure custom options
		let mut opts = Options::default();
		// Ensure we use fdatasync
		opts.set_use_fsync(false);
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
		opts.increase_parallelism(8);
		// Set the maximum number of write buffers
		opts.set_max_write_buffer_number(32);
		// Set the amount of data to build up in memory
		opts.set_write_buffer_size(256 * 1024 * 1024);
		// Set the target file size for compaction
		opts.set_target_file_size_base(512 * 1024 * 1024);
		// Set minimum number of write buffers to merge
		opts.set_min_write_buffer_number_to_merge(4);
		// Allow multiple writers to update memtables
		opts.set_allow_concurrent_memtable_write(true);
		// Avoid unnecessary blocking IO
		opts.set_avoid_unnecessary_blocking_io(true);
		// Use separate write thread queues
		opts.set_enable_pipelined_write(true);
		// Enable separation of keys and values
		opts.set_enable_blob_files(true);
		// Store 4KB values separate from keys
		opts.set_min_blob_size(4 * 1024);
		// Set specific compression levels
		opts.set_compression_per_level(&[
			DBCompressionType::None,
			DBCompressionType::None,
			DBCompressionType::Snappy,
			DBCompressionType::Snappy,
			DBCompressionType::Snappy,
		]);
		// Set the block cache size in bytes
		let mut block_opts = BlockBasedOptions::default();
		let cache = Cache::new_lru_cache(512 * 1024 * 1024);
		block_opts.set_hybrid_ribbon_filter(10.0, 2);
		block_opts.set_block_cache(&cache);
		opts.set_block_based_table_factory(&block_opts);
		opts.set_blob_cache(&cache);
		opts.set_row_cache(&cache);
		// Create the store
		Ok(Self(Arc::new(OptimisticTransactionDB::open(&opts, DATABASE_DIR)?)))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<RocksDBClient> {
		Ok(RocksDBClient {
			db: self.0.clone(),
		})
	}
}

pub(crate) struct RocksDBClient {
	db: Arc<OptimisticTransactionDB>,
}

impl BenchmarkClient for RocksDBClient {
	async fn shutdown(&self) -> Result<()> {
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

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan_bytes(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan_bytes(scan).await
	}
}

impl RocksDBClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Clone the datastore
		let db = self.db.clone();
		let key: Box<[u8]> = key.into();
		// Execute on the blocking threadpool
		affinitypool::execute(move || -> Result<_> {
			// Serialise the value
			let val = bincode::serialize(&val)?;
			// Set the transaction options
			let mut to = OptimisticTransactionOptions::default();
			to.set_snapshot(true);
			// Set the write options
			let mut wo = WriteOptions::default();
			wo.set_sync(false);
			// Create a new transaction
			let txn = db.transaction_opt(&wo, &to);
			// Process the data
			txn.put(&key, val)?;
			txn.commit()?;
			Ok(())
		})
		.await
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Clone the datastore
		let db = self.db.clone();
		let key: Box<[u8]> = key.into();
		// Execute on the blocking threadpool
		affinitypool::execute(move || -> Result<_> {
			// Set the transaction options
			let mut to = OptimisticTransactionOptions::default();
			to.set_snapshot(true);
			// Set the write options
			let mut wo = WriteOptions::default();
			wo.set_sync(false);
			// Create a new transaction
			let txn = db.transaction_opt(&wo, &to);
			// Configure read options
			let mut ro = ReadOptions::default();
			ro.set_snapshot(&txn.snapshot());
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
		})
		.await
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Clone the datastore
		let db = self.db.clone();
		let key: Box<[u8]> = key.into();
		// Execute on the blocking threadpool
		affinitypool::execute(move || -> Result<_> {
			// Serialise the value
			let val = bincode::serialize(&val)?;
			// Set the transaction options
			let mut to = OptimisticTransactionOptions::default();
			to.set_snapshot(true);
			// Set the write options
			let mut wo = WriteOptions::default();
			wo.set_sync(false);
			// Create a new transaction
			let txn = db.transaction_opt(&wo, &to);
			// Process the data
			txn.put(&key, val)?;
			txn.commit()?;
			Ok(())
		})
		.await
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Clone the datastore
		let db = self.db.clone();
		let key: Box<[u8]> = key.into();
		// Execute on the blocking threadpool
		affinitypool::execute(move || -> Result<_> {
			// Set the transaction options
			let mut to = OptimisticTransactionOptions::default();
			to.set_snapshot(true);
			// Set the write options
			let mut wo = WriteOptions::default();
			wo.set_sync(false);
			// Create a new transaction
			let txn = db.transaction_opt(&wo, &to);
			// Process the data
			txn.delete(&key)?;
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
		affinitypool::execute(move || -> Result<_> {
			// Set the transaction options
			let mut to = OptimisticTransactionOptions::default();
			to.set_snapshot(true);
			// Set the write options
			let mut wo = WriteOptions::default();
			wo.set_sync(false);
			// Create a new transaction
			let txn = db.transaction_opt(&wo, &to);
			// Configure read options
			let mut ro = ReadOptions::default();
			ro.set_snapshot(&txn.snapshot());
			ro.set_iterate_lower_bound([0u8]);
			ro.set_iterate_upper_bound([255u8]);
			ro.set_async_io(true);
			ro.fill_cache(true);
			// Create an iterator starting at the beginning
			let iter = txn.iterator_opt(IteratorMode::Start, ro);
			// Perform the relevant projection scan type
			match p {
				Projection::Id => {
					// Skip `offset` entries, then collect `limit` entries
					#[allow(clippy::unnecessary_filter_map)]
					Ok(iter
						.skip(s) // Skip the first `offset` entries
						.take(l) // Take the next `limit` entries
						.filter_map(|v| Some(black_box(v.unwrap().0)))
						.count())
				}
				Projection::Full => {
					// Skip `offset` entries, then collect `limit` entries
					#[allow(clippy::unnecessary_filter_map)]
					Ok(iter
						.skip(s) // Skip the first `offset` entries
						.take(l) // Take the next `limit` entries
						.filter_map(|v| Some(black_box(v.unwrap().1)))
						.count())
				}
				Projection::Count => {
					// Skip `offset` entries, then collect `limit` entries
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
