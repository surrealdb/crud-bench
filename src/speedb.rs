#![cfg(feature = "speedb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, NOT_SUPPORTED_ERROR};
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
use serde_json::Value;
use speedb::{
	DBCompactionStyle, DBCompressionType, IteratorMode, LogLevel, OptimisticTransactionDB,
	OptimisticTransactionOptions, Options, ReadOptions, Transaction, WriteOptions,
};
use std::hint::black_box;
use std::sync::Arc;

pub(crate) struct SpeeDBClientProvider(Arc<OptimisticTransactionDB>);

impl BenchmarkEngine<SpeeDBClient> for SpeeDBClientProvider {
	async fn setup(_kt: KeyType, _columns: Columns) -> Result<Self> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("speedb");
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
			DBCompressionType::Lz4hc,
			DBCompressionType::Lz4hc,
			DBCompressionType::Lz4hc,
		]);
		// Create the store
		Ok(Self(Arc::new(OptimisticTransactionDB::open(&opts, "speedb")?)))
	}
	async fn create_client(&self, _: Option<String>) -> Result<SpeeDBClient> {
		Ok(SpeeDBClient(self.0.clone()))
	}
}

pub(crate) struct SpeeDBClient(Arc<OptimisticTransactionDB>);

impl BenchmarkClient for SpeeDBClient {
	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		let _ = std::fs::remove_dir_all("rocksdb");
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

impl SpeeDBClient {
	fn get_transaction(&self) -> Transaction<OptimisticTransactionDB> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(false);
		// Create a new transaction
		self.0.transaction_opt(&wo, &to)
	}

	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let txn = self.get_transaction();
		// Process the data
		txn.put(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Get the database snapshot
		let ss = self.0.snapshot();
		// Configure read options
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&ss);
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Create a new transaction
		let txn = self.get_transaction();
		// Process the data
		let res = txn.get_pinned_opt(key, &ro)?;
		assert!(res.is_some());
		Ok(())
	}

	async fn update_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let txn = self.get_transaction();
		// Process the data
		txn.put(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.get_transaction();
		// Process the data
		txn.delete(key)?;
		txn.commit()?;
		Ok(())
	}

	async fn scan_bytes(&self, scan: &Scan) -> Result<usize> {
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}

		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(0);
		let p = scan.projection()?;

		// Create a new transaction
		let txn = self.get_transaction();

		// Create an iterator starting at the beginning
		let iter = txn.iterator(IteratorMode::Start);

		match p {
			Projection::Full => {
				// Skip `offset` entries, then collect `limit` entries
				let results: Vec<_> = iter
					.skip(s) // Skip the first `offset` entries
					.take(l) // Take the next `limit` entries
					.collect();

				// Print the results
				let mut count = 0;
				for item in results {
					let key = item?;
					black_box(key);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				// Skip `offset` entries, then collect `limit` entries
				let count = iter
					.skip(s) // Skip the first `offset` entries
					.take(l) // Take the next `limit` entries
					.count();
				Ok(count)
			}
			Projection::Id => {
				bail!(NOT_SUPPORTED_ERROR)
			}
		}
	}
}
