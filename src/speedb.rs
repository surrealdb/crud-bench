#![cfg(feature = "speedb")]

use anyhow::Error;
use anyhow::Result;
use speedb::{
	DBCompactionStyle, DBCompressionType, LogLevel, OptimisticTransactionDB,
	OptimisticTransactionOptions, Options, ReadOptions, WriteOptions,
};
use std::sync::Arc;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::KeyType;

pub(crate) struct SpeeDBClientProvider(Arc<OptimisticTransactionDB>);

impl BenchmarkEngine<SpeeDBClient> for SpeeDBClientProvider {
	async fn setup(_: KeyType) -> Result<Self, Error> {
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
		let _ = std::fs::remove_dir_all("speedb");
		// Ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(record)?;
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(false);
		// Create a new transaction
		let txn = self.0.transaction_opt(&wo, &to);
		// Process the data
		txn.put(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let key = &key.to_ne_bytes();
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(false);
		// Get the database snapshot
		let ss = self.0.snapshot();
		// Configure read options
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&ss);
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Create a new transaction
		let txn = self.0.transaction_opt(&wo, &to);
		// Process the data
		let read: Option<Vec<u8>> = txn.get_opt(key, &ro)?;
		assert!(read.is_some());
		Ok(())
	}

	async fn update_u32(&self, key: u32, record: &Record) -> Result<()> {
		let key = &key.to_ne_bytes();
		let val = bincode::serialize(record)?;
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(false);
		// Create a new transaction
		let txn = self.0.transaction_opt(&wo, &to);
		// Process the data
		txn.put(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let key = &key.to_ne_bytes();
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(false);
		// Create a new transaction
		let txn = self.0.transaction_opt(&wo, &to);
		// Process the data
		txn.delete(key)?;
		txn.commit()?;
		Ok(())
	}
}
