#![cfg(feature = "clouddb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use log::error;
use rocksdb::{
	AwsAccessType, BlockBasedOptions, BottommostLevelCompaction, Cache, CloudCredentials, CloudDB,
	CloudFileSystem, CloudFileSystemOptions, CompactOptions, DBCompactionStyle, DBCompressionType,
	FlushOptions, IteratorMode, LogLevel, Options, ReadOptions, WaitForCompactOptions, WriteBatch,
	WriteOptions,
};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const DATABASE_DIR: &str = "clouddb";

fn calculate_rocksdb_memory() -> u64 {
	let memory = Config::new();
	memory.cache_gb * 1024 * 1024 * 1024
}

pub(crate) struct CloudDBClientProvider {
	db: Arc<CloudDB>,
	#[allow(dead_code)] // kept alive for the DB's lifetime
	cloud_fs: CloudFileSystem,
	sync: bool,
}

impl BenchmarkEngine<CloudDBClient> for CloudDBClientProvider {
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}

	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		let memory = calculate_rocksdb_memory();

		// === Build CloudFileSystem in pure-local mode ===
		// `AwsAccessType::Anonymous` plus empty src/dest buckets means the
		// S3StorageProvider is constructed but never used for any I/O — all
		// operations go to the local filesystem at DATABASE_DIR.
		let mut creds = CloudCredentials::default();
		creds.set_type(AwsAccessType::Anonymous);

		let mut cloud_opts = CloudFileSystemOptions::default();
		cloud_opts.set_credentials(&creds);
		// Do NOT set src_bucket / dest_bucket — both stay empty so no S3 calls happen.
		cloud_opts.set_keep_local_sst_files(true);
		cloud_opts.set_keep_local_log_files(true);
		// Belt-and-braces: even with no bucket, skip any open-time cloud listing.
		cloud_opts.set_skip_cloud_listing_on_open(true);

		let cloud_fs = CloudFileSystem::new(&cloud_opts)?;
		let cloud_env = cloud_fs.create_cloud_env()?;

		// === RocksDB Options — identical to rocksdb_plain.rs for a fair comparison ===
		let mut opts = Options::default();
		opts.set_env(&cloud_env);
		opts.set_use_fsync(false);
		opts.set_max_open_files(1024);
		opts.set_log_level(LogLevel::Error);
		opts.set_keep_log_file_num(20);
		opts.create_if_missing(true);
		opts.create_missing_column_families(true);
		opts.set_compaction_style(DBCompactionStyle::Level);
		opts.increase_parallelism(num_cpus::get() as i32);
		opts.set_max_background_jobs(num_cpus::get() as i32 * 2);
		opts.set_max_write_buffer_number(32);
		opts.set_write_buffer_size(256 * 1024 * 1024);
		opts.set_target_file_size_base(128 * 1024 * 1024);
		opts.set_target_file_size_multiplier(10);
		opts.set_min_write_buffer_number_to_merge(6);
		opts.set_level_zero_file_num_compaction_trigger(8);
		opts.set_compaction_readahead_size(16 * 1024 * 1024);
		opts.set_max_subcompactions(4);
		opts.set_allow_concurrent_memtable_write(true);
		opts.set_enable_write_thread_adaptive_yield(true);
		opts.set_avoid_unnecessary_blocking_io(true);
		opts.set_enable_pipelined_write(true);
		opts.set_enable_blob_files(true);
		opts.set_min_blob_size(4 * 1024);
		opts.set_wal_size_limit_mb(1024);
		opts.set_compression_per_level(&[
			DBCompressionType::None,
			DBCompressionType::None,
			DBCompressionType::Snappy,
			DBCompressionType::Snappy,
			DBCompressionType::Snappy,
		]);
		let cache = Cache::new_lru_cache(memory as usize);
		let mut block_opts = BlockBasedOptions::default();
		block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
		block_opts.set_pin_top_level_index_and_filter(true);
		block_opts.set_bloom_filter(10.0, false);
		block_opts.set_block_size(64 * 1024);
		block_opts.set_block_cache(&cache);
		opts.set_block_based_table_factory(&block_opts);
		opts.set_blob_cache(&cache);
		opts.set_row_cache(&cache);
		opts.set_allow_mmap_reads(true);

		let db = match std::env::var("ROCKSDB_BACKGROUND_FLUSH").is_ok() {
			false => {
				opts.set_manual_wal_flush(false);
				Arc::new(CloudDB::open(&opts, &cloud_fs, DATABASE_DIR)?)
			}
			true => {
				opts.set_manual_wal_flush(true);
				let db = Arc::new(CloudDB::open(&opts, &cloud_fs, DATABASE_DIR)?);
				let dbc = db.clone();
				std::thread::spawn(move || {
					loop {
						std::thread::sleep(Duration::from_millis(200));
						if let Err(err) = dbc.flush_wal(true) {
							error!("Failed to flush WAL: {err}");
						}
					}
				});
				db
			}
		};

		Ok(Self {
			db,
			cloud_fs,
			sync: options.sync,
		})
	}

	async fn create_client(&self) -> Result<CloudDBClient> {
		Ok(CloudDBClient {
			db: self.db.clone(),
			sync: self.sync,
		})
	}
}

pub(crate) struct CloudDBClient {
	db: Arc<CloudDB>,
	sync: bool,
}

impl BenchmarkClient for CloudDBClient {
	type ReadRow = BenchValue;

	async fn shutdown(&self) -> Result<()> {
		self.db.cancel_all_background_work(true);
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		Ok(())
	}

	async fn compact(&self) -> Result<()> {
		let mut opts = FlushOptions::default();
		opts.set_wait(true);
		let _ = self.db.flush_wal(true);
		let _ = self.db.flush_opt(&opts);
		let mut opts = CompactOptions::default();
		opts.set_change_level(true);
		opts.set_target_level(4);
		opts.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
		self.db.compact_range_opt(Some(&[0u8]), Some(&[255u8]), &opts);
		let mut opts = WaitForCompactOptions::default();
		opts.set_flush(true);
		self.db.wait_for_compact(&opts)?;
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

impl CloudDBClient {
	fn write_opts(&self) -> WriteOptions {
		let mut wo = WriteOptions::default();
		wo.set_sync(self.sync);
		wo
	}

	async fn create_bytes(&self, key: &[u8], val: BenchValue) -> Result<()> {
		let val = val.encode()?;
		self.db.put_opt(key, val, &self.write_opts())?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<BenchValue> {
		let mut ro = ReadOptions::default();
		ro.set_verify_checksums(false);
		ro.set_async_io(true);
		ro.fill_cache(true);
		let res = self.db.get_pinned_opt(key, &ro)?;
		assert!(res.is_some());
		let val = BenchValue::decode(res.unwrap().as_ref())?;
		Ok(black_box(val))
	}

	async fn update_bytes(&self, key: &[u8], val: BenchValue) -> Result<()> {
		let val = val.encode()?;
		self.db.put_opt(key, val, &self.write_opts())?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		self.db.delete_opt(key, &self.write_opts())?;
		Ok(())
	}

	async fn batch_create_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		let mut batch = WriteBatch::default();
		for result in key_vals {
			let (key, val) = result?;
			batch.put(&key, val);
		}
		self.db.write_opt(batch, &self.write_opts())?;
		Ok(())
	}

	async fn batch_read_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		let mut ro = ReadOptions::default();
		ro.set_verify_checksums(false);
		ro.set_async_io(true);
		ro.fill_cache(true);
		for key in keys {
			let res = self.db.get_pinned_opt(&key, &ro)?;
			assert!(res.is_some());
			let val = BenchValue::decode(res.unwrap().as_ref())?;
			black_box(val);
		}
		Ok(())
	}

	async fn batch_update_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		let mut batch = WriteBatch::default();
		for result in key_vals {
			let (key, val) = result?;
			batch.put(&key, val);
		}
		self.db.write_opt(batch, &self.write_opts())?;
		Ok(())
	}

	async fn batch_delete_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		let mut batch = WriteBatch::default();
		for key in keys {
			batch.delete(&key);
		}
		self.db.write_opt(batch, &self.write_opts())?;
		Ok(())
	}

	async fn scan_bytes(&self, scan: &Scan) -> Result<usize> {
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(usize::MAX);
		let p = scan.projection()?;
		let mut ro = ReadOptions::default();
		ro.set_readahead_size(2 * 1024 * 1024);
		ro.set_iterate_lower_bound([0u8]);
		ro.set_iterate_upper_bound([255u8]);
		ro.set_verify_checksums(false);
		ro.set_async_io(true);
		ro.fill_cache(true);
		let iter = self.db.iterator_opt(IteratorMode::Start, ro);
		match p {
			Projection::Id => {
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.unwrap().0);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.unwrap().1);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => Ok(iter.skip(s).take(l).count()),
		}
	}
}
