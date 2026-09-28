#![cfg(feature = "lmdb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use heed::types::Bytes;
use heed::{Database, EnvOpenOptions};
use heed::{Env, EnvFlags, WithoutTls};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

const DATABASE_DIR: &str = "lmdb";

const DEFAULT_SIZE: usize = 4_294_967_296; // 4GiB

static DATABASE_SIZE: LazyLock<usize> = LazyLock::new(|| {
	std::env::var("CRUD_BENCH_LMDB_DATABASE_SIZE")
		.map(|s| s.parse::<usize>().unwrap_or(DEFAULT_SIZE))
		.unwrap_or(DEFAULT_SIZE)
});

pub(crate) struct LmDBClientProvider {
	db: Arc<(Env<WithoutTls>, Database<Bytes, Bytes>)>,
	dir: PathBuf,
}

impl BenchmarkEngine<LmDBClient> for LmDBClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Determine directory and flags based on options
		let (dir, in_memory) = match options.endpoint.as_deref() {
			Some("memory") => {
				let path = if std::path::Path::new("/dev/shm").is_dir() {
					PathBuf::from("/dev/shm/crud-bench-lmdb")
				} else {
					std::env::temp_dir().join("crud-bench-lmdb")
				};
				(path, true)
			}
			Some("tmpfs") => {
				let path = if std::path::Path::new("/dev/shm").is_dir() {
					PathBuf::from("/dev/shm/crud-bench-lmdb")
				} else {
					std::env::temp_dir().join("crud-bench-lmdb")
				};
				(path, false)
			}
			Some(p) => (PathBuf::from(p), false),
			None => (PathBuf::from(DATABASE_DIR), false),
		};

		// Cleanup the data directory
		std::fs::remove_dir_all(&dir).ok();
		// Recreate the database directory
		std::fs::create_dir_all(&dir)?;

		// Configure the environment flags. Each flag below is independent, so
		// whether it applies is decided on its own rather than nested inside
		// the in-memory/disk branch above.
		let mut flags =
			// Skip the OS readahead heuristic: benchmark access is point/range
			// lookups driven by the workload, not a sequential scan the OS
			// could usefully prefetch ahead of.
			EnvFlags::NO_READ_AHEAD
			// Skip zeroing newly-mapped pages: every page is fully overwritten
			// before it is ever read.
			| EnvFlags::NO_MEM_INIT;
		// Skip fsync/fdatasync on every commit. Pure in-memory mode never
		// persists at all regardless of --sync, and otherwise this only
		// applies when --sync was not requested.
		if in_memory || !options.sync {
			flags |= EnvFlags::NO_SYNC | EnvFlags::NO_META_SYNC;
		}
		// Write through the memory map directly instead of write() syscalls.
		// Faster, and still durable via msync at commit as long as MAP_ASYNC
		// (below) is not also set. Used for in-memory mode and whenever
		// --optimised was requested.
		if in_memory || options.optimised {
			flags |= EnvFlags::WRITE_MAP;
		}
		// Defer flushes to the OS page-cache writeback instead of
		// synchronising at commit. This is what makes in-memory mode
		// non-durable, and it must never be combined with --sync: MAP_ASYNC
		// silently defeats sync durability even when NO_SYNC/NO_META_SYNC are
		// not set, so --optimised only enables it when --sync is off.
		if in_memory || (options.optimised && !options.sync) {
			flags |= EnvFlags::MAP_ASYNC;
		}

		// Calculate database map size (scale with samples if larger than default 4GiB)
		let map_size = (*DATABASE_SIZE).max((options.samples as usize).saturating_mul(2048));

		// Create a new environment
		let env = unsafe {
			EnvOpenOptions::new()
				// Allow more transactions than threads
				.read_txn_without_tls()
				// Configure database flags
				.flags(flags)
				// We only use one db for benchmarks
				.max_dbs(1)
				// Optimize for expected concurrent readers
				.max_readers(126)
				// Set the database size
				.map_size(map_size)
				// Open the database
				.open(&dir)
		}?;
		// Create the database
		let db = {
			// Open a new transaction
			let mut txn = env.write_txn()?;
			// Initiate the database
			env.create_database::<Bytes, Bytes>(&mut txn, None)?
		};
		// Create the store
		Ok(Self {
			db: Arc::new((env, db)),
			dir,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<LmDBClient> {
		Ok(LmDBClient {
			db: self.db.clone(),
			dir: self.dir.clone(),
		})
	}
}

pub(crate) struct LmDBClient {
	db: Arc<(Env<WithoutTls>, Database<Bytes, Bytes>)>,
	dir: PathBuf,
}

impl BenchmarkClient for LmDBClient {
	// The return type when reading a row
	type ReadRow = BenchValue;

	async fn shutdown(&self) -> Result<()> {
		// Cleanup the data directory
		std::fs::remove_dir_all(&self.dir).ok();
		// Ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.create_bytes(&key.to_be_bytes(), val).await
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.create_bytes(&key.into_bytes(), val).await
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		self.read_bytes(&key.to_be_bytes()).await
	}

	async fn read_string(&self, key: String) -> Result<BenchValue> {
		self.read_bytes(&key.into_bytes()).await
	}

	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.update_bytes(&key.to_be_bytes(), val).await
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.update_bytes(&key.into_bytes(), val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete_bytes(&key.to_be_bytes()).await
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
			Ok((key.to_be_bytes().to_vec(), val))
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
		let keys_iter = keys.map(|key| key.to_be_bytes().to_vec());
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
			Ok((key.to_be_bytes().to_vec(), val))
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
		let keys_iter = keys.map(|key| key.to_be_bytes().to_vec());
		self.batch_delete_bytes(keys_iter).await
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		let keys_iter = keys.map(|key| key.into_bytes());
		self.batch_delete_bytes(keys_iter).await
	}
}

impl LmDBClient {
	async fn create_bytes(&self, key: &[u8], val: BenchValue) -> Result<()> {
		// Serialise the value
		let val = val.encode()?;
		// Create a new transaction
		let mut txn = self.db.0.write_txn()?;
		// Process the data
		self.db.1.put(&mut txn, key, val.as_ref())?;
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<BenchValue> {
		// Create a new transaction
		let txn = self.db.0.read_txn()?;
		// Process the data
		let res: Option<_> = self.db.1.get(&txn, key)?;
		// Check the value exists
		assert!(res.is_some());
		// Deserialise the value
		let val = BenchValue::decode(res.unwrap())?;
		// All ok
		Ok(black_box(val))
	}

	async fn update_bytes(&self, key: &[u8], val: BenchValue) -> Result<()> {
		// Serialise the value
		let val = val.encode()?;
		// Create a new transaction
		let mut txn = self.db.0.write_txn()?;
		// Process the data
		self.db.1.put(&mut txn, key, &val)?;
		txn.commit()?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.0.write_txn()?;
		// Process the data
		self.db.1.delete(&mut txn, key)?;
		txn.commit()?;
		Ok(())
	}

	async fn batch_create_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.0.write_txn()?;
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			self.db.1.put(&mut txn, &key, &val)?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_read_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let txn = self.db.0.read_txn()?;
		// Process the data
		for key in keys {
			// Get the current value
			let res: Option<_> = self.db.1.get(&txn, &key)?;
			// Check the value exists
			assert!(res.is_some());
			// Deserialise the value
			let val = BenchValue::decode(res.unwrap())?;
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
		let mut txn = self.db.0.write_txn()?;
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			self.db.1.put(&mut txn, &key, &val)?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_delete_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.0.write_txn()?;
		// Process the data
		for key in keys {
			self.db.1.delete(&mut txn, &key)?;
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
		// Create a new transaction
		let txn = self.db.0.read_txn()?;
		// Create an iterator starting at the beginning
		let iter = self.db.1.iter(&txn)?;
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
