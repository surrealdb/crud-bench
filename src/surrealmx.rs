#![cfg(feature = "surrealmx")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use serde_json::Value;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use surrealmx::Database;
use surrealmx::{AolMode, FsyncMode, SnapshotMode};
use surrealmx::{DatabaseOptions, PersistenceOptions};

const DATABASE_DIR: &str = "surrealmx";

type Key = Vec<u8>;
type Val = Vec<u8>;

pub(crate) struct SurrealMXClientProvider(Arc<Database<Key, Val>>);

impl BenchmarkEngine<SurrealMXClient> for SurrealMXClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Check if persistence is enabled
		if options.persisted {
			// Specify the database options
			let opts = DatabaseOptions::default();
			// Specify the persistence options
			let persistence = match options.sync {
				// Write to AOL immediatelyand fsync when sync is true
				true => PersistenceOptions::new(DATABASE_DIR)
					.with_snapshot_mode(SnapshotMode::Never)
					.with_aol_mode(AolMode::SynchronousOnCommit)
					.with_fsync_mode(FsyncMode::EveryAppend),
				// Write to AOL in the background and don't fsync when sync is false
				false => PersistenceOptions::new(DATABASE_DIR)
					.with_snapshot_mode(SnapshotMode::Never)
					.with_aol_mode(AolMode::AsynchronousAfterCommit)
					.with_fsync_mode(FsyncMode::Never),
			};
			// Create the store
			return Ok(Self(Arc::new(Database::new_with_persistence(opts, persistence).unwrap())));
		}
		// Create the store
		Ok(Self(Arc::new(Database::new())))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<SurrealMXClient> {
		Ok(SurrealMXClient {
			db: self.0.clone(),
		})
	}
}

pub(crate) struct SurrealMXClient {
	db: Arc<Database<Key, Val>>,
}

impl BenchmarkClient for SurrealMXClient {
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

impl SurrealMXClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
		// Create a new transaction
		let mut txn = self.db.transaction(true);
		// Process the data
		txn.set(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.transaction(false);
		// Process the data
		let res = txn.get(key.to_vec())?;
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
		let mut txn = self.db.transaction(true);
		// Process the data
		txn.set(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.transaction(true);
		// Process the data
		txn.del(key)?;
		txn.commit()?;
		Ok(())
	}

	async fn batch_create_bytes(&self, key_vals: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.transaction(true);
		// Process the data
		for (key, val) in key_vals {
			txn.set(key, val)?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_read_bytes(&self, keys: Vec<Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.transaction(false);
		// Process the data
		for key in keys {
			// Get the current value
			let res = txn.get(key)?;
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
		let mut txn = self.db.transaction(true);
		// Process the data
		for (key, val) in key_vals {
			txn.set(key, val)?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_delete_bytes(&self, keys: Vec<Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.transaction(true);
		// Process the data
		for key in keys {
			txn.del(key)?;
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
		let p = scan.projection()?;
		// Create a new transaction
		let mut txn = self.db.transaction(false);
		let beg = [0u8].to_vec();
		let end = [255u8].to_vec();
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// Scan the desired range of keys
				let iter = txn.keys(beg..end, scan.start, scan.limit)?;
				// Create an iterator starting at the beginning
				let iter = iter.into_iter();
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				// Scan the desired range of keys
				let iter = txn.scan(beg..end, scan.start, scan.limit)?;
				// Create an iterator starting at the beginning
				let iter = iter.into_iter();
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter {
					black_box(v.1);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => Ok(txn.total(beg..end, scan.start, scan.limit)?),
		}
	}
}
