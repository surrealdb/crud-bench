#![cfg(feature = "mdbx")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use libmdbx::{
	Database, DatabaseOptions, Mode, NoWriteMap, PageSize, ReadWriteOptions, SyncMode, WriteFlags,
};
use serde_json::Value;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const DATABASE_DIR: &str = "mdbx";

pub(crate) struct MDBXClientProvider(Arc<Database<NoWriteMap>>);

impl BenchmarkEngine<MDBXClient> for MDBXClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Cleanup the data directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Configure database options
		let options = DatabaseOptions {
			// Configure the read-write options
			mode: Mode::ReadWrite(ReadWriteOptions {
				sync_mode: if options.sync {
					SyncMode::Durable
				} else {
					SyncMode::UtterlyNoSync
				},
				// No maximum database size
				max_size: None,
				// 64MB minimum database size
				min_size: Some(64 * 1024 * 1024),
				// Grow in 256MB steps
				growth_step: Some(256 * 1024 * 1024),
				// Disable shrinking in benchmarks
				shrink_threshold: None,
			}),
			// 16KB pages for better sequential performance
			page_size: Some(PageSize::Set(16384)),
			// Exclusive mode - no inter-process locking overhead
			exclusive: true,
			// LIFO garbage collection for better cache performance
			liforeclaim: true,
			// Disable readahead for better random access
			no_rdahead: true,
			// Skip memory initialization for performance
			no_meminit: true,
			// Coalesce transactions for better write performance
			coalesce: true,
			// Optimize for expected concurrent readers
			max_readers: Some(126),
			// We only use one table for benchmarks
			max_tables: Some(1),
			// Use defaults for transaction limits
			..Default::default()
		};
		// Create the database
		let db = Database::open_with_options(DATABASE_DIR, options)?;
		// Begin a new transaction
		let tx = db.begin_rw_txn()?;
		// Open the default table
		let tb = tx.open_table(None)?;
		// Prime the table for permaopen
		tx.prime_for_permaopen(tb);
		// Commit the transaction
		tx.commit()?;
		// Create the store
		Ok(Self(Arc::new(db)))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<MDBXClient> {
		Ok(MDBXClient {
			db: self.0.clone(),
		})
	}
}

pub(crate) struct MDBXClient {
	db: Arc<Database<NoWriteMap>>,
}

impl BenchmarkClient for MDBXClient {
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

impl MDBXClient {
	async fn create_bytes(&self, key: &[u8], val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serde::encode_to_vec(&val, bincode::config::standard())?;
		// Create a new transaction
		let txn = self.db.begin_rw_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Process the data
		txn.put(&table, key, &val, WriteFlags::empty())?;
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin_ro_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Process the data
		let res: Option<Vec<u8>> = txn.get(&table, key)?;
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
		let txn = self.db.begin_rw_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Process the data
		txn.put(&table, key, &val, WriteFlags::empty())?;
		txn.commit()?;
		Ok(())
	}

	async fn delete_bytes(&self, key: &[u8]) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin_rw_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Process the data
		txn.del(&table, key, None)?;
		txn.commit()?;
		Ok(())
	}

	async fn batch_create_bytes(
		&self,
		key_vals: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
	) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin_rw_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			txn.put(&table, &key, &val, WriteFlags::empty())?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_read_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin_ro_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Process the data
		for key in keys {
			// Get the current value
			let res: Option<Vec<u8>> = txn.get(&table, &key)?;
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
		// Create a new transaction
		let txn = self.db.begin_rw_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Process the data
		for result in key_vals {
			let (key, val) = result?;
			txn.put(&table, &key, &val, WriteFlags::empty())?;
		}
		// Commit the batch
		txn.commit()?;
		Ok(())
	}

	async fn batch_delete_bytes(&self, keys: impl Iterator<Item = Vec<u8>>) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin_rw_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Process the data
		for key in keys {
			txn.del(&table, &key, None)?;
		}
		// Commit the batch
		txn.commit()?;
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
		let txn = self.db.begin_ro_txn()?;
		// Open the default table
		let table = txn.open_table(None)?;
		// Create a cursor for iteration
		let iter = txn.cursor(&table)?.into_iter();
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
