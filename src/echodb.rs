#![cfg(feature = "echodb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use echodb::{Database, new};
use serde_json::Value;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

type Key = Vec<u8>;
type Val = Vec<u8>;

pub(crate) struct EchoDBClientProvider(Arc<Database<Key, Val>>);

impl BenchmarkEngine<EchoDBClient> for EchoDBClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(_: KeyType, _columns: Columns, _options: &Benchmark) -> Result<Self> {
		// Create the store
		Ok(Self(Arc::new(new())))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<EchoDBClient> {
		Ok(EchoDBClient {
			db: self.0.clone(),
		})
	}
}

pub(crate) struct EchoDBClient {
	db: Arc<Database<Key, Val>>,
}

impl BenchmarkClient for EchoDBClient {
	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create_bytes(key.to_ne_bytes().to_vec(), val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create_bytes(key.into_bytes(), val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read_bytes(key.to_ne_bytes().to_vec()).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read_bytes(key.into_bytes()).await
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		self.update_bytes(key.to_ne_bytes().to_vec(), val).await
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		self.update_bytes(key.into_bytes(), val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete_bytes(key.to_ne_bytes().to_vec()).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete_bytes(key.into_bytes()).await
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan_bytes(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan_bytes(scan).await
	}
}

impl EchoDBClient {
	async fn create_bytes(&self, key: Vec<u8>, val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.db.begin(true).await;
		// Process the data
		txn.set(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn read_bytes(&self, key: Vec<u8>) -> Result<()> {
		// Create a new transaction
		let txn = self.db.begin(false).await;
		// Process the data
		let res = txn.get(key)?;
		// Check the value exists
		assert!(res.is_some());
		// Deserialise the value
		black_box(res.unwrap());
		// All ok
		Ok(())
	}

	async fn update_bytes(&self, key: Vec<u8>, val: Value) -> Result<()> {
		// Serialise the value
		let val = bincode::serialize(&val)?;
		// Create a new transaction
		let mut txn = self.db.begin(true).await;
		// Process the data
		txn.set(key, val)?;
		txn.commit()?;
		Ok(())
	}

	async fn delete_bytes(&self, key: Vec<u8>) -> Result<()> {
		// Create a new transaction
		let mut txn = self.db.begin(true).await;
		// Process the data
		txn.del(key)?;
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
		let txn = self.db.begin(false).await;
		let beg = [0u8].to_vec();
		let end = [255u8].to_vec();
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// Scan the desired range of keys
				let iter = txn.keys(beg..end, s + l)?;
				// Create an iterator starting at the beginning
				let iter = iter.into_iter();
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				// Scan the desired range of keys
				let iter = txn.scan(beg..end, s + l)?;
				// Create an iterator starting at the beginning
				let iter = iter.into_iter();
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in iter.skip(s).take(l) {
					black_box(v.1);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				Ok(txn
					.keys(beg..end, s + l)?
					.iter()
					.skip(s) // Skip the first `offset` entries
					.take(l) // Take the next `limit` entries
					.count())
			}
		}
	}
}
