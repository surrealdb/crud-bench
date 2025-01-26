#![cfg(feature = "redis")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{bail, Result};
use futures::StreamExt;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use serde_json::Value;
use std::hint::black_box;
use tokio::sync::Mutex;

pub const DEFAULT: &str = "redis://:root@127.0.0.1:6379/";

pub(crate) const REDIS_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "redis",
	pre_args: "-p 127.0.0.1:6379:6379",
	post_args: "redis-server --requirepass root",
};

pub(crate) struct RedisClientProvider {
	url: String,
}

impl BenchmarkEngine<RedisClient> for RedisClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		Ok(Self {
			url: options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned(),
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<RedisClient> {
		let client = Client::open(self.url.as_str())?;
		Ok(RedisClient {
			conn_iter: Mutex::new(client.get_multiplexed_async_connection().await?),
			conn_record: Mutex::new(client.get_multiplexed_async_connection().await?),
		})
	}
}

pub(crate) struct RedisClient {
	conn_iter: Mutex<MultiplexedConnection>,
	conn_record: Mutex<MultiplexedConnection>,
}

impl BenchmarkClient for RedisClient {
	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let val: Vec<u8> = self.conn_record.lock().await.get(key).await?;
		assert!(!val.is_empty());
		black_box(val);
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_string(&self, key: String) -> Result<()> {
		let val: Vec<u8> = self.conn_record.lock().await.get(key).await?;
		assert!(!val.is_empty());
		black_box(val);
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.conn_record.lock().await.del(key).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_string(&self, key: String) -> Result<()> {
		self.conn_record.lock().await.del(key).await?;
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan_bytes(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan_bytes(scan).await
	}
}

impl RedisClient {
	async fn scan_bytes(&self, scan: &Scan) -> Result<usize> {
		// Conditional scans are not supported
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(usize::MAX);
		let p = scan.projection()?;
		// Get the two connection types
		let mut conn_iter = self.conn_iter.lock().await;
		let mut conn_record = self.conn_record.lock().await;
		// Create an iterator starting at the beginning
		let mut iter = conn_iter.scan::<String>().await?.skip(s);
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for _ in 0..l {
					if let Some(k) = iter.next().await {
						black_box(k);
						count += 1;
					} else {
						break;
					}
				}
				Ok(count)
			}
			Projection::Full => {
				let mut count = 0;
				// Stream keys and fetch values concurrently
				while let Some(k) = iter.next().await {
					let v: Vec<u8> = conn_record.get(k).await?;
					black_box(v);
					count += 1;
					if count >= l {
						break;
					}
				}
				Ok(count)
			}
			Projection::Count => match scan.limit {
				None => Ok(iter.count().await),
				Some(l) => Ok(iter.take(l).count().await),
			},
		}
	}
}
