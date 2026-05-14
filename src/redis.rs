#![cfg(feature = "redis")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, anyhow, bail};
use futures::StreamExt;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client, ScanOptions};
use std::hint::black_box;
use tokio::sync::Mutex;

pub const DEFAULT: &str = "redis://:root@127.0.0.1:6379/";

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	// Redis 6+ supports `io-threads` for network I/O parallelism. The Redis
	// docs recommend not exceeding 8 and leaving room for the main thread.
	let io_threads = num_cpus::get().saturating_sub(1).clamp(2, 8);
	// Persistence: AOF on/off + sync flush. When persisted=true we also
	// disable RDB explicitly so the default snapshot schedule doesn't
	// contend with AOF writes during the benchmark.
	let persistence = match (options.persisted, options.sync) {
		(false, _) => "--appendonly no --save ''".to_string(),
		(true, false) => "--appendonly yes --appendfsync everysec --save ''".to_string(),
		(true, true) => "--appendonly yes --appendfsync always --save ''".to_string(),
	};
	// Memory cap (optimised only) — without one the container OOM-kills
	// rather than evicting; this keeps comparisons deterministic.
	let memory = match options.optimised {
		true => {
			let cache_gb = crate::memory::Config::new().cache_gb.max(1);
			format!("--maxmemory {cache_gb}gb --maxmemory-policy noeviction")
		}
		false => String::new(),
	};
	DockerParams {
		image: "redis",
		pre_args: "-p 127.0.0.1:6379:6379".to_string(),
		post_args: format!(
			"redis-server --requirepass root --io-threads {io_threads} \
			 --io-threads-do-reads yes {persistence} {memory}"
		),
	}
}

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
	// The return type when reading a row
	type ReadRow = BenchValue;

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		let val = val.encode()?;
		let _: () = self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		let val = val.encode()?;
		let _: () = self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		let val: Vec<u8> = self.conn_record.lock().await.get(key).await?;
		assert!(!val.is_empty());
		let val = BenchValue::decode(&val)?;
		Ok(black_box(val))
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_string(&self, key: String) -> Result<BenchValue> {
		let val: Vec<u8> = self.conn_record.lock().await.get(key).await?;
		assert!(!val.is_empty());
		let val = BenchValue::decode(&val)?;
		Ok(black_box(val))
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		let val = val.encode()?;
		let _: () = self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		let val = val.encode()?;
		let _: () = self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		let _: () = self.conn_record.lock().await.del(key).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_string(&self, key: String) -> Result<()> {
		let _: () = self.conn_record.lock().await.del(key).await?;
		Ok(())
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
		// Build the SET pipeline
		let mut conn = self.conn_record.lock().await;
		let mut pipe = redis::pipe();
		for (k, v) in key_vals {
			pipe.cmd("SET").arg(k).arg(v.encode()?).ignore();
		}
		// Execute the pipeline
		pipe.exec_async(&mut *conn).await?;
		Ok(())
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		// Build the SET pipeline
		let mut conn = self.conn_record.lock().await;
		let mut pipe = redis::pipe();
		for (k, v) in key_vals {
			pipe.cmd("SET").arg(k).arg(v.encode()?).ignore();
		}
		// Execute the pipeline
		pipe.exec_async(&mut *conn).await?;
		Ok(())
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		// Build the GET pipeline
		let mut conn = self.conn_record.lock().await;
		let mut pipe = redis::pipe();
		let mut count = 0usize;
		for k in keys {
			pipe.cmd("GET").arg(k);
			count += 1;
		}
		// Execute the pipeline and inspect the responses
		let vals: Vec<Option<Vec<u8>>> = pipe.query_async(&mut *conn).await?;
		assert_eq!(vals.len(), count);
		for v in vals {
			let v = v.ok_or_else(|| anyhow!("missing key"))?;
			assert!(!v.is_empty());
			black_box(v);
		}
		Ok(())
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		// Build the GET pipeline
		let mut conn = self.conn_record.lock().await;
		let mut pipe = redis::pipe();
		let mut count = 0usize;
		for k in keys {
			pipe.cmd("GET").arg(k);
			count += 1;
		}
		// Execute the pipeline and inspect the responses
		let vals: Vec<Option<Vec<u8>>> = pipe.query_async(&mut *conn).await?;
		assert_eq!(vals.len(), count);
		for v in vals {
			let v = v.ok_or_else(|| anyhow!("missing key"))?;
			assert!(!v.is_empty());
			black_box(v);
		}
		Ok(())
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		// SET overwrites in Redis, so update has identical wire shape to create.
		self.batch_create_u32(key_vals).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		// SET overwrites in Redis, so update has identical wire shape to create.
		self.batch_create_string(key_vals).await
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		// Build the DEL pipeline
		let mut conn = self.conn_record.lock().await;
		let mut pipe = redis::pipe();
		for k in keys {
			pipe.cmd("DEL").arg(k).ignore();
		}
		// Execute the pipeline
		pipe.exec_async(&mut *conn).await?;
		Ok(())
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		// Build the DEL pipeline
		let mut conn = self.conn_record.lock().await;
		let mut pipe = redis::pipe();
		for k in keys {
			pipe.cmd("DEL").arg(k).ignore();
		}
		// Execute the pipeline
		pipe.exec_async(&mut *conn).await?;
		Ok(())
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
		// Configure the scan options for improve iteration
		let opts = ScanOptions::default().with_count(5000);
		// Create an iterator starting at the beginning
		let mut iter = conn_iter.scan_options::<String>(opts).await?.skip(s);
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
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
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
				// Full count queries are too slow
				None => bail!(NOT_SUPPORTED_ERROR),
				Some(l) => Ok(iter.take(l).count().await),
			},
		}
	}
}
