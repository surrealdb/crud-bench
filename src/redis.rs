#![cfg(feature = "redis")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::value::BenchValue;
use crate::valueprovider::{ColumnType, Columns};
use crate::{
	Benchmark, Index, KeyType, Projection, Scan, VectorDistance, VectorIndexStrategy,
	VectorQuerySpec,
};
use anyhow::{Result, anyhow, bail};
use futures::StreamExt;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client, ScanOptions};
use std::hint::black_box;
use tokio::sync::Mutex;

pub const DEFAULT: &str = "redis://:root@127.0.0.1:6379/";

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	// Redis 6+ supports `io-threads` for network I/O parallelism (command
	// execution itself is still single-threaded). Docs recommend capping at
	// 8 and leaving room for the main thread. With `appendfsync always`
	// (i.e. --sync) the main thread is fsync-bound and the I/O threads sit
	// idle, so we drop back to one thread in that case.
	let io_threads = if options.sync {
		1
	} else {
		num_cpus::get().saturating_sub(1).clamp(2, 8)
	};
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
			 {persistence} {memory}"
		),
	}
}

pub(crate) struct RedisClientProvider {
	url: String,
	vector_field: Option<(String, usize)>,
}

impl BenchmarkEngine<RedisClient> for RedisClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, columns: Columns, options: &Benchmark) -> Result<Self> {
		let vector_field = columns.0.iter().find_map(|(n, t)| match t {
			ColumnType::FloatVector(dim) => Some((n.clone(), *dim)),
			_ => None,
		});
		Ok(Self {
			url: options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned(),
			vector_field,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<RedisClient> {
		let client = Client::open(self.url.as_str())?;
		Ok(RedisClient {
			conn_iter: Mutex::new(client.get_multiplexed_async_connection().await?),
			conn_record: Mutex::new(client.get_multiplexed_async_connection().await?),
			vector_field: self.vector_field.clone(),
		})
	}
}

pub(crate) struct RedisClient {
	conn_iter: Mutex<MultiplexedConnection>,
	conn_record: Mutex<MultiplexedConnection>,
	/// `(field_name, dim)` when the schema declares a vector column. When set,
	/// CRUD operations dual-write the vector bytes to a `vec:{key}` HASH so the
	/// Redis Stack `FT.CREATE` index can target a per-key indexed payload.
	vector_field: Option<(String, usize)>,
}

impl BenchmarkClient for RedisClient {
	// The return type when reading a row
	type ReadRow = BenchValue;

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.maybe_write_vector(&key.to_string(), &val).await?;
		let val = val.encode()?;
		let _: () = self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.maybe_write_vector(&key, &val).await?;
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
		self.maybe_write_vector(&key.to_string(), &val).await?;
		let val = val.encode()?;
		let _: () = self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.maybe_write_vector(&key, &val).await?;
		let val = val.encode()?;
		let _: () = self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.maybe_delete_vector(&key.to_string()).await?;
		let _: () = self.conn_record.lock().await.del(key).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_string(&self, key: String) -> Result<()> {
		self.maybe_delete_vector(&key).await?;
		let _: () = self.conn_record.lock().await.del(key).await?;
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		self.scan_bytes(scan).await
	}

	async fn scan_string(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		self.scan_bytes(scan).await
	}

	async fn build_vector_index(
		&self,
		_spec: &Index,
		vq: &VectorQuerySpec,
		dim: usize,
		name: &str,
	) -> Result<()> {
		let Some(metric) = redis_distance_metric(vq.distance) else {
			// Redis Stack lacks a native L1/Manhattan metric — refuse to
			// silently substitute another metric (which would produce
			// incorrect KNN results) and let the framework skip the run.
			bail!(NOT_SUPPORTED_ERROR);
		};
		// Build the parameter list for the `VECTOR <algo> <count> …`
		// portion of `FT.CREATE`. Each token must be a separate Redis
		// command argument — passing the whole string as one arg sends
		// it as a single token and the server rejects it with
		// "Bad: arguments for vector similarity number of parameters".
		let dim_s = dim.to_string();
		let (algo, vector_params): (&'static str, Vec<String>) = match vq.index_strategy {
			VectorIndexStrategy::Bruteforce => (
				"FLAT",
				vec![
					"6".into(),
					"TYPE".into(),
					"FLOAT32".into(),
					"DIM".into(),
					dim_s,
					"DISTANCE_METRIC".into(),
					metric.into(),
				],
			),
			VectorIndexStrategy::Hnsw {
				m,
				ef_construction,
				ef_search,
				..
			} => (
				"HNSW",
				vec![
					"12".into(),
					"TYPE".into(),
					"FLOAT32".into(),
					"DIM".into(),
					dim_s,
					"DISTANCE_METRIC".into(),
					metric.into(),
					"M".into(),
					m.to_string(),
					"EF_CONSTRUCTION".into(),
					ef_construction.to_string(),
					"EF_RUNTIME".into(),
					ef_search.to_string(),
				],
			),
			VectorIndexStrategy::DiskAnn {
				..
			} => bail!(NOT_SUPPORTED_ERROR),
		};
		// Drop any leftover index with the same name and (re)create.
		let mut conn = self.conn_record.lock().await;
		let _: () =
			redis::cmd("FT.DROPINDEX").arg(name).query_async(&mut *conn).await.unwrap_or(());
		let mut create = redis::cmd("FT.CREATE");
		create
			.arg(name)
			.arg("ON")
			.arg("HASH")
			.arg("PREFIX")
			.arg(1)
			.arg("vec:")
			.arg("SCHEMA")
			.arg("v")
			.arg("VECTOR")
			.arg(algo);
		for p in &vector_params {
			create.arg(p.as_str());
		}
		let _: () = create.query_async(&mut *conn).await?;
		Ok(())
	}

	async fn scan_vector_u32(
		&self,
		scan: &Scan,
		query: &[f32],
		_ctx: ScanContext,
	) -> Result<usize> {
		self.knn_scan(scan, query).await
	}

	async fn scan_vector_string(
		&self,
		scan: &Scan,
		query: &[f32],
		_ctx: ScanContext,
	) -> Result<usize> {
		self.knn_scan(scan, query).await
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
		// Build the DEL pipeline. When a vector column is declared, pair each
		// primary `DEL k` with a `DEL vec:{k}` so the `vec:{key}` HASH mirror
		// (written by `maybe_write_vector`) is cleaned up in the same
		// round-trip — otherwise the delete phase leaves stale embeddings
		// behind in the `vec:` namespace.
		let has_vec = self.vector_field.is_some();
		let mut conn = self.conn_record.lock().await;
		let mut pipe = redis::pipe();
		for k in keys {
			if has_vec {
				pipe.cmd("DEL").arg(format!("vec:{k}")).ignore();
			}
			pipe.cmd("DEL").arg(k).ignore();
		}
		// Execute the pipeline
		pipe.exec_async(&mut *conn).await?;
		Ok(())
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		// See `batch_delete_u32` — same dual-DEL pattern.
		let has_vec = self.vector_field.is_some();
		let mut conn = self.conn_record.lock().await;
		let mut pipe = redis::pipe();
		for k in keys {
			if has_vec {
				pipe.cmd("DEL").arg(format!("vec:{k}")).ignore();
			}
			pipe.cmd("DEL").arg(k).ignore();
		}
		// Execute the pipeline
		pipe.exec_async(&mut *conn).await?;
		Ok(())
	}
}

/// Map the benchmark's distance enum to Redis Stack's distance metric keyword.
///
/// Returns `None` for metrics Redis Stack does not implement natively — the
/// caller is responsible for surfacing this as `NOT_SUPPORTED_ERROR` rather
/// than silently substituting another metric (which would corrupt the
/// reported KNN results).
fn redis_distance_metric(d: VectorDistance) -> Option<&'static str> {
	match d {
		VectorDistance::Cosine => Some("COSINE"),
		VectorDistance::Euclidean => Some("L2"),
		VectorDistance::InnerProduct => Some("IP"),
		// Redis Stack has no native L1 / Manhattan.
		VectorDistance::Manhattan => None,
	}
}

impl RedisClient {
	/// Dual-write the embedding bytes to `vec:{key}` HASH so the FT vector
	/// index can target a dedicated key prefix. No-op when the schema does
	/// not declare a vector column.
	async fn maybe_write_vector(&self, key: &str, val: &BenchValue) -> Result<()> {
		let Some((field, dim)) = self.vector_field.as_ref() else {
			return Ok(());
		};
		let inner =
			val.get_field(field).ok_or_else(|| anyhow!("redis: missing vector field `{field}`"))?;
		let v = inner
			.as_float_vector()
			.ok_or_else(|| anyhow!("redis: field `{field}` is not a FloatVector"))?;
		if v.len() != *dim {
			bail!("redis: vector dim mismatch ({}, expected {dim})", v.len());
		}
		let bytes: &[u8] = bytemuck::cast_slice(v);
		let hkey = format!("vec:{key}");
		let mut conn = self.conn_record.lock().await;
		let _: () =
			redis::cmd("HSET").arg(hkey).arg("v").arg(bytes).query_async(&mut *conn).await?;
		Ok(())
	}

	/// Mirror of `maybe_write_vector` for the delete path: drops the
	/// `vec:{key}` HASH so the embedding doesn't outlive the primary record.
	/// No-op when the schema does not declare a vector column.
	async fn maybe_delete_vector(&self, key: &str) -> Result<()> {
		if self.vector_field.is_none() {
			return Ok(());
		}
		let hkey = format!("vec:{key}");
		let mut conn = self.conn_record.lock().await;
		let _: () = redis::cmd("DEL").arg(hkey).query_async(&mut *conn).await?;
		Ok(())
	}

	async fn knn_scan(&self, scan: &Scan, query: &[f32]) -> Result<usize> {
		let vq = scan
			.vector_query
			.as_ref()
			.ok_or_else(|| anyhow!("knn_scan: scan `{}` missing vector_query", scan.name))?;
		let k = vq.top_k;
		let bytes: &[u8] = bytemuck::cast_slice(query);
		let mut conn = self.conn_record.lock().await;
		let res: redis::Value = redis::cmd("FT.SEARCH")
			.arg(&scan.id)
			.arg(format!("*=>[KNN {k} @v $q AS score]"))
			.arg("PARAMS")
			.arg(2)
			.arg("q")
			.arg(bytes)
			.arg("DIALECT")
			.arg(2)
			.arg("LIMIT")
			.arg(0)
			.arg(k)
			.arg("RETURN")
			.arg(0)
			.query_async(&mut *conn)
			.await?;
		// FT.SEARCH returns `[total, key1, key2, ...]` (with RETURN 0). Use the
		// reported `total` capped at `k` for the row-count return.
		if let redis::Value::Array(items) = &res
			&& let Some(redis::Value::Int(total)) = items.first()
		{
			return Ok((*total as usize).min(k));
		}
		Ok(k)
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
		// Get the two connection types
		let mut conn_iter = self.conn_iter.lock().await;
		let mut conn_record = self.conn_record.lock().await;
		// Configure the scan options for improved iteration.
		// `MATCH [^v]*` excludes the parallel `vec:{key}` HASH mirror that
		// the vector benchmark dual-writes (see `maybe_write_vector`). Data
		// keys generated by `keyprovider` only ever start with digits or
		// hex chars (`0`-`9`, `a`-`f`), so excluding `v` is unambiguous.
		// Note: Redis glob uses `[^set]` for negation (server-side
		// `stringmatchlen`), not the shell-style `[!set]`.
		let opts = ScanOptions::default().with_count(5000).with_pattern("[^v]*");
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
