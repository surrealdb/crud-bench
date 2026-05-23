#![cfg(feature = "surrealdb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::SurrealDBDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config as MemoryConfig;
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{
	Benchmark, Index, KeyType, Projection, Scan, VectorDistance, VectorIndexStrategy,
	VectorQuerySpec,
};
use anyhow::{Result, bail};
use log::{error, warn};
use std::env;
use std::hint::black_box;
use std::time::Duration;
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
use surrealdb::opt::{Config, Resource};
use surrealdb::types::{
	Array, Bytes as SurrealBytes, Datetime, Number, Object, RecordId, RecordIdKey, SurrealValue,
	ToSql, Uuid as SurrealUuid, Value,
};
use tokio::time::{sleep, timeout};

/// Convert a [`BenchValue`] to a native [`surrealdb::types::Value`]. UUID,
/// datetime, decimal, and bytes go through directly without a JSON detour.
fn bench_to_surreal_value(v: BenchValue) -> Value {
	match v {
		BenchValue::Null => Value::Null,
		BenchValue::Bool(b) => Value::Bool(b),
		BenchValue::Int(i) => Value::Number(Number::Int(i)),
		BenchValue::UInt(u) => match i64::try_from(u) {
			Ok(i) => Value::Number(Number::Int(i)),
			Err(_) => Value::Number(Number::Float(u as f64)),
		},
		BenchValue::Float(f) => Value::Number(Number::Float(f)),
		BenchValue::Decimal(d) => Value::Number(Number::Decimal(d)),
		BenchValue::String(s) => Value::String(s),
		BenchValue::Bytes(b) => Value::Bytes(SurrealBytes::from(b)),
		BenchValue::Uuid(u) => Value::Uuid(SurrealUuid::from(u)),
		BenchValue::DateTime(dt) => Value::Datetime(Datetime::from(dt)),
		BenchValue::Array(a) => {
			Value::Array(Array::from(a.into_iter().map(bench_to_surreal_value).collect::<Vec<_>>()))
		}
		BenchValue::Object(o) => {
			let mut obj = Object::new();
			for (k, v) in o {
				obj.insert(k, bench_to_surreal_value(v));
			}
			Value::Object(obj)
		}
		BenchValue::FloatVector(v) => Value::Array(Array::from(
			v.into_iter().map(|f| Value::Number(Number::Float(f as f64))).collect::<Vec<_>>(),
		)),
	}
}

/// Convert a native [`surrealdb::types::Value`] back to a [`BenchValue`] for
/// round-trip fidelity at the read path.
fn surreal_to_bench_value(v: Value) -> BenchValue {
	match v {
		Value::None | Value::Null => BenchValue::Null,
		Value::Bool(b) => BenchValue::Bool(b),
		Value::Number(Number::Int(i)) => BenchValue::Int(i),
		Value::Number(Number::Float(f)) => BenchValue::Float(f),
		Value::Number(Number::Decimal(d)) => BenchValue::Decimal(d),
		Value::String(s) => BenchValue::String(s),
		Value::Bytes(b) => BenchValue::Bytes(b.to_vec()),
		Value::Uuid(u) => BenchValue::Uuid(u.into()),
		Value::Datetime(dt) => BenchValue::DateTime(*dt),
		Value::Array(a) => {
			BenchValue::Array(Vec::from(a).into_iter().map(surreal_to_bench_value).collect())
		}
		Value::Object(o) => {
			let mut out: Vec<(String, BenchValue)> = Vec::new();
			for (k, v) in o {
				out.push((k, surreal_to_bench_value(v)));
			}
			BenchValue::Object(out)
		}
		// Fall back to canonical SQL-style printing for SurrealDB-specific
		// types that do not have a direct BenchValue variant (Duration,
		// Geometry, RecordId, etc.).
		other => BenchValue::String(other.to_sql()),
	}
}

const DEFAULT: &str = "ws://127.0.0.1:8000";
const TABLE: &str = "record";

/// Map a [`VectorDistance`] to the SurrealQL `DIST <kw>` clause used in
/// `DEFINE INDEX ... HNSW/DISKANN`.
fn surreal_distance_keyword(d: VectorDistance) -> &'static str {
	match d {
		VectorDistance::Cosine => "COSINE",
		VectorDistance::Euclidean => "EUCLIDEAN",
		VectorDistance::InnerProduct => "INNER_PRODUCT",
		VectorDistance::Manhattan => "MANHATTAN",
	}
}

/// Map a [`VectorDistance`] to the SurrealQL function path used by bruteforce scans.
///
/// SurrealDB partitions vector ops between `vector::distance::*` (lower = closer)
/// and `vector::similarity::*` (higher = closer). `cosine` lives under similarity,
/// so the caller must reverse the ORDER BY direction — see [`surreal_distance_order`].
///
/// `InnerProduct` maps to `vector::dot` (raw `Σ aᵢ·bᵢ`) so it matches what
/// pgvector's `<#>` and Redis's `IP` measure. `vector::similarity::pearson`
/// would mean-center the inputs first and produce a bounded [-1, 1]
/// correlation — that's a different metric.
fn surreal_distance_function(d: VectorDistance) -> &'static str {
	match d {
		VectorDistance::Cosine => "vector::similarity::cosine",
		VectorDistance::Euclidean => "vector::distance::euclidean",
		VectorDistance::InnerProduct => "vector::dot",
		VectorDistance::Manhattan => "vector::distance::manhattan",
	}
}

/// ORDER BY direction so the nearest neighbours sort to the top of the result set.
fn surreal_distance_order(d: VectorDistance) -> &'static str {
	match d {
		// Similarity scores: higher is closer.
		VectorDistance::Cosine | VectorDistance::InnerProduct => "DESC",
		// Distance metrics: lower is closer.
		VectorDistance::Euclidean | VectorDistance::Manhattan => "ASC",
	}
}

/// Wraps a SurrealDB [`types::Value`](surrealdb::types::Value);
/// [`BenchValue`] is produced only via [`From`]/[`Into`].
pub(crate) struct Row(pub Value);

impl From<Row> for BenchValue {
	fn from(row: Row) -> BenchValue {
		// `db.select(Resource::from(("record", key)))` compiles to
		// `SELECT * FROM <id>` which always returns a `Value::Array` of
		// matching rows — even for a single-record select. Unwrap the
		// singleton so downstream consumers see the `BenchValue::Object`
		// they expect from a single-row read (otherwise field lookups
		// like `embedding` go through `BenchValue::Array.get_field(...)`
		// and return `None`, causing the vector holdout to skip).
		let inner = match row.0 {
			Value::Array(a) if a.len() == 1 => {
				Vec::from(a).into_iter().next().expect("len == 1 guard")
			}
			other => other,
		};
		surreal_to_bench_value(inner)
	}
}

/// Wrap a SurrealDB result error so the failing SurrealQL surfaces in two
/// places: a `log::error!` line emitted immediately (visible in the bench's
/// stderr stream while it is still running), and the `anyhow::Error` chain
/// returned to the caller (which `main` prints with the `{:#}` formatter so
/// the chain — including the `query: ...` context — is shown end-to-end).
///
/// SurrealDB query errors do not embed the SQL that triggered them, which
/// makes diagnosing failures during long-running scenarios — e.g. a
/// distributed-scan timeout buried four phases into the bench — very
/// awkward. Use at every call site that builds or sends a SurrealQL string:
///
/// ```ignore
/// let sql = format!("SELECT ... {c} {l}");
/// let res = self.db.query(&sql).await
///     .map_err(log_sql_err(&sql))?
///     .take(0)
///     .map_err(log_sql_err(&sql))?;
/// ```
///
/// The closure owns the SQL by value so the returned `FnOnce` is `'static`
/// and composes cleanly with `?` across multiple result-returning steps.
/// Returns true when a SurrealDB driver error originates from the parser
/// rejecting unrecognised syntax (e.g. DiskANN DDL on a SurrealDB build that
/// doesn't ship it yet, or any future version drift). Used by
/// `build_vector_index` to convert those failures into `NOT_SUPPORTED_ERROR`
/// so the framework skips the run cleanly instead of aborting.
///
/// Both the SDK's `Error::Db` path (`Error("Parse error: …")`) and the
/// `Response::check` path surface the SurrealDB core error string verbatim,
/// so a substring match is sufficient and version-agnostic.
fn is_surreal_parse_error<E: std::fmt::Display>(e: &E) -> bool {
	e.to_string().contains("Parse error")
}

/// Returns true when a SurrealDB error is the upstream DiskANN runtime
/// reporting its own search failure. The DiskANN index ANN backend is new
/// in nightly and surfaces intermittent runtime errors (typically on the
/// first query against a freshly built graph). Caught at the scan boundary
/// so the run skips cleanly until the upstream engine stabilises — the
/// check disappears naturally once the runtime no longer emits this
/// message.
fn is_surreal_diskann_runtime_error<E: std::fmt::Display>(e: &E) -> bool {
	e.to_string().contains("DiskANN KNN search failed")
}

fn log_sql_err<E>(sql: &str) -> impl FnOnce(E) -> anyhow::Error
where
	E: std::fmt::Display + Into<anyhow::Error>,
{
	let sql = sql.to_owned();
	move |e| {
		error!("SurrealDB query failed: {sql}\n  cause: {e}");
		// Use the explicit `Into` bound (rather than `anyhow::Error::from(e)`)
		// so the bound documented above resolves: anyhow's blanket `From<E>`
		// requires `E: std::error::Error + Send + Sync + 'static`, but
		// `Into<anyhow::Error>` is what we actually want callers to satisfy.
		let err: anyhow::Error = e.into();
		err.context(format!("query: {sql}"))
	}
}

/// Storage backend for Docker (`server:<backend>`).
pub(crate) enum Docker {
	Memory,
	Rocksdb,
	Surrealkv,
}

/// Connection mode derived from `--endpoint`.
pub(crate) enum Endpoint {
	/// `server:rocksdb` | `server:memory` | `server:surrealkv` (default when omitted: server:rocksdb)
	Docker(Docker),
	/// `rocksdb:…`, `surrealkv:…`, `memory`, `mem://`, `mem:…`
	Embedded(String),
	/// Remote SurrealDB (`ws` / `http` URL)
	Remote(String),
}

/// `true` when crud-bench should start the SurrealDB Docker container.
pub(crate) fn wants_docker(endpoint: Option<&str>) -> bool {
	matches!(parse_endpoint(endpoint), Ok(Endpoint::Docker(_)))
}

pub(crate) fn parse_endpoint(opt: Option<&str>) -> Result<Endpoint> {
	// If no endpoint is specified, use the default
	let Some(raw) = opt else {
		return Ok(Endpoint::Docker(Docker::Rocksdb));
	};
	// Trim whitespace from the endpoint
	let s = raw.trim();
	// If the endpoint is empty, use the default
	if s.is_empty() {
		return Ok(Endpoint::Docker(Docker::Rocksdb));
	}
	// If the endpoint is a remote endpoint, return it
	if s.starts_with("ws://")
		|| s.starts_with("wss://")
		|| s.starts_with("http://")
		|| s.starts_with("https://")
	{
		return Ok(Endpoint::Remote(s.to_string()));
	}
	// If the endpoint is a server backend, return it
	if let Some(rest) = s.strip_prefix("server:") {
		return match rest {
			"rocksdb" => Ok(Endpoint::Docker(Docker::Rocksdb)),
			"memory" => Ok(Endpoint::Docker(Docker::Memory)),
			"surrealkv" => Ok(Endpoint::Docker(Docker::Surrealkv)),
			_ => bail!(
				"Invalid server backend {rest:?}. Expected server:rocksdb, server:memory, or server:surrealkv.",
			),
		};
	}
	// If the endpoint is a memory endpoint, return it
	if s == "memory" {
		return Ok(Endpoint::Embedded("mem://".to_string()));
	}
	// If the endpoint is a memory endpoint, return it
	if s.starts_with("mem:") {
		return Ok(Endpoint::Embedded(s.to_string()));
	}
	// If the endpoint is a rocksdb or surrealkv endpoint, return it
	if s.starts_with("rocksdb:") || s.starts_with("surrealkv:") {
		return Ok(Endpoint::Embedded(s.to_string()));
	}
	bail!(
		"Invalid SurrealDB endpoint {:?}. Expected:\n\
		 - server:rocksdb | server:memory | server:surrealkv (Docker)\n\
		 - rocksdb:<path>[?args] | surrealkv:<path>[?args] | memory | mem:// | mem:<path>[?args] (embedded)\n\
		 - ws://... | wss://... | http://... | https://... (remote)",
		s
	)
}

/// Calculate SurrealDB RocksDB specific memory allocation
fn calculate_surrealdb_memory() -> u64 {
	// Load the system memory
	let memory = MemoryConfig::new();
	// Use ~80% of recommended cache allocation
	(memory.cache_gb * 4 / 6).max(1)
}

/// Retrieves the SurrealDB username from the environment variable or returns the default.
///
/// Reads the `SURREALDB_USER` environment variable. If not set, defaults to `"root"`.
/// This username is used for both Docker container startup and client authentication.
pub(super) fn surrealdb_username() -> String {
	env::var("SURREALDB_USER").unwrap_or_else(|_| String::from("root"))
}

/// Retrieves the SurrealDB password from the environment variable or returns the default.
///
/// Reads the `SURREALDB_PASS` environment variable. If not set, defaults to `"root"`.
/// This password is used for both Docker container startup and client authentication.
pub(super) fn surrealdb_password() -> String {
	env::var("SURREALDB_PASS").unwrap_or_else(|_| String::from("root"))
}

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	// Parse the endpoint or use the default
	let backend =
		parse_endpoint(options.endpoint.as_deref()).unwrap_or(Endpoint::Docker(Docker::Rocksdb));
	// Calculate memory allocation
	let cache_gb = calculate_surrealdb_memory();
	// Get credentials from environment variables or use defaults
	let username = surrealdb_username();
	let password = surrealdb_password();
	// Configure the sync parameter
	let sync = if options.sync {
		"every"
	} else {
		"never"
	};
	// Return Docker parameters
	match backend {
		Endpoint::Embedded(_) | Endpoint::Remote(_) => {
			unreachable!("docker() must only be called when wants_docker is true")
		}
		Endpoint::Docker(Docker::Memory) => DockerParams {
			image: "surrealdb/surrealdb:nightly",
			pre_args: "--ulimit nofile=65536:65536 -p 8000:8000 --user root".to_string(),
			post_args: match options.persisted {
				true => {
					format!(
						"start --user {username} --pass {password} mem://?crud-bench.db?aol=sync&sync={sync}"
					)
				}
				false => {
					format!("start --user {username} --pass {password} mem://")
				}
			},
		},
		Endpoint::Docker(Docker::Rocksdb) => DockerParams {
			image: "surrealdb/surrealdb:nightly",
			pre_args: match options.optimised {
				true => format!(
					"--ulimit nofile=65536:65536 -p 8000:8000 -e SURREAL_ROCKSDB_BLOCK_CACHE_SIZE={cache_gb}GB --user root",
				),
				false => "--ulimit nofile=65536:65536 -p 8000:8000 --user root".to_string(),
			},
			post_args: format!(
				"start --user {username} --pass {password} rocksdb:/data/crud-bench.db?sync={sync}",
			),
		},
		Endpoint::Docker(Docker::Surrealkv) => DockerParams {
			image: "surrealdb/surrealdb:nightly",
			pre_args: match options.optimised {
				true => format!(
					"--ulimit nofile=65536:65536 -p 8000:8000 -e SURREAL_SURREALKV_MAX_VALUE_CACHE_SIZE={cache_gb}GB --user root",
				),
				false => "--ulimit nofile=65536:65536 -p 8000:8000 --user root".to_string(),
			},
			post_args: format!(
				"start --user {username} --pass {password} surrealkv:/data/crud-bench.db?sync={sync}",
			),
		},
	}
}

pub(crate) struct SurrealDBClientProvider {
	client: Option<Surreal<Any>>,
	endpoint: String,
	root: Root,
}

pub(super) async fn initialise_db(endpoint: &str, root: Root) -> Result<Surreal<Any>> {
	// Set the root user
	let config = Config::new().user(root.clone());
	// Connect to the database
	let db = connect((endpoint, config)).await?;
	// Signin as a namespace, database, or root user
	db.signin(root).await?;
	// Select a specific namespace / database
	db.use_ns("test").use_db("test").await?;
	// Return the client
	Ok(db)
}

impl BenchmarkEngine<SurrealDBClient> for SurrealDBClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Parse the benchmark engine endpoint
		let mode = parse_endpoint(options.endpoint.as_deref())?;
		// Define root user details from environment variables or use defaults
		let username = surrealdb_username();
		let password = surrealdb_password();
		let root = Root {
			username,
			password,
		};
		// Create the benchmark engine client
		let (endpoint, client) = match mode {
			Endpoint::Docker(_) => (DEFAULT.to_string(), None),
			Endpoint::Remote(url) => (url, None),
			Endpoint::Embedded(url) => {
				// Configure the sync parameter
				let sync = if options.sync {
					"every"
				} else {
					"never"
				};
				// Configure the full endpoint URL
				let full_url = if url.contains('?') {
					format!("{url}&sync={sync}")
				} else {
					format!("{url}?sync={sync}")
				};
				let db = initialise_db(&full_url, root.clone()).await?;
				(full_url, Some(db))
			}
		};
		// Return the client provider
		Ok(Self {
			endpoint,
			root,
			client,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<SurrealDBClient> {
		let client = match &self.client {
			Some(client) => client.clone(),
			None => initialise_db(&self.endpoint, self.root.clone()).await?,
		};
		Ok(SurrealDBClient::new(client))
	}
}

pub(crate) struct SurrealDBClient {
	db: Surreal<Any>,
}

impl SurrealDBClient {
	pub(super) const fn new(db: Surreal<Any>) -> Self {
		Self {
			db,
		}
	}
}

#[derive(Debug, SurrealValue)]
#[surreal(crate = "surrealdb::types")]
struct Bindings<T: SurrealValue> {
	content: Value,
	id: T,
}

impl BenchmarkClient for SurrealDBClient {
	// The return type when reading a row
	type ReadRow = Row;

	async fn startup(&self) -> Result<()> {
		// Ensure the table exists. This wouldn't
		// normally be an issue, as SurrealDB is
		// schemaless. However, because we are testing
		// benchmarking of concurrent, optimistic
		// transactions, each initial concurrent
		// insert/create into the table attempts
		// to set up the NS+DB+TB, and this causes
		// 'resource busy' key conflict failures.
		let sql = "
            REMOVE TABLE IF EXISTS record;
			DEFINE TABLE record;
		";
		self.db.query(sql).await.map_err(log_sql_err(sql))?.check().map_err(log_sql_err(sql))?;
		Ok(())
	}

	async fn compact(&self) -> Result<()> {
		// Issue a system compaction request
		let sql = "ALTER SYSTEM COMPACT";
		// Compact all databases and namespaces in SurrealDB
		let response = self.db.query(sql).await.map_err(log_sql_err(sql))?;
		// Compaction only works on RocksDB storage engine
		match response.check() {
			Ok(_) => Ok(()),
			Err(e) => {
				if e.to_string().contains("does not support compaction") {
					Ok(())
				} else {
					Err(log_sql_err(sql)(e))
				}
			}
		}
	}

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.create(key as i64, val).await
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<Row> {
		self.read(key as i64).await
	}

	async fn read_string(&self, key: String) -> Result<Row> {
		self.read(key).await
	}

	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.update(key as i64, val).await
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.update(key, val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key as i64).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key).await
	}

	async fn build_index(&self, spec: &Index, name: &str) -> Result<()> {
		// Get the unique flag
		let unique = if spec.unique.unwrap_or(false) {
			"UNIQUE"
		} else {
			""
		}
		.to_string();
		// Get the fields
		let fields = spec.fields.join(", ");
		// Check if an index type is specified
		let sql = match &spec.index_type {
			Some(kind) if kind == "fulltext" => {
				// Define the analyzer
				let sql = format!(
					"DEFINE ANALYZER IF NOT EXISTS {name} TOKENIZERS blank,class FILTERS lowercase,ascii;"
				);
				self.db
					.query(&sql)
					.await
					.map_err(log_sql_err(&sql))?
					.check()
					.map_err(log_sql_err(&sql))?;
				// Define the index concurrently (so we don't maintain an open transaction during the indexing
				format!(
					"DEFINE INDEX {name} ON TABLE record FIELDS {fields} FULLTEXT ANALYZER {name} BM25 CONCURRENTLY"
				)
			}
			_ => {
				format!("DEFINE INDEX {name} ON TABLE record FIELDS {fields} {unique} CONCURRENTLY")
			}
		};
		// Create the index
		self.db.query(&sql).await.map_err(log_sql_err(&sql))?.check().map_err(log_sql_err(&sql))?;
		// Wait until the index is ready
		loop {
			let sql = format!("INFO FOR INDEX {name} ON record");
			let r: surrealdb::types::Value = self
				.db
				.query(&sql)
				.await
				.map_err(log_sql_err(&sql))?
				.take(0)
				.map_err(log_sql_err(&sql))?;
			let j = r.to_sql();
			let building = r.get("building");
			let status = building.get("status").as_string().expect(&j);
			match status.as_str() {
				"ready" => break,
				"indexing" | "cleaning" | "started" => {}
				_ => bail!("Unexpected status: {}", r.into_json_value()),
			}
			sleep(Duration::from_millis(500)).await;
		}
		// All ok
		Ok(())
	}

	async fn drop_index(&self, name: &str) -> Result<()> {
		// Retry helper closure for handling transient "Resource busy" errors.
		//
		// ## Why Retry is Necessary
		//
		// After intensive concurrent scan operations (e.g., 12 clients × 48 threads = 576 tasks),
		// each scan creates a READ transaction in SurrealDB that holds a RocksDB snapshot.
		// These snapshots capture the database state and provide MVCC (Multi-Version Concurrency Control).
		//
		// When REMOVE INDEX executes immediately after scans complete:
		// 1. Client-side: All Rust futures have finished (via try_join_all)
		// 2. Server-side: SurrealDB/RocksDB may still have:
		//    - Active transaction objects not yet fully released
		//    - Snapshot references held in memory
		//    - Deferred cleanup operations in progress
		//
		// ## The Conflict
		//
		// REMOVE INDEX runs in a WRITE transaction that needs to:
		// - Delete index metadata keys (del_tb_index)
		// - Update table definition (put_tb)
		// - Clear caches
		//
		// RocksDB's optimistic transaction engine detects conflicts between:
		// - Active READ snapshots from completed scan operations
		// - WRITE transaction from REMOVE INDEX trying to modify metadata
		//
		// This results in: "The query was not executed due to a failed transaction. Resource busy:"
		//
		// ## Why Retry Works
		//
		// The error is transient. As transaction objects are dropped and snapshots released,
		// the metadata locks become available. The 500ms sleep allows sufficient time for:
		// - Async transaction cleanup to complete
		// - RocksDB to release internal snapshot references
		// - Memory management to finalize deferred operations
		//
		// Since REMOVE INDEX IF EXISTS is idempotent, retrying is safe and appropriate
		// for benchmark scenarios where the goal is reliable completion rather than
		// immediate failure on transient resource contention.
		let retry = |sql: String, max_wait: Duration| async move {
			let fut = async {
				loop {
					match self.db.query(&sql).await?.check() {
						Ok(_) => return Ok(()),
						Err(e) => {
							let msg = e.to_string();
							// Be permissive on the match to tolerate tiny wording changes.
							// We accept both the executor-level wrapper and the raw KV
							// transaction conflict error, which surfaces directly when the
							// failing statement is the only one in the query.
							const RETRYABLE: &[&str] = &[
								"This transaction can be retried",
								"The query was not executed due to a failed transaction",
							];
							if RETRYABLE.iter().any(|p| msg.contains(p)) {
								warn!("Retrying {sql} due to {msg}");
								sleep(Duration::from_millis(500)).await;
								continue;
							}
							return Err(e);
						}
					}
				}
			};
			match timeout(max_wait, fut).await {
				Ok(res) => res.map_err(|e| e.into()),
				Err(_) => {
					bail!("Timed out after {:?} waiting to execute: {}", max_wait, sql)
				}
			}
		};
		// Remove the index
		let sql = format!("REMOVE INDEX IF EXISTS {name} ON TABLE record");
		retry(sql, Duration::from_secs(300)).await?;
		// Remove the analyzer
		let sql = format!("REMOVE ANALYZER IF EXISTS {name}");
		retry(sql, Duration::from_secs(180)).await?;
		// All ok
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		self.scan(scan, ctx).await
	}

	async fn scan_string(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		self.scan(scan, ctx).await
	}

	async fn build_vector_index(
		&self,
		spec: &Index,
		vq: &VectorQuerySpec,
		dim: usize,
		name: &str,
	) -> Result<()> {
		let fields = spec.fields.join(", ");
		let dist = surreal_distance_keyword(vq.distance);
		let sql = match vq.index_strategy {
			VectorIndexStrategy::Bruteforce => {
				// Bruteforce on SurrealDB means no auxiliary index; the scan
				// path uses raw vector::distance::* with ORDER BY LIMIT.
				bail!(NOT_SUPPORTED_ERROR)
			}
			VectorIndexStrategy::Hnsw {
				m,
				ef_construction,
				..
			} => {
				format!(
					"DEFINE INDEX {name} ON TABLE record FIELDS {fields} HNSW DIMENSION {dim} DIST {dist} EFC {ef_construction} M {m} CONCURRENTLY"
				)
			}
			VectorIndexStrategy::DiskAnn {
				degree,
				l_build,
				alpha,
				..
			} => {
				format!(
					"DEFINE INDEX {name} ON TABLE record FIELDS {fields} DISKANN DIMENSION {dim} DISTANCE {dist} DEGREE {degree} L_BUILD {l_build} ALPHA {alpha} CONCURRENTLY"
				)
			}
		};
		// Treat any parse error as NotSupported: the running SurrealDB
		// version doesn't recognise the DDL (DiskANN before the syntax
		// lands in OSS, future drift between releases, etc.). The
		// framework already handles NotSupported as a clean skip.
		let resp = match self.db.query(&sql).await {
			Ok(r) => r,
			Err(e) if is_surreal_parse_error(&e) => bail!(NOT_SUPPORTED_ERROR),
			Err(e) => return Err(log_sql_err(&sql)(e)),
		};
		if let Err(e) = resp.check() {
			if is_surreal_parse_error(&e) {
				bail!(NOT_SUPPORTED_ERROR);
			}
			return Err(log_sql_err(&sql)(e));
		}
		// Wait until the index is ready (same poll loop as `build_index`).
		loop {
			let q = format!("INFO FOR INDEX {name} ON record");
			let r: surrealdb::types::Value = self
				.db
				.query(&q)
				.await
				.map_err(log_sql_err(&q))?
				.take(0)
				.map_err(log_sql_err(&q))?;
			let j = r.to_sql();
			let building = r.get("building");
			let status = building.get("status").as_string().expect(&j);
			match status.as_str() {
				"ready" => break,
				"indexing" | "cleaning" | "started" => {}
				_ => bail!("Unexpected status: {}", r.into_json_value()),
			}
			sleep(Duration::from_millis(500)).await;
		}
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
		self.batch_create(key_vals.map(|(k, v)| (k as i64, v))).await
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals).await
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_read(keys.map(|k| k as i64)).await
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_read(keys).await
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals.map(|(k, v)| (k as i64, v))).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals).await
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_delete(keys.map(|k| k as i64)).await
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_delete(keys).await
	}
}

impl SurrealDBClient {
	async fn create<T>(&self, key: T, val: BenchValue) -> Result<()>
	where
		T: Into<RecordIdKey>,
	{
		let sql = "CREATE $id CONTENT $content RETURN NULL";
		let content = bench_to_surreal_value(val);
		let res = self
			.db
			.query(sql)
			.bind(Bindings {
				id: Value::RecordId(RecordId::new(TABLE, key)),
				content,
			})
			.await
			.map_err(log_sql_err(sql))?
			.take::<surrealdb::types::Value>(0)
			.map_err(log_sql_err(sql))?;
		assert!(!res.is_none());
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<Row>
	where
		T: Into<RecordIdKey>,
	{
		let v: Value = self.db.select(Resource::from(("record", key))).await?;
		assert!(!v.is_none());
		Ok(black_box(Row(v)))
	}

	async fn update<T>(&self, key: T, val: BenchValue) -> Result<()>
	where
		T: Into<RecordIdKey>,
	{
		let sql = "UPDATE $id CONTENT $content RETURN NULL";
		// Strip any `id` field from the content — `UPDATE $id CONTENT $content`
		// rejects content that carries an explicit id ("Found 'record:…' for
		// the `id` field, but a specific record has been specified"). The
		// read-back side now hands back a proper Object (post-singleton-Array
		// unwrap), so mixed read/write workloads round-trip the id field
		// unless we drop it here.
		let mut content = bench_to_surreal_value(val);
		if let Value::Object(ref mut obj) = content {
			obj.remove("id");
		}
		let res = self
			.db
			.query(sql)
			.bind(Bindings {
				id: Value::RecordId(RecordId::new(TABLE, key)),
				content,
			})
			.await
			.map_err(log_sql_err(sql))?
			.take::<surrealdb::types::Value>(0)
			.map_err(log_sql_err(sql))?;
		assert!(!res.is_none());
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: Into<RecordIdKey>,
	{
		let sql = "DELETE $id RETURN NULL";
		let res = self
			.db
			.query(sql)
			.bind(("id", Value::RecordId(RecordId::new(TABLE, key))))
			.await
			.map_err(log_sql_err(sql))?
			.take::<surrealdb::types::Value>(0)
			.map_err(log_sql_err(sql))?;
		assert!(!res.is_none());
		Ok(())
	}

	async fn knn_scan(&self, scan: &Scan, query: &[f32]) -> Result<usize> {
		let vq = scan.vector_query.as_ref().ok_or_else(|| {
			anyhow::anyhow!("knn_scan called without a vector_query on scan `{}`", scan.name)
		})?;
		let field = &vq.field;
		let k = vq.top_k;
		let is_diskann = matches!(vq.index_strategy, VectorIndexStrategy::DiskAnn { .. });
		let sql = match vq.index_strategy {
			VectorIndexStrategy::Bruteforce => {
				let func_path = surreal_distance_function(vq.distance);
				let dir = surreal_distance_order(vq.distance);
				// Aliased distance so the parser's "ORDER BY idiom must appear
				// in SELECT" rule is satisfied (surrealdb-private 0df9e38c era).
				format!(
					"SELECT id, {func_path}({field}, $q) AS _d FROM record ORDER BY _d {dir} LIMIT {k}"
				)
			}
			VectorIndexStrategy::Hnsw {
				ef_search,
				..
			} => {
				format!("SELECT id FROM record WHERE {field} <|{k},{ef_search}|> $q")
			}
			VectorIndexStrategy::DiskAnn {
				l_search,
				..
			} => {
				format!("SELECT id FROM record WHERE {field} <|{k},{l_search}|> $q")
			}
		};
		// Bind the query vector as a SurrealQL array so the server doesn't
		// re-parse a multi-KB array literal on every iteration.
		let q_value = Value::Array(Array::from(
			query.iter().map(|f| Value::Number(Number::Float(*f as f64))).collect::<Vec<_>>(),
		));
		// DiskANN runtime in current nightly intermittently fails the search
		// itself even though the index built successfully. Treat that as
		// NotSupported so the scan skips cleanly — same pattern as the parse
		// error path in `build_vector_index`. The check is scoped to the
		// DiskANN strategy so genuine errors on Bruteforce / HNSW still abort.
		let mut resp = match self.db.query(&sql).bind(("q", q_value)).await {
			Ok(r) => r,
			Err(e) if is_diskann && is_surreal_diskann_runtime_error(&e) => {
				bail!(NOT_SUPPORTED_ERROR)
			}
			Err(e) => return Err(log_sql_err(&sql)(e)),
		};
		let res: surrealdb::types::Value = match resp.take(0) {
			Ok(v) => v,
			Err(e) if is_diskann && is_surreal_diskann_runtime_error(&e) => {
				bail!(NOT_SUPPORTED_ERROR)
			}
			Err(e) => return Err(log_sql_err(&sql)(e)),
		};
		let Some(arr) = res.as_array() else {
			bail!("knn scan: unexpected response shape: {}", res.to_sql());
		};
		Ok(arr.len())
	}

	async fn scan(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		// SurrealDB requires a full-text index to use the @@ operator
		if ctx == ScanContext::WithoutIndex
			&& let Some(index) = &scan.with_index
			&& let Some(kind) = &index.index_type
			&& kind == "fulltext"
		{
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.map(|s| format!("START {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = SurrealDBDialect::filter_clause(scan)?;
		let o = SurrealDBDialect::order_by_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let sql = format!("SELECT id FROM record {c} {o} {s} {l}");
				let res: surrealdb::types::Value = self
					.db
					.query(&sql)
					.await
					.map_err(log_sql_err(&sql))?
					.take(0)
					.map_err(log_sql_err(&sql))?;
				let Some(arr) = res.as_array() else {
					panic!("Unexpected response type");
				};
				Ok(arr.len())
			}
			Projection::Full => {
				let sql = format!("SELECT * FROM record {c} {o} {s} {l}");
				let res: surrealdb::types::Value = self
					.db
					.query(&sql)
					.await
					.map_err(log_sql_err(&sql))?
					.take(0)
					.map_err(log_sql_err(&sql))?;
				let Some(arr) = res.as_array() else {
					panic!("Unexpected response type");
				};
				Ok(arr.len())
			}
			Projection::Count => {
				let sql = if s.is_empty() && l.is_empty() {
					format!("SELECT count() FROM record {c} GROUP ALL")
				} else {
					format!("SELECT count() FROM (SELECT 1 FROM record {c} {s} {l}) GROUP ALL")
				};
				let res: Option<usize> = self
					.db
					.query(&sql)
					.await
					.map_err(log_sql_err(&sql))?
					.take("count")
					.map_err(log_sql_err(&sql))?;
				Ok(res.unwrap())
			}
		}
	}

	async fn batch_create<K>(
		&self,
		key_vals: impl Iterator<Item = (K, BenchValue)> + Send,
	) -> Result<()>
	where
		K: Into<RecordIdKey>,
	{
		// Construct the records
		let rows: Vec<Value> = key_vals
			.map(|(k, v)| {
				let mut obj = match bench_to_surreal_value(v) {
					Value::Object(o) => o,
					_ => panic!("Unexpected value type"),
				};
				obj.insert("id", Value::RecordId(RecordId::new(TABLE, k)));
				Value::Object(obj)
			})
			.collect();
		// Construct the SQL query
		let sql = "INSERT $rows RETURN NONE";
		// Execute the SQL query
		self.db
			.query(sql)
			.bind(("rows", Value::Array(Array::from(rows))))
			.await
			.map_err(log_sql_err(sql))?
			.check()
			.map_err(log_sql_err(sql))?;
		// All ok
		Ok(())
	}

	async fn batch_read<K>(&self, keys: impl Iterator<Item = K> + Send) -> Result<()>
	where
		K: Into<RecordIdKey>,
	{
		// Construct the Record IDs
		let ids: Vec<Value> = keys.map(|k| Value::RecordId(RecordId::new("record", k))).collect();
		// Store the total Record IDs
		let ids_len = ids.len();
		// Construct the SQL query
		let sql = "SELECT * FROM $ids";
		// Execute the SQL query
		let res: surrealdb::types::Value = self
			.db
			.query(sql)
			.bind(("ids", Value::Array(Array::from(ids))))
			.await
			.map_err(log_sql_err(sql))?
			.take(0)
			.map_err(log_sql_err(sql))?;
		// Check the response
		let Some(arr) = res.as_array() else {
			panic!("Unexpected response type");
		};
		assert_eq!(arr.len(), ids_len);
		// All ok
		Ok(())
	}

	async fn batch_update<K>(
		&self,
		key_vals: impl Iterator<Item = (K, BenchValue)> + Send,
	) -> Result<()>
	where
		K: Into<RecordIdKey>,
	{
		// Construct the records
		let rows: Vec<Value> = key_vals
			.map(|(k, v)| {
				let mut obj = match bench_to_surreal_value(v) {
					Value::Object(o) => o,
					_ => panic!("Unexpected value type"),
				};
				obj.insert("id", Value::RecordId(RecordId::new(TABLE, k)));
				Value::Object(obj)
			})
			.collect();
		// Construct the SQL query
		let sql = "FOR $row IN $rows { UPDATE $row.id CONTENT $row RETURN NONE }";
		// Execute the SQL query
		self.db
			.query(sql)
			.bind(("rows", Value::Array(Array::from(rows))))
			.await
			.map_err(log_sql_err(sql))?
			.check()
			.map_err(log_sql_err(sql))?;
		Ok(())
	}

	async fn batch_delete<K>(&self, keys: impl Iterator<Item = K> + Send) -> Result<()>
	where
		K: Into<RecordIdKey>,
	{
		// Construct the Record IDs
		let ids: Vec<Value> = keys.map(|k| Value::RecordId(RecordId::new("record", k))).collect();
		// Construct the SQL query
		let sql = "DELETE $ids";
		// Execute the SQL query
		self.db
			.query(sql)
			.bind(("ids", Value::Array(Array::from(ids))))
			.await
			.map_err(log_sql_err(sql))?
			.check()
			.map_err(log_sql_err(sql))?;
		// All ok
		Ok(())
	}
}
