#![cfg(feature = "surrealdb2")]

//! SurrealDB 2.x adapter.
//!
//! Parallel to [crate::surrealdb], but linked against the v2 SDK (renamed in
//! Cargo.toml as `surrealdb2`). The v2 SDK can only talk to a v2 server (the
//! RPC protocol changed in v3), and v2 lives in `surrealdb::sql::*` rather
//! than v3's `surrealdb::types::*`. Surreal-side syntax diffs vs v3:
//! `ALTER SYSTEM COMPACT` doesn't exist, and the DiskANN vector index isn't
//! shipped yet — both are caught at runtime (compact is a no-op; DiskANN
//! DDL trips the parse-error path and is reported as NotSupported).

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
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::env;
use std::hint::black_box;
use std::time::Duration;
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::Resource;
use surrealdb::opt::auth::Root;
use surrealdb::sql::{
	Array, Bytes as SurrealBytes, Datetime, Id, Number, Object, Strand, Thing, Uuid as SurrealUuid,
	Value,
};
use surrealdb2 as surrealdb;
use tokio::time::{sleep, timeout};

/// Convert a [`BenchValue`] to a native v2 [`Value`].
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
		BenchValue::String(s) => Value::Strand(Strand::from(s)),
		BenchValue::Bytes(b) => Value::Bytes(SurrealBytes::from(b)),
		BenchValue::Uuid(u) => Value::Uuid(SurrealUuid::from(u)),
		BenchValue::DateTime(dt) => Value::Datetime(Datetime::from(dt)),
		BenchValue::Array(a) => {
			Value::Array(Array::from(a.into_iter().map(bench_to_surreal_value).collect::<Vec<_>>()))
		}
		BenchValue::Object(o) => {
			let mut map: BTreeMap<String, Value> = BTreeMap::new();
			for (k, v) in o {
				map.insert(k, bench_to_surreal_value(v));
			}
			Value::Object(Object::from(map))
		}
		BenchValue::FloatVector(v) => Value::Array(Array::from(
			v.into_iter().map(|f| Value::Number(Number::Float(f as f64))).collect::<Vec<_>>(),
		)),
	}
}

/// Convert a native v2 [`Value`] back to a [`BenchValue`]. For variants that
/// don't have a direct BenchValue mapping (Duration, Geometry, Thing, …) we
/// fall back to the SurrealQL `Display` form, mirroring the v3 adapter.
fn surreal_to_bench_value(v: Value) -> BenchValue {
	match v {
		Value::None | Value::Null => BenchValue::Null,
		Value::Bool(b) => BenchValue::Bool(b),
		Value::Number(Number::Int(i)) => BenchValue::Int(i),
		Value::Number(Number::Float(f)) => BenchValue::Float(f),
		Value::Number(Number::Decimal(d)) => BenchValue::Decimal(d),
		Value::Strand(s) => BenchValue::String(s.0),
		Value::Bytes(b) => BenchValue::Bytes(Vec::<u8>::from(b)),
		Value::Uuid(u) => BenchValue::Uuid(u.0),
		Value::Datetime(dt) => BenchValue::DateTime(dt.0),
		Value::Array(a) => BenchValue::Array(a.0.into_iter().map(surreal_to_bench_value).collect()),
		Value::Object(o) => {
			let mut out: Vec<(String, BenchValue)> = Vec::new();
			for (k, v) in o.0 {
				out.push((k, surreal_to_bench_value(v)));
			}
			BenchValue::Object(out)
		}
		other => BenchValue::String(other.to_string()),
	}
}

const DEFAULT: &str = "ws://127.0.0.1:8000";
const TABLE: &str = "record";

fn surreal_distance_keyword(d: VectorDistance) -> &'static str {
	match d {
		VectorDistance::Cosine => "COSINE",
		VectorDistance::Euclidean => "EUCLIDEAN",
		VectorDistance::InnerProduct => "INNER_PRODUCT",
		VectorDistance::Manhattan => "MANHATTAN",
	}
}

fn surreal_distance_function(d: VectorDistance) -> &'static str {
	match d {
		VectorDistance::Cosine => "vector::similarity::cosine",
		VectorDistance::Euclidean => "vector::distance::euclidean",
		VectorDistance::InnerProduct => "vector::dot",
		VectorDistance::Manhattan => "vector::distance::manhattan",
	}
}

fn surreal_distance_order(d: VectorDistance) -> &'static str {
	match d {
		VectorDistance::Cosine | VectorDistance::InnerProduct => "DESC",
		VectorDistance::Euclidean | VectorDistance::Manhattan => "ASC",
	}
}

/// Wraps a v2 [`Value`]; [`BenchValue`] is produced only via [`From`]/[`Into`].
pub(crate) struct Row(pub Value);

impl From<Row> for BenchValue {
	fn from(row: Row) -> BenchValue {
		// `db.select(Resource::from(rid))` returns a `Value::Array` of matching
		// rows even for a single record — unwrap the singleton so the caller
		// sees `BenchValue::Object` rather than `BenchValue::Array(vec![Object])`.
		let inner = match row.0 {
			Value::Array(a) if a.0.len() == 1 => a.0.into_iter().next().expect("len == 1 guard"),
			other => other,
		};
		surreal_to_bench_value(inner)
	}
}

fn is_surreal_parse_error<E: std::fmt::Display>(e: &E) -> bool {
	e.to_string().contains("Parse error")
}

fn log_sql_err<E>(sql: &str) -> impl FnOnce(E) -> anyhow::Error
where
	E: std::fmt::Display + Into<anyhow::Error>,
{
	let sql = sql.to_owned();
	move |e| {
		error!("SurrealDB v2 query failed: {sql}\n  cause: {e}");
		let err: anyhow::Error = e.into();
		err.context(format!("query: {sql}"))
	}
}

const RETRYABLE_CONFLICT_MARKERS: &[&str] =
	&["This transaction can be retried", "The query was not executed due to a failed transaction"];

fn is_retryable_conflict<E: std::fmt::Display>(e: &E) -> bool {
	let msg = e.to_string();
	RETRYABLE_CONFLICT_MARKERS.iter().any(|p| msg.contains(p))
}

async fn run_dml_with_retry<F, Fut>(sql: &str, mut op: F) -> Result<()>
where
	F: FnMut() -> Fut,
	Fut: std::future::Future<Output = std::result::Result<(), surrealdb::Error>>,
{
	const MAX_RETRIES: u32 = 16;
	const MAX_DELAY: Duration = Duration::from_millis(100);
	let mut delay = Duration::from_millis(1);
	let mut attempts = 0u32;
	loop {
		match op().await {
			Ok(()) => return Ok(()),
			Err(e) if is_retryable_conflict(&e) && attempts < MAX_RETRIES => {
				attempts += 1;
				warn!("Retrying {sql} due to transaction conflict (attempt {attempts}): {e}");
				sleep(delay).await;
				delay = (delay * 2).min(MAX_DELAY);
			}
			Err(e) => return Err(log_sql_err(sql)(e)),
		}
	}
}

pub(crate) enum Docker {
	Memory,
	Rocksdb,
	Surrealkv,
}

pub(crate) enum Endpoint {
	Docker(Docker),
	Embedded(String),
	Remote(String),
}

pub(crate) fn wants_docker(endpoint: Option<&str>) -> bool {
	matches!(parse_endpoint(endpoint), Ok(Endpoint::Docker(_)))
}

pub(crate) fn parse_endpoint(opt: Option<&str>) -> Result<Endpoint> {
	let Some(raw) = opt else {
		return Ok(Endpoint::Docker(Docker::Rocksdb));
	};
	let s = raw.trim();
	if s.is_empty() {
		return Ok(Endpoint::Docker(Docker::Rocksdb));
	}
	if s.starts_with("ws://")
		|| s.starts_with("wss://")
		|| s.starts_with("http://")
		|| s.starts_with("https://")
	{
		return Ok(Endpoint::Remote(s.to_string()));
	}
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
	if s == "memory" {
		return Ok(Endpoint::Embedded("mem://".to_string()));
	}
	if s.starts_with("mem:") {
		return Ok(Endpoint::Embedded(s.to_string()));
	}
	if s.starts_with("rocksdb:") || s.starts_with("surrealkv:") {
		return Ok(Endpoint::Embedded(s.to_string()));
	}
	bail!(
		"Invalid SurrealDB v2 endpoint {:?}. Expected:\n\
		 - server:rocksdb | server:memory | server:surrealkv (Docker)\n\
		 - rocksdb:<path>[?args] | surrealkv:<path>[?args] | memory | mem:// | mem:<path>[?args] (embedded)\n\
		 - ws://... | wss://... | http://... | https://... (remote)",
		s
	)
}

fn calculate_surrealdb_memory() -> u64 {
	let memory = MemoryConfig::new();
	(memory.cache_gb * 4 / 6).max(1)
}

/// Reuse the same env vars as the v3 adapter so a host running both flavours
/// can share credentials.
pub(super) fn surrealdb_username() -> String {
	env::var("SURREALDB_USER").unwrap_or_else(|_| String::from("root"))
}

pub(super) fn surrealdb_password() -> String {
	env::var("SURREALDB_PASS").unwrap_or_else(|_| String::from("root"))
}

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	let backend =
		parse_endpoint(options.endpoint.as_deref()).unwrap_or(Endpoint::Docker(Docker::Rocksdb));
	let cache_gb = calculate_surrealdb_memory();
	let username = surrealdb_username();
	let password = surrealdb_password();
	// v2 doesn't parse `?sync=` off the datastore path the way v3 does
	// (`Datastore::new` matches the path verbatim). The only switch for
	// fsync-on-commit in v2 is the `SURREAL_SYNC_DATA` env var — both the
	// RocksDB and SurrealKV engines key off it (`cnf::SYNC_DATA`, default
	// `false`). Forward `--sync` through as that env var so v2 benchmarks
	// honour the same durability contract as v3 (`?sync=every`).
	let sync_env = if options.sync {
		" -e SURREAL_SYNC_DATA=true"
	} else {
		""
	};
	match backend {
		Endpoint::Embedded(_) | Endpoint::Remote(_) => {
			unreachable!("docker() must only be called when wants_docker is true")
		}
		Endpoint::Docker(Docker::Memory) => DockerParams {
			image: "surrealdb/surrealdb:v2.6.5",
			pre_args: format!("--ulimit nofile=65536:65536 -p 8000:8000{sync_env} --user root"),
			post_args: format!("start --user {username} --pass {password} memory"),
		},
		Endpoint::Docker(Docker::Rocksdb) => DockerParams {
			image: "surrealdb/surrealdb:v2.6.5",
			pre_args: match options.optimised {
				true => format!(
					"--ulimit nofile=65536:65536 -p 8000:8000 -e SURREAL_ROCKSDB_BLOCK_CACHE_SIZE={cache_gb}GB{sync_env} --user root",
				),
				false => format!("--ulimit nofile=65536:65536 -p 8000:8000{sync_env} --user root"),
			},
			post_args: format!(
				"start --user {username} --pass {password} rocksdb:/data/crud-bench.db",
			),
		},
		Endpoint::Docker(Docker::Surrealkv) => DockerParams {
			image: "surrealdb/surrealdb:v2.6.5",
			pre_args: match options.optimised {
				true => format!(
					"--ulimit nofile=65536:65536 -p 8000:8000 -e SURREAL_SURREALKV_MAX_VALUE_CACHE_SIZE={cache_gb}GB{sync_env} --user root",
				),
				false => format!("--ulimit nofile=65536:65536 -p 8000:8000{sync_env} --user root"),
			},
			post_args: format!(
				"start --user {username} --pass {password} surrealkv:/data/crud-bench.db",
			),
		},
	}
}

pub(crate) struct SurrealDB2ClientProvider {
	client: Option<Surreal<Any>>,
	endpoint: String,
	// v2's `Root<'a>` borrows the credentials, so we own them on the provider
	// and synthesise a fresh `Root<'_>` per connection.
	username: String,
	password: String,
}

pub(super) async fn initialise_db(
	endpoint: &str,
	username: &str,
	password: &str,
) -> Result<Surreal<Any>> {
	let root = Root {
		username,
		password,
	};
	let config = Config::new().user(root);
	let db = connect((endpoint, config)).await?;
	db.signin(root).await?;
	db.use_ns("test").use_db("test").await?;
	Ok(db)
}

impl BenchmarkEngine<SurrealDB2Client> for SurrealDB2ClientProvider {
	async fn setup(_: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		let mode = parse_endpoint(options.endpoint.as_deref())?;
		// v2's only fsync-on-commit switch is the `SURREAL_SYNC_DATA` env var
		// (no path query param, no CLI flag). For embedded mode we set it in
		// the bench process before `initialise_db` touches the kvs layer so
		// the LazyLock in `cnf::SYNC_DATA` resolves to the right value. For
		// Docker mode the equivalent `-e SURREAL_SYNC_DATA=true` is wired in
		// `docker()`. Remote mode honours whatever the remote server was
		// started with.
		if options.sync && matches!(mode, Endpoint::Embedded(_)) {
			// SAFETY: `setup` runs once at startup, before any benchmark
			// worker tasks spawn, and before the v2 SDK reads SYNC_DATA. No
			// concurrent env access is possible at this point.
			unsafe {
				env::set_var("SURREAL_SYNC_DATA", "true");
			}
		}
		if options.persisted {
			warn!(
				"--persisted has no effect on SurrealDB 2.x targets (v2 has no `mem://?…&aol=sync` support); memory backends are non-persistent"
			);
		}
		let username = surrealdb_username();
		let password = surrealdb_password();
		let (endpoint, client) = match mode {
			Endpoint::Docker(_) => (DEFAULT.to_string(), None),
			Endpoint::Remote(url) => (url, None),
			Endpoint::Embedded(url) => {
				let db = initialise_db(&url, &username, &password).await?;
				(url, Some(db))
			}
		};
		Ok(Self {
			endpoint,
			username,
			password,
			client,
		})
	}

	async fn create_client(&self) -> Result<SurrealDB2Client> {
		let client = match &self.client {
			Some(client) => client.clone(),
			None => initialise_db(&self.endpoint, &self.username, &self.password).await?,
		};
		Ok(SurrealDB2Client::new(client))
	}
}

pub(crate) struct SurrealDB2Client {
	db: Surreal<Any>,
}

impl SurrealDB2Client {
	pub(super) const fn new(db: Surreal<Any>) -> Self {
		Self {
			db,
		}
	}
}

/// Plain serde wrapper so a query like `CREATE $id CONTENT $content` can bind
/// both placeholders in one call. v2's `.bind(...)` takes any `Serialize`,
/// and a struct serialised as an object becomes a name→value binding map.
#[derive(Serialize)]
struct Bindings {
	id: Value,
	content: Value,
}

impl BenchmarkClient for SurrealDB2Client {
	type ReadRow = Row;

	async fn startup(&self) -> Result<()> {
		// Pre-create the table so concurrent first-writes don't all race to
		// stand up NS+DB+TB and trip "resource busy" — same rationale as the
		// v3 adapter.
		let sql = "
			REMOVE TABLE IF EXISTS record;
			DEFINE TABLE record;
		";
		self.db.query(sql).await.map_err(log_sql_err(sql))?.check().map_err(log_sql_err(sql))?;
		Ok(())
	}

	async fn compact(&self) -> Result<()> {
		// `ALTER SYSTEM COMPACT` is v3-only; on v2 there's no equivalent
		// statement so compaction is a no-op.
		Ok(())
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
		let unique = if spec.unique.unwrap_or(false) {
			"UNIQUE"
		} else {
			""
		}
		.to_string();
		let fields = spec.fields.join(", ");
		let sql = match &spec.index_type {
			Some(kind) if kind == "fulltext" => {
				let sql = format!(
					"DEFINE ANALYZER IF NOT EXISTS {name} TOKENIZERS blank,class FILTERS lowercase,ascii;"
				);
				self.db
					.query(&sql)
					.await
					.map_err(log_sql_err(&sql))?
					.check()
					.map_err(log_sql_err(&sql))?;
				// v2 spells the full-text index `SEARCH ANALYZER … BM25`;
				// the `FULLTEXT ANALYZER` keyword is v3-only. The `@@` match
				// operator used on the scan side is the same in both.
				format!(
					"DEFINE INDEX {name} ON TABLE record FIELDS {fields} SEARCH ANALYZER {name} BM25 CONCURRENTLY"
				)
			}
			_ => {
				format!("DEFINE INDEX {name} ON TABLE record FIELDS {fields} {unique} CONCURRENTLY")
			}
		};
		self.db.query(&sql).await.map_err(log_sql_err(&sql))?.check().map_err(log_sql_err(&sql))?;
		// Poll until the index reports ready. v2's `INFO FOR INDEX` returns
		// `{ building: { status } }` — same shape as v3 — but the SDK has no
		// `Value::get()` helper, so we hop through `into_json()` and walk the
		// JSON instead.
		loop {
			let sql = format!("INFO FOR INDEX {name} ON record");
			let r: surrealdb::Value = self
				.db
				.query(&sql)
				.await
				.map_err(log_sql_err(&sql))?
				.take(0)
				.map_err(log_sql_err(&sql))?;
			let j: JsonValue = r.into_inner().into_json();
			let status = j
				.get("building")
				.and_then(|b| b.get("status"))
				.and_then(|s| s.as_str())
				.unwrap_or("");
			match status {
				"ready" => break,
				"indexing" | "cleaning" | "started" => {}
				other => bail!("Unexpected index status `{other}`: {j}"),
			}
			sleep(Duration::from_millis(500)).await;
		}
		Ok(())
	}

	async fn drop_index(&self, name: &str) -> Result<()> {
		// Same retry/snapshot-cleanup rationale as the v3 adapter — concurrent
		// scan snapshots can briefly block metadata writes, and `IF EXISTS`
		// makes the retry idempotent.
		let retry = |sql: String, max_wait: Duration| async move {
			let fut = async {
				loop {
					match self.db.query(&sql).await?.check() {
						Ok(_) => return Ok(()),
						Err(e) => {
							if is_retryable_conflict(&e) {
								warn!("Retrying {sql} due to {e}");
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
				Err(_) => bail!("Timed out after {:?} waiting to execute: {}", max_wait, sql),
			}
		};
		let sql = format!("REMOVE INDEX IF EXISTS {name} ON TABLE record");
		retry(sql, Duration::from_secs(300)).await?;
		let sql = format!("REMOVE ANALYZER IF EXISTS {name}");
		retry(sql, Duration::from_secs(180)).await?;
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
			VectorIndexStrategy::Bruteforce => bail!(NOT_SUPPORTED_ERROR),
			VectorIndexStrategy::Hnsw {
				m,
				ef_construction,
				..
			} => format!(
				"DEFINE INDEX {name} ON TABLE record FIELDS {fields} HNSW DIMENSION {dim} DIST {dist} EFC {ef_construction} M {m} CONCURRENTLY"
			),
			VectorIndexStrategy::DiskAnn {
				degree,
				l_build,
				alpha,
				..
			} => format!(
				"DEFINE INDEX {name} ON TABLE record FIELDS {fields} DISKANN DIMENSION {dim} DISTANCE {dist} DEGREE {degree} L_BUILD {l_build} ALPHA {alpha} CONCURRENTLY"
			),
		};
		// DiskANN DDL doesn't exist in v2; the parser rejects it and we map
		// that to a clean NotSupported skip rather than a hard fail.
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
		loop {
			let q = format!("INFO FOR INDEX {name} ON record");
			let r: surrealdb::Value = self
				.db
				.query(&q)
				.await
				.map_err(log_sql_err(&q))?
				.take(0)
				.map_err(log_sql_err(&q))?;
			let j: JsonValue = r.into_inner().into_json();
			let status = j
				.get("building")
				.and_then(|b| b.get("status"))
				.and_then(|s| s.as_str())
				.unwrap_or("");
			match status {
				"ready" => break,
				"indexing" | "cleaning" | "started" => {}
				other => bail!("Unexpected index status `{other}`: {j}"),
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

/// Build a v2 `Value::Thing` for record `record:<key>`. `Id: From<i64> | From<String>`
/// in v2 mirrors v3's `RecordIdKey`, so the same call shape works for both key types.
fn thing<K: Into<Id>>(key: K) -> Value {
	Value::Thing(Thing::from((TABLE, key.into())))
}

/// `surrealdb::RecordId` (api wrapper) — used at the `db.select(...)` boundary
/// where a `Resource` requires the wrapper rather than the raw `Thing`.
fn record_id<K: Into<surrealdb::RecordIdKey>>(key: K) -> surrealdb::RecordId {
	surrealdb::RecordId::from_table_key(TABLE, key)
}

impl SurrealDB2Client {
	async fn create<K>(&self, key: K, val: BenchValue) -> Result<()>
	where
		K: Into<Id>,
	{
		let sql = "CREATE $id CONTENT $content RETURN NULL";
		let content = bench_to_surreal_value(val);
		let id = thing(key);
		run_dml_with_retry(sql, || async {
			let res = self
				.db
				.query(sql)
				.bind(Bindings {
					id: id.clone(),
					content: content.clone(),
				})
				.await?
				.take::<surrealdb::Value>(0)?;
			assert!(!res.into_inner().is_none());
			Ok(())
		})
		.await
	}

	async fn read<K>(&self, key: K) -> Result<Row>
	where
		K: Into<surrealdb::RecordIdKey>,
	{
		let v: surrealdb::Value = self.db.select(Resource::from(record_id(key))).await?;
		let inner = v.into_inner();
		// `select(Resource::from(rid))` returns `Value::Array` of matching rows
		// even for a single record (see the comment on `From<Row> for BenchValue`),
		// so a miss is `Value::Array(vec![])`, not `Value::None`. Catch both.
		let empty = match &inner {
			Value::None | Value::Null => true,
			Value::Array(a) => a.0.is_empty(),
			_ => false,
		};
		assert!(!empty, "read: no record found for key");
		Ok(black_box(Row(inner)))
	}

	async fn update<K>(&self, key: K, val: BenchValue) -> Result<()>
	where
		K: Into<Id>,
	{
		let sql = "UPDATE $id CONTENT $content RETURN NULL";
		// `UPDATE $id CONTENT $content` rejects content carrying an explicit
		// id field — same constraint as v3, same fix: strip it before binding.
		let mut content = bench_to_surreal_value(val);
		if let Value::Object(ref mut obj) = content {
			obj.0.remove("id");
		}
		let id = thing(key);
		run_dml_with_retry(sql, || async {
			let res = self
				.db
				.query(sql)
				.bind(Bindings {
					id: id.clone(),
					content: content.clone(),
				})
				.await?
				.take::<surrealdb::Value>(0)?;
			assert!(!res.into_inner().is_none());
			Ok(())
		})
		.await
	}

	async fn delete<K>(&self, key: K) -> Result<()>
	where
		K: Into<Id>,
	{
		let sql = "DELETE $id RETURN NULL";
		let id = thing(key);
		run_dml_with_retry(sql, || async {
			let res =
				self.db.query(sql).bind(("id", id.clone())).await?.take::<surrealdb::Value>(0)?;
			assert!(!res.into_inner().is_none());
			Ok(())
		})
		.await
	}

	async fn knn_scan(&self, scan: &Scan, query: &[f32]) -> Result<usize> {
		let vq = scan.vector_query.as_ref().ok_or_else(|| {
			anyhow::anyhow!("knn_scan called without a vector_query on scan `{}`", scan.name)
		})?;
		let field = &vq.field;
		let k = vq.top_k;
		let sql = match vq.index_strategy {
			VectorIndexStrategy::Bruteforce => {
				let func_path = surreal_distance_function(vq.distance);
				let dir = surreal_distance_order(vq.distance);
				format!(
					"SELECT id, {func_path}({field}, $q) AS _d FROM record ORDER BY _d {dir} LIMIT {k}"
				)
			}
			VectorIndexStrategy::Hnsw {
				ef_search,
				..
			} => format!("SELECT id FROM record WHERE {field} <|{k},{ef_search}|> $q"),
			VectorIndexStrategy::DiskAnn {
				l_search,
				..
			} => format!("SELECT id FROM record WHERE {field} <|{k},{l_search}|> $q"),
		};
		let q_value = Value::Array(Array::from(
			query.iter().map(|f| Value::Number(Number::Float(*f as f64))).collect::<Vec<_>>(),
		));
		let mut resp = self.db.query(&sql).bind(("q", q_value)).await.map_err(log_sql_err(&sql))?;
		let res: surrealdb::Value = resp.take(0).map_err(log_sql_err(&sql))?;
		match res.into_inner() {
			Value::Array(a) => Ok(a.0.len()),
			other => bail!("knn scan: unexpected response shape: {}", other),
		}
	}

	async fn scan(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		if ctx == ScanContext::WithoutIndex
			&& let Some(index) = &scan.with_index
			&& let Some(kind) = &index.index_type
			&& kind == "fulltext"
		{
			bail!(NOT_SUPPORTED_ERROR);
		}
		let s = scan.start.map(|s| format!("START {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = SurrealDBDialect::filter_clause(scan)?;
		let o = SurrealDBDialect::order_by_clause(scan)?;
		let p = scan.projection()?;
		match p {
			Projection::Id => {
				let sql = format!("SELECT id FROM record {c} {o} {s} {l}");
				let res: surrealdb::Value = self
					.db
					.query(&sql)
					.await
					.map_err(log_sql_err(&sql))?
					.take(0)
					.map_err(log_sql_err(&sql))?;
				match res.into_inner() {
					Value::Array(a) => Ok(a.0.len()),
					_ => panic!("Unexpected response type"),
				}
			}
			Projection::Full => {
				let sql = format!("SELECT * FROM record {c} {o} {s} {l}");
				let res: surrealdb::Value = self
					.db
					.query(&sql)
					.await
					.map_err(log_sql_err(&sql))?
					.take(0)
					.map_err(log_sql_err(&sql))?;
				match res.into_inner() {
					Value::Array(a) => Ok(a.0.len()),
					_ => panic!("Unexpected response type"),
				}
			}
			Projection::Count => {
				let sql = if s.is_empty() && l.is_empty() {
					format!("SELECT count() FROM record {c} GROUP ALL")
				} else {
					format!("SELECT count() FROM (SELECT 1 FROM record {c} {s} {l}) GROUP ALL")
				};
				// `GROUP ALL` yields `[{ count: N }]`, or an empty array when
				// nothing matches. Read the raw value and pull `count` out of
				// the first row rather than relying on the typed `take("count")`
				// path, which doesn't surface the field reliably on v2.
				let res: surrealdb::Value = self
					.db
					.query(&sql)
					.await
					.map_err(log_sql_err(&sql))?
					.take(0)
					.map_err(log_sql_err(&sql))?;
				let j: JsonValue = res.into_inner().into_json();
				let arr = j
					.as_array()
					.ok_or_else(|| anyhow::anyhow!("count scan: expected array, got {j}"))?;
				// GROUP ALL on zero matches returns []; treat that as count=0
				// rather than an error.
				let Some(row) = arr.first() else {
					return Ok(0);
				};
				let count = row.get("count").and_then(|c| c.as_u64()).ok_or_else(|| {
					anyhow::anyhow!("count scan: missing/non-numeric `count` field in {row}")
				})?;
				Ok(count as usize)
			}
		}
	}

	async fn batch_create<K>(
		&self,
		key_vals: impl Iterator<Item = (K, BenchValue)> + Send,
	) -> Result<()>
	where
		K: Into<Id>,
	{
		let rows: Vec<Value> = key_vals
			.map(|(k, v)| {
				let mut obj = match bench_to_surreal_value(v) {
					Value::Object(o) => o,
					_ => panic!("Unexpected value type"),
				};
				obj.0.insert("id".to_string(), thing(k));
				Value::Object(obj)
			})
			.collect();
		let sql = "INSERT $rows RETURN NONE";
		let rows = Value::Array(Array::from(rows));
		run_dml_with_retry(sql, || async {
			self.db.query(sql).bind(("rows", rows.clone())).await?.check()?;
			Ok(())
		})
		.await
	}

	async fn batch_read<K>(&self, keys: impl Iterator<Item = K> + Send) -> Result<()>
	where
		K: Into<Id>,
	{
		let ids: Vec<Value> = keys.map(thing).collect();
		let ids_len = ids.len();
		let sql = "SELECT * FROM $ids";
		let res: surrealdb::Value = self
			.db
			.query(sql)
			.bind(("ids", Value::Array(Array::from(ids))))
			.await
			.map_err(log_sql_err(sql))?
			.take(0)
			.map_err(log_sql_err(sql))?;
		match res.into_inner() {
			Value::Array(a) => assert_eq!(a.0.len(), ids_len),
			_ => panic!("Unexpected response type"),
		}
		Ok(())
	}

	async fn batch_update<K>(
		&self,
		key_vals: impl Iterator<Item = (K, BenchValue)> + Send,
	) -> Result<()>
	where
		K: Into<Id>,
	{
		let rows: Vec<Value> = key_vals
			.map(|(k, v)| {
				let mut obj = match bench_to_surreal_value(v) {
					Value::Object(o) => o,
					_ => panic!("Unexpected value type"),
				};
				obj.0.insert("id".to_string(), thing(k));
				Value::Object(obj)
			})
			.collect();
		let sql = "FOR $row IN $rows { UPDATE $row.id CONTENT $row RETURN NONE }";
		let rows = Value::Array(Array::from(rows));
		run_dml_with_retry(sql, || async {
			self.db.query(sql).bind(("rows", rows.clone())).await?.check()?;
			Ok(())
		})
		.await
	}

	async fn batch_delete<K>(&self, keys: impl Iterator<Item = K> + Send) -> Result<()>
	where
		K: Into<Id>,
	{
		let ids: Vec<Value> = keys.map(thing).collect();
		let sql = "DELETE $ids";
		let ids = Value::Array(Array::from(ids));
		run_dml_with_retry(sql, || async {
			self.db.query(sql).bind(("ids", ids.clone())).await?.check()?;
			Ok(())
		})
		.await
	}
}
