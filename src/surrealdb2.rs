#![cfg(feature = "surrealdb2")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::database::Database;
use crate::dialect::SurrealDB2Dialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config as MemoryConfig;
use crate::valueprovider::Columns;
use crate::{Benchmark, Index, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use log::warn;
use serde_json::Value;
use std::env;
use std::time::Duration;
use surrealdb2::Surreal;
use surrealdb2::engine::any::{Any, connect};
use surrealdb2::opt::Config;
use surrealdb2::opt::auth::Root;
use surrealdb2::sql::Value as CoreValue;
use tokio::time::{sleep, timeout};

const DEFAULT: &str = "ws://127.0.0.1:8000";

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
	// Calculate memory allocation
	let cache_gb = calculate_surrealdb_memory();
	// Get credentials from environment variables or use defaults
	let username = surrealdb_username();
	let password = surrealdb_password();
	// Return Docker parameters
	match options.database {
		Database::Surrealdb2Memory => DockerParams {
			image: "surrealdb/surrealdb:v2",
			pre_args: "--ulimit nofile=65536:65536 -p 8000:8000 --user root".to_string(),
			post_args: format!("start --user {username} --pass {password} memory"),
		},
		Database::Surrealdb2Rocksdb => DockerParams {
			image: "surrealdb/surrealdb:v2",
			pre_args: match options.optimised {
				true => format!(
					"--ulimit nofile=65536:65536 -p 8000:8000 -e SURREAL_SYNC_DATA={} -e SURREAL_ROCKSDB_BLOCK_CACHE_SIZE={cache_gb}GB --user root",
					if options.sync {
						"true"
					} else {
						"false"
					}
				),
				false => format!(
					"--ulimit nofile=65536:65536 -p 8000:8000 -e SURREAL_SYNC_DATA={} --user root",
					if options.sync {
						"true"
					} else {
						"false"
					}
				),
			},
			post_args: format!(
				"start --user {username} --pass {password} rocksdb:/data/crud-bench.db"
			),
		},
		Database::Surrealdb2Surrealkv => DockerParams {
			image: "surrealdb/surrealdb:v2",
			pre_args: match options.optimised {
				true => format!(
					"--ulimit nofile=65536:65536 -p 8000:8000 -e SURREAL_SYNC_DATA={} -e SURREAL_SURREALKV_MAX_VALUE_CACHE_SIZE={cache_gb}GB --user root",
					if options.sync {
						"true"
					} else {
						"false"
					}
				),
				false => format!(
					"--ulimit nofile=65536:65536 -p 8000:8000 -e SURREAL_SYNC_DATA={} --user root",
					if options.sync {
						"true"
					} else {
						"false"
					}
				),
			},
			post_args: format!(
				"start --user {username} --pass {password} surrealkv:/data/crud-bench.db"
			),
		},
		_ => unreachable!(),
	}
}

pub(crate) struct SurrealDB2ClientProvider {
	client: Option<Surreal<Any>>,
	endpoint: String,
	username: String,
	password: String,
}

/// Initialise a SurrealDB v2 connection
pub(super) async fn initialise_db(
	endpoint: &str,
	username: &str,
	password: &str,
) -> Result<Surreal<Any>> {
	// Set the root user for embedded engines
	let root = Root {
		username,
		password,
	};
	let config = Config::new().user(root);
	// Connect to the database
	let db = connect((endpoint, config)).await?;
	// Signin as a namespace, database, or root user
	db.signin(Root {
		username,
		password,
	})
	.await?;
	// Select a specific namespace / database
	db.use_ns("test").use_db("test").await?;
	// Return the client
	Ok(db)
}

impl BenchmarkEngine<SurrealDB2Client> for SurrealDB2ClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Get the custom endpoint if specified
		let endpoint = options.endpoint.as_deref().unwrap_or(DEFAULT).replace("memory", "mem://");
		// Define root user details from environment variables or use defaults
		let username = surrealdb_username();
		let password = surrealdb_password();
		// Setup the optional client
		let client = match endpoint.split_once(':').unwrap().0 {
			// We want to be able to create multiple connections
			// when testing against an external server
			"ws" | "wss" | "http" | "https" => None,
			// When the database is embedded, create only
			// one client to avoid sending queries to the
			// wrong database
			_ => Some(initialise_db(&endpoint, &username, &password).await?),
		};
		Ok(Self {
			endpoint,
			username,
			password,
			client,
		})
	}
	/// Creates a new client for this benchmarking engine
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
	pub(super) fn new(db: Surreal<Any>) -> Self {
		Self {
			db,
		}
	}
}

impl BenchmarkClient for SurrealDB2Client {
	async fn startup(&self) -> Result<()> {
		// Ensure the table exists. This wouldn't
		// normally be an issue, as SurrealDB is
		// schemaless. However, because we are testing
		// benchmarking of concurrent, optimistic
		// transactions, each initial concurrent
		// insert/create into the table attempts
		// to set up the NS+DB+TB, and this causes
		// 'resource busy' key conflict failures.
		let surql = "
            REMOVE TABLE IF EXISTS record;
			DEFINE TABLE record;
		";
		self.db.query(surql).await?.check()?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read(key).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read(key).await
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		self.update(key, val).await
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		self.update(key, val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key).await
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
		let sql = match spec.index_type.as_deref() {
			Some("fulltext") => {
				// Define the analyzer
				let sql = format!(
					"DEFINE ANALYZER IF NOT EXISTS {name} TOKENIZERS blank,class FILTERS lowercase,ascii;"
				);
				self.db.query(sql).await?.check()?;
				// Define the index concurrently
				format!(
					"DEFINE INDEX {name} ON TABLE record FIELDS {fields} SEARCH ANALYZER {name} BM25 CONCURRENTLY"
				)
			}
			Some("hnsw") => {
				// HNSW may not be available in SurrealDB v2; will fail gracefully
				format!(
					"DEFINE INDEX {name} ON TABLE record FIELDS {fields} HNSW DIMENSION 4 DIST EUCLIDEAN CONCURRENTLY"
				)
			}
			_ => {
				format!("DEFINE INDEX {name} ON TABLE record FIELDS {fields} {unique} CONCURRENTLY")
			}
		};
		// Create the index
		self.db.query(sql).await?.check()?;
		// Wait until the index is ready
		loop {
			let sql = format!("INFO FOR INDEX {name} ON record");
			let r: Option<Value> = self.db.query(sql).await?.take(0)?;
			let r = r.unwrap_or(Value::Null);
			let status = r
				.get("building")
				.and_then(|b| b.get("status"))
				.and_then(|s| s.as_str())
				.unwrap_or("unknown");
			match status {
				"ready" => break,
				"indexing" | "cleaning" | "started" => {}
				_ => bail!("Unexpected status: {}", r),
			}
			sleep(Duration::from_millis(500)).await;
		}
		// All ok
		Ok(())
	}

	async fn drop_index(&self, name: &str) -> Result<()> {
		// Retry helper closure for handling transient "Resource busy" errors.
		//
		// After intensive concurrent scan operations, each scan creates a READ
		// transaction in SurrealDB that holds a RocksDB snapshot. When REMOVE INDEX
		// executes immediately after scans complete, it may conflict with snapshots
		// still being released. Retrying is safe since REMOVE INDEX IF EXISTS is
		// idempotent.
		let retry = |sql: String, max_wait: Duration| async move {
			let fut = async {
				loop {
					match self.db.query(&sql).await?.check() {
						Ok(_) => return Ok(()),
						Err(e) => {
							let msg = e.to_string();
							// Be permissive on the match to tolerate tiny wording changes
							if msg.starts_with(
								"The query was not executed due to a failed transaction.",
							) {
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
		retry(sql, Duration::from_secs(120)).await?;
		// Remove the analyzer
		let sql = format!("REMOVE ANALYZER IF EXISTS {name}");
		retry(sql, Duration::from_secs(60)).await?;
		// All ok
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		self.scan(scan, ctx).await
	}

	async fn scan_string(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		self.scan(scan, ctx).await
	}

	async fn run_setup_queries(&self, queries: &[String]) -> Result<()> {
		for q in queries {
			self.db.query(q.as_str()).await?.check()?;
		}
		Ok(())
	}
}

/// Extract the length of a SurrealDB v2 Value result.
///
/// Uses the SDK's own Value type to bypass serde deserialization, which
/// fails on Thing (record ID) types with "invalid type: enum" errors.
fn core_value_len(val: surrealdb2::Value) -> usize {
	match val.into_inner() {
		CoreValue::Array(arr) => arr.len(),
		CoreValue::None => 0,
		_ => 1,
	}
}

impl SurrealDB2Client {
	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: serde::Serialize + Send + 'static,
	{
		self.db
			.query("CREATE type::thing('record', $key) CONTENT $content RETURN NULL")
			.bind(("key", key))
			.bind(("content", val))
			.await?
			.check()?;
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<()>
	where
		T: serde::Serialize + Send + 'static,
	{
		// Use surrealdb2::Value to avoid serde deserialization of Thing types
		let res: surrealdb2::Value = self
			.db
			.query("SELECT * FROM type::thing('record', $key)")
			.bind(("key", key))
			.await?
			.take(0)?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: serde::Serialize + Send + 'static,
	{
		self.db
			.query("UPDATE type::thing('record', $key) CONTENT $content RETURN NULL")
			.bind(("key", key))
			.bind(("content", val))
			.await?
			.check()?;
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: serde::Serialize + Send + 'static,
	{
		self.db
			.query("DELETE type::thing('record', $key) RETURN NULL")
			.bind(("key", key))
			.await?
			.check()?;
		Ok(())
	}

	async fn scan(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		// Skip index-dependent queries when running without an index
		if ctx == ScanContext::WithoutIndex {
			if let Some(index) = &scan.index {
				match index.index_type.as_deref() {
					Some("fulltext") | Some("mtree") | Some("hnsw") => {
						bail!(NOT_SUPPORTED_ERROR);
					}
					_ => {}
				}
			}
		}
		// Check for a raw query (with v2 fallback)
		if let Some(sql) = scan.raw_query("surrealdb2") {
			let res: surrealdb2::Value = self.db.query(sql).await?.take(0)?;
			return Ok(core_value_len(res).max(1)); // At least 1 for single-result queries
		}
		// Extract parameters
		let s = scan.start.map(|s| format!("START {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = SurrealDB2Dialect::filter_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let sql = format!("SELECT id FROM record {c} {s} {l}");
				let res: surrealdb2::Value = self.db.query(sql).await?.take(0)?;
				Ok(core_value_len(res))
			}
			Projection::Full => {
				let sql = format!("SELECT * FROM record {c} {s} {l}");
				let res: surrealdb2::Value = self.db.query(sql).await?.take(0)?;
				Ok(core_value_len(res))
			}
			Projection::Count => {
				let sql = if s.is_empty() && l.is_empty() {
					format!("SELECT count() FROM record {c} GROUP ALL")
				} else {
					format!("SELECT count() FROM (SELECT 1 FROM record {c} {s} {l}) GROUP ALL")
				};
				let res: Option<usize> = self.db.query(sql).await?.take("count")?;
				Ok(res.unwrap())
			}
		}
	}
}
