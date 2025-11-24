#![cfg(feature = "surrealdb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::database::Database;
use crate::dialect::SurrealDBDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config as MemoryConfig;
use crate::valueprovider::Columns;
use crate::{Benchmark, Index, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use log::warn;
use serde_json::Value;
use std::time::Duration;
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
use surrealdb::opt::{Config, Resource};
use surrealdb::types::RecordIdKey;
use surrealdb_types::{SurrealValue, ToSql};
use tokio::time::{sleep, timeout};

const DEFAULT: &str = "ws://127.0.0.1:8000";

/// Calculate SurrealDB RocksDB specific memory allocation
fn calculate_surrealdb_memory() -> u64 {
	// Load the system memory
	let memory = MemoryConfig::new();
	// Use ~80% of recommended cache allocation
	(memory.cache_gb * 4 / 6).max(1)
}

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	// Calculate memory allocation
	let cache_gb = calculate_surrealdb_memory();
	// Return Docker parameters
	match options.database {
		Database::SurrealdbMemory => DockerParams {
			image: "surrealdb/surrealdb:nightly",
			pre_args: "--ulimit nofile=65536:65536 -p 8000:8000 --user root".to_string(),
			post_args: "start --user root --pass root memory".to_string(),
		},
		Database::SurrealdbRocksdb => DockerParams {
			image: "surrealdb/surrealdb:nightly",
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
			post_args: "start --user root --pass root rocksdb:/data/crud-bench.db".to_string(),
		},
		Database::SurrealdbSurrealkv => DockerParams {
			image: "surrealdb/surrealdb:nightly",
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
			post_args: "start --user root --pass root surrealkv:/data/crud-bench.db".to_string(),
		},
		_ => unreachable!(),
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
		// Get the custom endpoint if specified
		let endpoint = options.endpoint.as_deref().unwrap_or(DEFAULT).replace("memory", "mem://");
		// Define root user details
		let root = Root {
			username: String::from("root"),
			password: String::from("root"),
		};
		// Setup the optional client
		let client = match endpoint.split_once(':').unwrap().0 {
			// We want to be able to create multiple connections
			// when testing against an external server
			"ws" | "wss" | "http" | "https" => None,
			// When the database is embedded, create only
			// one client to avoid sending queries to the
			// wrong database
			_ => Some(initialise_db(&endpoint, root.clone()).await?),
		};
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
struct Bindings<T: SurrealValue> {
	content: Value,
	key: T,
}

impl BenchmarkClient for SurrealDBClient {
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
		self.read(key as i64).await
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
		let sql = match &spec.index_type {
			Some(kind) if kind == "fulltext" => {
				// Define the analyzer
				let sql = format!(
					"DEFINE ANALYZER IF NOT EXISTS {name} TOKENIZERS blank,class FILTERS lowercase,ascii;"
				);
				self.db.query(sql).await?.check()?;
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
		self.db.query(sql).await?.check()?;
		// Wait until the index is ready
		loop {
			let sql = format!("INFO FOR INDEX {name} ON record");
			let r: surrealdb::types::Value = self.db.query(sql).await?.take(0)?;
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
}

impl SurrealDBClient {
	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: SurrealValue + 'static,
	{
		let res = self
			.db
			.query("CREATE type::record('record', $key) CONTENT $content RETURN NULL")
			.bind(Bindings {
				key,
				content: val,
			})
			.await?
			.take::<surrealdb::types::Value>(0)?;
		assert!(!res.is_none());
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<()>
	where
		T: Into<RecordIdKey>,
	{
		let res = self.db.select(Resource::from(("record", key))).await?;
		assert!(!res.is_none());
		Ok(())
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: SurrealValue + 'static,
	{
		let res = self
			.db
			.query("UPDATE type::record('record', $key) CONTENT $content RETURN NULL")
			.bind(Bindings {
				key,
				content: val,
			})
			.await?
			.take::<surrealdb::types::Value>(0)?;
		assert!(!res.is_none());
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: SurrealValue + 'static,
	{
		let res = self
			.db
			.query("DELETE type::record('record', $key) RETURN NULL")
			.bind(("key", key))
			.await?
			.take::<surrealdb::types::Value>(0)?;
		assert!(!res.is_none());
		Ok(())
	}

	async fn scan(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		// SurrealDB requires a full-text index to use the @@ operator
		if ctx == ScanContext::WithoutIndex
			&& let Some(index) = &scan.index
			&& let Some(kind) = &index.index_type
			&& kind == "fulltext"
		{
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.map(|s| format!("START {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = SurrealDBDialect::filter_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let sql = format!("SELECT id FROM record {c} {s} {l}");
				let res: surrealdb::types::Value = self.db.query(sql).await?.take(0)?;
				let Some(arr) = res.as_array() else {
					panic!("Unexpected response type");
				};
				Ok(arr.len())
			}
			Projection::Full => {
				let sql = format!("SELECT * FROM record {c} {s} {l}");
				let res: surrealdb::types::Value = self.db.query(sql).await?.take(0)?;
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
				let res: Option<usize> = self.db.query(sql).await?.take("count")?;
				Ok(res.unwrap())
			}
		}
	}
}
