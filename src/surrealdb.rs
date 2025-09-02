#![cfg(feature = "surrealdb")]

use crate::database::Database;
use crate::dialect::SurrealDBDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::memory::Config as MemoryConfig;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use surrealdb::RecordId;
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
use surrealdb::opt::{Config, Raw, Resource};

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
	root: Root<'static>,
}

async fn initialise_db(endpoint: &str, root: Root<'static>) -> Result<Surreal<Any>> {
	// Set the root user
	let config = Config::new().user(root).ast_payload();
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
			username: "root",
			password: "root",
		};
		// Setup the optional client
		let client = match endpoint.split_once(':').unwrap().0 {
			// We want to be able to create multiple connections
			// when testing against an external server
			"ws" | "wss" | "http" | "https" => None,
			// When the database is embedded, create only
			// one client to avoid sending queries to the
			// wrong database
			_ => Some(initialise_db(&endpoint, root).await?),
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
			None => initialise_db(&self.endpoint, self.root).await?,
		};
		Ok(SurrealDBClient::new(client))
	}
}

pub(crate) struct SurrealDBClient {
	db: Surreal<Any>,
}

impl SurrealDBClient {
	const fn new(db: Surreal<Any>) -> Self {
		Self {
			db,
		}
	}
}

#[derive(Debug, Deserialize)]
struct SurrealRecord {
	#[allow(dead_code)]
	id: RecordId,
}

#[derive(Debug, Serialize)]
struct Bindings<T> {
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
		self.db.query(Raw::from(surql)).await?.check()?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		let res = self
			.db
			.query(Raw::from("CREATE type::thing('record', $key) CONTENT $content RETURN NULL"))
			.bind(Bindings {
				key,
				content: val,
			})
			.await?
			.take::<surrealdb::Value>(0)?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		let res = self
			.db
			.query(Raw::from("CREATE type::thing('record', $key) CONTENT $content RETURN NULL"))
			.bind(Bindings {
				key,
				content: val,
			})
			.await?
			.take::<surrealdb::Value>(0)?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let res = self.db.select(Resource::from(("record", key as i64))).await?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let res = self.db.select(Resource::from(("record", key))).await?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		let res = self
			.db
			.query(Raw::from("UPDATE type::thing('record', $key) CONTENT $content RETURN NULL"))
			.bind(Bindings {
				key,
				content: val,
			})
			.await?
			.take::<surrealdb::Value>(0)?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let res = self
			.db
			.query(Raw::from("UPDATE type::thing('record', $key) CONTENT $content RETURN NULL"))
			.bind(Bindings {
				key,
				content: val,
			})
			.await?
			.take::<surrealdb::Value>(0)?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let res = self
			.db
			.query(Raw::from("DELETE type::thing('record', $key) RETURN NULL"))
			.bind(("key", key))
			.await?
			.take::<surrealdb::Value>(0)?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		let res = self
			.db
			.query(Raw::from("DELETE type::thing('record', $key) RETURN NULL"))
			.bind(("key", key))
			.await?
			.take::<surrealdb::Value>(0)?;
		assert!(!res.into_inner().is_none());
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}
}

impl SurrealDBClient {
	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.map(|s| format!("START {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = SurrealDBDialect::filter_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let sql = format!("SELECT id FROM record {c} {s} {l}");
				let res: surrealdb::Value = self.db.query(Raw::from(sql)).await?.take(0)?;
				let val = res.into_inner();
				let Some(arr) = val.as_array() else {
					panic!("Unexpected response type");
				};
				Ok(arr.len())
			}
			Projection::Full => {
				let sql = format!("SELECT * FROM record {c} {s} {l}");
				let res: surrealdb::Value = self.db.query(Raw::from(sql)).await?.take(0)?;
				let val = res.into_inner();
				let Some(arr) = val.as_array() else {
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
				let res: Option<usize> = self.db.query(Raw::from(sql)).await?.take("count")?;
				Ok(res.unwrap())
			}
		}
	}
}
