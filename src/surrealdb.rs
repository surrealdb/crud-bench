#![cfg(feature = "surrealdb")]

use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;
use surrealdb::engine::any::{connect, Any};
use surrealdb::opt::auth::Root;
use surrealdb::opt::Config;
use surrealdb::RecordId;
use surrealdb::Surreal;

pub(crate) const SURREALDB_MEMORY_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "surrealdb/surrealdb:nightly",
	pre_args: "-p 8000:8000",
	post_args: "start --user root --pass root memory",
};

pub(crate) const SURREALDB_ROCKSDB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "surrealdb/surrealdb:nightly",
	pre_args: "-p 8000:8000",
	post_args: "start --user root --pass root rocksdb://tmp/crud-bench.db",
};

pub(crate) const SURREALDB_SURREALKV_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "surrealdb/surrealdb:nightly",
	pre_args: "-p 8000:8000",
	post_args: "start --user root --pass root surrealkv://tmp/crud-bench.db",
};

pub(crate) struct SurrealDBClientProvider {
	client: Option<Surreal<Any>>,
	endpoint: String,
	root: Root<'static>,
}

async fn initialise_db(endpoint: &str, root: Root<'static>) -> Result<Surreal<Any>> {
	// Set the root user
	let config = Config::new().user(root);
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
	async fn setup(_: KeyType, _columns: Columns, endpoint: Option<&str>) -> Result<Self> {
		// Get the endpoint if specified
		let endpoint = endpoint.unwrap_or("ws://127.0.0.1:8000").replace("memory", "mem://");
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
		let res: Option<SurrealRecord> =
			self.db.create(("record", key as i64)).content(val).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		let res: Option<SurrealRecord> = self.db.create(("record", key)).content(val).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let res: Option<SurrealRecord> = self.db.select(("record", key as i64)).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let res: Option<SurrealRecord> = self.db.select(("record", key)).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		let _: Option<SurrealRecord> = self.db.update(("record", key as i64)).content(val).await?;
		Ok(())
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let res: Option<SurrealRecord> = self.db.update(("record", key)).content(val).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let res: Option<SurrealRecord> = self.db.delete(("record", key as i64)).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		let res: Option<SurrealRecord> = self.db.delete(("record", key)).await?;
		assert!(res.is_some());
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
		let c = scan.condition.as_ref().map(|s| format!("WHERE {s}")).unwrap_or_default();
		// Perform the relevant projection scan type
		match scan.projection()? {
			Projection::Id => {
				let sql = format!("SELECT id FROM record {c} {s} {l}");
				let res: Vec<SurrealRecord> = self.db.query(sql).await?.take(0)?;
				Ok(res.len())
			}
			Projection::Full => {
				let sql = format!("SELECT * FROM record {c} {s} {l}");
				let res: Vec<SurrealRecord> = self.db.query(sql).await?.take(0)?;
				Ok(res.len())
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
