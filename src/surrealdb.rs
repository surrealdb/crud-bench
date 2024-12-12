#![cfg(feature = "surrealdb")]

use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;
use surrealdb::engine::any::{connect, Any};
use surrealdb::opt::auth::Root;
use surrealdb::sql::Thing;
use surrealdb::Surreal;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::docker::DockerParams;
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};

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

#[derive(Default)]
pub(crate) struct SurrealDBClientProvider {}

impl BenchmarkEngine<SurrealDBClient> for SurrealDBClientProvider {
	async fn setup(_: KeyType, _columns: Columns) -> Result<Self> {
		Ok(Self {})
	}
	async fn create_client(&self, endpoint: Option<String>) -> Result<SurrealDBClient> {
		// Get the endpoint if specified
		let ep = endpoint.unwrap_or("ws://127.0.0.1:8000".to_owned());
		// Connect to the database
		let db = connect(ep).await?;
		// Signin as a namespace, database, or root user
		db.signin(Root {
			username: "root",
			password: "root",
		})
		.await?;
		// Select a specific namespace / database
		db.use_ns("test").use_db("test").await?;
		// Return the client
		Ok(SurrealDBClient {
			db,
		})
	}
}

pub(crate) struct SurrealDBClient {
	db: Surreal<Any>,
}

#[derive(Debug, Deserialize)]
struct SurrealRecord {
	#[allow(dead_code)]
	id: Thing,
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
		self.db.query("REMOVE TABLE IF EXISTS record").await?;
		self.db.query("DEFINE TABLE record").await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		let created: Option<SurrealRecord> =
			self.db.create(("record", key as i64)).content(val).await?;
		assert!(created.is_some());
		Ok(())
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		let created: Option<SurrealRecord> = self.db.create(("record", key)).content(val).await?;
		assert!(created.is_some());
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let read: Option<SurrealRecord> = self.db.select(("record", key as i64)).await?;
		assert!(read.is_some());
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let read: Option<SurrealRecord> = self.db.select(("record", key)).await?;
		assert!(read.is_some());
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		let s = scan.start.map(|s| format!("START {}", s)).unwrap_or("".to_string());
		let l = scan.limit.map(|s| format!("LIMIT {}", s)).unwrap_or("".to_string());
		let c = scan.condition.as_ref().map(|s| format!("WHERE {}", s)).unwrap_or("".to_string());
		let p = scan.projection()?;
		let stm = match p {
			Projection::Id => format!("SELECT id FROM record {c} {s} {l}"),
			Projection::Full => format!("SELECT * FROM record {c} {s} {l}"),
			Projection::Count => format!("SELECT COUNT() FROM record {c} {s} {l}"),
		};
		match p {
			Projection::Id | Projection::Full => {
				let res: Vec<SurrealRecord> = self.db.query(stm).await?.take(0)?;
				Ok(res.len())
			}
			Projection::Count => {
				let count: Vec<usize> = self.db.query(stm).await?.take(0)?;
				Ok(count[0])
			}
		}
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan_u32(scan).await
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		let updated: Option<SurrealRecord> =
			self.db.update(("record", key as i64)).content(val).await?;
		assert!(updated.is_some());
		Ok(())
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let updated: Option<SurrealRecord> = self.db.update(("record", key)).content(val).await?;
		assert!(updated.is_some());
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let deleted: Option<SurrealRecord> = self.db.delete(("record", key as i64)).await?;
		assert!(deleted.is_some());
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		let deleted: Option<SurrealRecord> = self.db.delete(("record", key)).await?;
		assert!(deleted.is_some());
		Ok(())
	}
}
