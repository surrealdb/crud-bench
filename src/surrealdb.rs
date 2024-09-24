#![cfg(feature = "surrealdb")]

use anyhow::Result;
use serde::Deserialize;
use surrealdb::engine::any::{connect, Any};
use surrealdb::opt::auth::Root;
use surrealdb::sql::Thing;
use surrealdb::Surreal;

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::docker::DockerParams;

pub(crate) const SURREALDB_MEMORY_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "surrealdb/surrealdb:nightly",
	pre_args: "-p 127.0.0.1:8000:8000",
	post_args: "start --user root --pass root memory",
};

pub(crate) const SURREALDB_ROCKSDB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "surrealdb/surrealdb:nightly",
	pre_args: "-p 127.0.0.1:8000:8000",
	post_args: "start --user root --pass root rocksdb://tmp/crud-bench.db",
};

pub(crate) const SURREALDB_SURREALKV_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "surrealdb/surrealdb:nightly",
	pre_args: "-p 127.0.0.1:8000:8000",
	post_args: "start --user root --pass root surrealkv://tmp/crud-bench.db",
};

#[derive(Default)]
pub(crate) struct SurrealDBClientProvider {}

impl BenchmarkEngine<SurrealDBClient> for SurrealDBClientProvider {
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
	async fn startup(&mut self) -> Result<()> {
		// Ensure the table exists. This wouldn't
		// normally be an issue, as SurrealDB is
		// schemaless, but because we are testing
		// benchmarking of concurrent, optimistic
		// transactions, each initial concurrent
		// insert/create into the table attempts
		// to setup the NS+DB+TB, and this causes
		// 'resource busy' key conflict failures.
		self.db.query("REMOVE TABLE IF EXISTS record").await?;
		self.db.query("DEFINE TABLE record").await?;
		Ok(())
	}

	async fn read(&mut self, key: i32) -> Result<()> {
		let read: Option<Record> = self.db.select(("record", key as i64)).await?;
		assert!(read.is_some());
		Ok(())
	}

	async fn create(&mut self, key: i32, record: &Record) -> Result<()> {
		let created: Option<SurrealRecord> =
			self.db.create(("record", key as i64)).content(record.clone()).await?;
		assert!(created.is_some());
		Ok(())
	}

	async fn update(&mut self, key: i32, record: &Record) -> Result<()> {
		let updated: Option<SurrealRecord> =
			self.db.update(("record", key as i64)).content(record.clone()).await?;
		assert!(updated.is_some());
		Ok(())
	}

	async fn delete(&mut self, key: i32) -> Result<()> {
		let deleted: Option<Record> = self.db.delete(("record", key as i64)).await?;
		assert!(deleted.is_some());
		Ok(())
	}
}
