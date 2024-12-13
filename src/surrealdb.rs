#![cfg(feature = "surrealdb")]

use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use surrealdb::engine::any::{connect, Any};
use surrealdb::error::Api;
use surrealdb::opt::auth::Root;
use surrealdb::opt::Config;
use surrealdb::Surreal;
use surrealdb::{Error, RecordId};

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

pub(crate) struct SurrealDBClientProvider(Arc<Surreal<Any>>);

impl Default for SurrealDBClientProvider {
	fn default() -> Self {
		SurrealDBClientProvider(Arc::new(Surreal::init()))
	}
}

impl BenchmarkEngine<SurrealDBClient> for SurrealDBClientProvider {
	async fn setup(_: KeyType, _columns: Columns) -> Result<Self> {
		Ok(Default::default())
	}
	async fn create_client(&self, endpoint: Option<String>) -> Result<SurrealDBClient> {
		// Get the endpoint if specified
		let ep = endpoint.unwrap_or("ws://127.0.0.1:8000".to_owned()).replace("memory", "mem://");
		// Define root user details
		let root = Root {
			username: "root",
			password: "root",
		};
		let config = Config::new().user(root);
		// Return the client
		let client = match ep.split_once(':').unwrap().0 {
			"ws" | "wss" | "http" | "https" => {
				// Connect to the database and instantiate the remote client
				SurrealDBClient::Remote(connect((ep, config)).await?)
			}
			_ => {
				// Connect to the database
				match self.0.connect((ep, config)).await {
					Ok(..) | Err(Error::Api(Api::AlreadyConnected)) => {
						// We have connected successfully
					}
					Err(error) => return Err(error.into()),
				}
				SurrealDBClient::Local(self.0.clone())
			}
		};
		// Signin as a namespace, database, or root user
		client.db().signin(root).await?;
		// Select a specific namespace / database
		client.db().use_ns("test").use_db("test").await?;
		Ok(client)
	}
}

pub(crate) enum SurrealDBClient {
	Remote(Surreal<Any>),
	Local(Arc<Surreal<Any>>),
}

impl SurrealDBClient {
	fn db(&self) -> &Surreal<Any> {
		match self {
			SurrealDBClient::Remote(client) => client,
			SurrealDBClient::Local(client) => client,
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
		self.db().query("REMOVE TABLE IF EXISTS record").await?;
		self.db().query("DEFINE TABLE record").await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		let res: Option<SurrealRecord> =
			self.db().create(("record", key as i64)).content(val).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		let res: Option<SurrealRecord> = self.db().create(("record", key)).content(val).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let res: Option<SurrealRecord> = self.db().select(("record", key as i64)).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let res: Option<SurrealRecord> = self.db().select(("record", key)).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		let _: Option<SurrealRecord> =
			self.db().update(("record", key as i64)).content(val).await?;
		Ok(())
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let res: Option<SurrealRecord> = self.db().update(("record", key)).content(val).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let res: Option<SurrealRecord> = self.db().delete(("record", key as i64)).await?;
		assert!(res.is_some());
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		let res: Option<SurrealRecord> = self.db().delete(("record", key)).await?;
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
		let s = scan.start.map(|s| format!("START {}", s)).unwrap_or("".to_string());
		let l = scan.limit.map(|s| format!("LIMIT {}", s)).unwrap_or("".to_string());
		let c = scan.condition.as_ref().map(|s| format!("WHERE {}", s)).unwrap_or("".to_string());
		let p = scan.projection()?;
		let stm = match p {
			Projection::Id => format!("SELECT id FROM record {c} {s} {l}"),
			Projection::Full => format!("SELECT * FROM record {c} {s} {l}"),
			Projection::Count => {
				if s.is_empty() && l.is_empty() {
					format!("SELECT id FROM record {c} {s} {l} GROUP ALL")
				} else {
					format!("SELECT COUNT() FROM (SELECT id FROM record {c} {s} {l}) GROUP ALL")
				}
			}
		};
		match p {
			Projection::Id | Projection::Full => {
				let res: Vec<SurrealRecord> = self.db().query(stm).await?.take(0)?;
				Ok(res.len())
			}
			Projection::Count => {
				let res: Vec<Value> = self.db().query(stm).await?.take(0)?;
				Ok(
					res.first()
						.unwrap()
						.as_object()
						.unwrap()
						.get("count")
						.unwrap()
						.as_i64()
						.unwrap() as usize,
				)
			}
		}
	}
}
