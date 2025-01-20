#![cfg(feature = "arangodb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::ArangoDBDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{bail, Result};
use arangors::client::reqwest::ReqwestClient;
use arangors::document::options::InsertOptions;
use arangors::document::options::RemoveOptions;
use arangors::{Collection, Connection, Database, GenericConnection};
use serde_json::Value;
use std::hint::black_box;
use std::time::Duration;
use tokio::sync::Mutex;

pub const DEFAULT: &str = "http://127.0.0.1:8529";

pub(crate) const ARANGODB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "arangodb",
	pre_args: "--ulimit nofile=65536:65536 -p 127.0.0.1:8529:8529 -e ARANGO_NO_AUTH=1",
	post_args: "--server.scheduler-queue-size 8192 --server.prio1-size 8192 --server.prio2-size 8192 --server.maximal-queue-size 8192",
};

pub(crate) struct ArangoDBClientProvider {
	key: KeyType,
	url: String,
}

impl BenchmarkEngine<ArangoDBClient> for ArangoDBClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		Ok(Self {
			key: kt,
			url: options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned(),
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<ArangoDBClient> {
		let (conn, db, co) = create_arango_client(&self.url).await?;
		Ok(ArangoDBClient {
			keytype: self.key,
			connection: conn,
			database: Mutex::new(db),
			collection: Mutex::new(co),
		})
	}
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		Some(Duration::from_secs(15))
	}
}

pub(crate) struct ArangoDBClient {
	keytype: KeyType,
	connection: GenericConnection<ReqwestClient>,
	database: Mutex<Database<ReqwestClient>>,
	collection: Mutex<Collection<ReqwestClient>>,
}

async fn create_arango_client(
	url: &str,
) -> Result<(GenericConnection<ReqwestClient>, Database<ReqwestClient>, Collection<ReqwestClient>)>
{
	// Create the connection to the database
	let conn = Connection::establish_without_auth(url).await.unwrap();
	// Create the benchmarking database
	let db = match conn.create_database("crud-bench").await {
		Err(_) => conn.db("crud-bench").await.unwrap(),
		Ok(db) => db,
	};
	// Create the becnhmark record collection
	let co = match db.create_collection("record").await {
		Err(_) => db.collection("record").await.unwrap(),
		Ok(db) => db,
	};
	Ok((conn, db, co))
}

impl BenchmarkClient for ArangoDBClient {
	async fn startup(&self) -> Result<()> {
		// Ensure we drop the database first.
		// We can drop the database initially
		// because the other clients will be
		// created subsequently, and will then
		// create the database as necessary.
		self.connection.drop_database("crud-bench").await?;
		// Everything ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.create(key.to_string(), val).await,
		}
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.create(key, val).await,
		}
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.read(key.to_string()).await,
		}
	}

	async fn read_string(&self, key: String) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.read(key).await,
		}
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.update(key.to_string(), val).await,
		}
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.update(key, val).await,
		}
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.delete(key.to_string()).await,
		}
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.delete(key).await,
		}
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.scan(scan).await,
		}
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.scan(scan).await,
		}
	}
}

impl ArangoDBClient {
	fn to_doc(key: String, mut val: Value) -> Result<Value> {
		let obj = val.as_object_mut().unwrap();
		obj.insert("_key".to_string(), key.into());
		Ok(val)
	}

	async fn create(&self, key: String, val: Value) -> Result<()> {
		let val = Self::to_doc(key, val)?;
		let opt =
			InsertOptions::builder().wait_for_sync(false).return_new(true).overwrite(false).build();
		let res = { self.collection.lock().await.create_document(val, opt).await? };
		assert!(res.new_doc().is_some());
		Ok(())
	}

	async fn read(&self, key: String) -> Result<()> {
		let doc = { self.collection.lock().await.document::<Value>(&key).await? };
		assert!(doc.is_object());
		assert_eq!(doc.get("_key").unwrap().as_str().unwrap(), key);
		Ok(())
	}

	async fn update(&self, key: String, val: Value) -> Result<()> {
		let val = Self::to_doc(key, val)?;
		let opt =
			InsertOptions::builder().wait_for_sync(false).return_new(true).overwrite(true).build();
		let res = { self.collection.lock().await.create_document(val, opt).await? };
		assert!(res.new_doc().is_some());
		Ok(())
	}

	async fn delete(&self, key: String) -> Result<()> {
		let opt = RemoveOptions::builder().wait_for_sync(true).build();
		let res = { self.collection.lock().await.remove_document::<Value>(&key, opt, None).await? };
		assert!(res.has_response());
		Ok(())
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(i64::MAX as usize);
		let c = ArangoDBDialect::filter_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("FOR r IN record {c} LIMIT {s}, {l} RETURN {{ _id: r._id }}");
				let res: Vec<Value> = { self.database.lock().await.aql_str(&stm).await.unwrap() };
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let stm = format!("FOR r IN record {c} LIMIT {s}, {l} RETURN r");
				let res: Vec<Value> = { self.database.lock().await.aql_str(&stm).await.unwrap() };
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let stm = format!(
					"FOR r IN record {c} LIMIT {s}, {l} COLLECT WITH COUNT INTO count RETURN count"
				);
				let res: Vec<Value> = { self.database.lock().await.aql_str(&stm).await.unwrap() };
				let count = res.first().unwrap().as_i64().unwrap();
				Ok(count as usize)
			}
		}
	}
}
