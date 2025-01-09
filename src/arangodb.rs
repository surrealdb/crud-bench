#![cfg(feature = "arangodb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
use arangors::client::reqwest::ReqwestClient;
use arangors::document::options::InsertOptions;
use arangors::document::options::RemoveOptions;
use arangors::{Collection, Connection, Database};
use serde_json::Value;
use std::hint::black_box;
use std::time::Duration;

pub(crate) const ARANGODB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "arangodb",
	pre_args: "-p 127.0.0.1:8529:8529 -e ARANGO_NO_AUTH=1",
	post_args: "",
};

pub(crate) struct ArangoDBClientProvider {
	url: String,
}

impl BenchmarkEngine<ArangoDBClient> for ArangoDBClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, endpoint: Option<&str>) -> Result<Self> {
		Ok(Self {
			url: endpoint.unwrap_or("http://127.0.0.1:8529").to_owned(),
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<ArangoDBClient> {
		let (db, co) = create_arango_client(&self.url).await?;
		Ok(ArangoDBClient {
			database: db,
			collection: co,
		})
	}
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		Some(Duration::from_secs(15))
	}
}

pub(crate) struct ArangoDBClient {
	database: Database<ReqwestClient>,
	collection: Collection<ReqwestClient>,
}

async fn create_arango_client(
	url: &str,
) -> Result<(Database<ReqwestClient>, Collection<ReqwestClient>)> {
	let conn = Connection::establish_without_auth(url).await.unwrap();
	let db = match conn.create_database("crud-bench").await {
		Err(_) => conn.db("crud-bench").await.unwrap(),
		Ok(db) => db,
	};
	let co = match db.create_collection("record").await {
		Err(_) => db.collection("record").await.unwrap(),
		Ok(db) => db,
	};
	Ok((db, co))
}

impl BenchmarkClient for ArangoDBClient {
	async fn startup(&self) -> Result<()> {
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create(key.to_string(), val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read(key.to_string()).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read(key).await
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		self.update(key.to_string(), val).await
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		self.update(key, val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key.to_string()).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key).await
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
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
		let res = self.collection.create_document(val, opt).await?;
		assert!(res.new_doc().is_some());
		Ok(())
	}

	async fn read(&self, key: String) -> Result<()> {
		let doc = self.collection.document::<Value>(&key).await?;
		assert!(doc.is_object());
		assert_eq!(doc.get("_key").unwrap().as_str().unwrap(), key);
		Ok(())
	}

	async fn update(&self, key: String, val: Value) -> Result<()> {
		let val = Self::to_doc(key, val)?;
		let opt =
			InsertOptions::builder().wait_for_sync(false).return_new(true).overwrite(true).build();
		let res = self.collection.create_document(val, opt).await?;
		assert!(res.new_doc().is_some());
		Ok(())
	}

	async fn delete(&self, key: String) -> Result<()> {
		let opt = RemoveOptions::builder().wait_for_sync(true).build();
		let res = self.collection.remove_document::<Value>(&key, opt, None).await?;
		assert!(res.has_response());
		Ok(())
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Contional scans are not supported
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(i64::MAX as usize);
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("FOR doc IN record LIMIT {s}, {l} RETURN {{ _id: doc._id }}");
				let res: Vec<Value> = self.database.aql_str(&stm).await.unwrap();
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
				let stm = format!("FOR doc IN record LIMIT {s}, {l} RETURN doc");
				let res: Vec<Value> = self.database.aql_str(&stm).await.unwrap();
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
					"FOR doc IN record LIMIT {s}, {l} COLLECT WITH COUNT INTO count RETURN count"
				);
				let res: Vec<Value> = self.database.aql_str(&stm).await.unwrap();
				let count = res.first().unwrap().as_i64().unwrap();
				Ok(count as usize)
			}
		}
	}
}
