#![cfg(feature = "mongodb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, NOT_SUPPORTED_ERROR};
use crate::docker::DockerParams;
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
use futures::{StreamExt, TryStreamExt};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::ClientOptions;
use mongodb::options::DatabaseOptions;
use mongodb::options::IndexOptions;
use mongodb::options::ReadConcern;
use mongodb::options::WriteConcern;
use mongodb::{bson, Client, Collection, Cursor, Database, IndexModel};
use serde_json::Value;
use std::hint::black_box;

pub(crate) const MONGODB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "mongo",
	pre_args: "-p 127.0.0.1:27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=root",
	post_args: "",
};

pub(crate) struct MongoDBClientProvider {}

impl BenchmarkEngine<MongoDBClient> for MongoDBClientProvider {
	async fn setup(_kt: KeyType, _columns: Columns) -> Result<Self> {
		Ok(Self {})
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<MongoDBClient> {
		Ok(MongoDBClient(create_mongo_client(endpoint).await?))
	}
}

pub(crate) struct MongoDBClient(Database);

async fn create_mongo_client(endpoint: Option<String>) -> Result<Database>
where
{
	let url = endpoint.unwrap_or("mongodb://root:root@localhost:27017".to_owned());
	let opts = ClientOptions::parse(&url).await?;
	let client = Client::with_options(opts)?;
	let db = client.database_with_options(
		"crud-bench",
		DatabaseOptions::builder()
			.write_concern(WriteConcern::builder().journal(false).build())
			.read_concern(ReadConcern::majority())
			.build(),
	);
	Ok(db)
}

impl BenchmarkClient for MongoDBClient {
	async fn startup(&self) -> Result<()> {
		let index = IndexOptions::builder().unique(true).build();
		let model = IndexModel::builder().keys(doc! { "id": 1 }).options(index).build();
		let _ = self.collection().create_index(model).await?;
		Ok(())
	}

	async fn compact(&self) -> Result<()> {
		// For a database compaction
		self.0
			.run_command(doc! {
				"compact": "record",
				"dryRun": false,
				"force": true,
			})
			.await?;
		// Ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let doc = self.read(key).await?;
		assert_eq!(doc.unwrap().get("id").unwrap().as_i64().unwrap() as u32, key);
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let doc = self.read(key.clone()).await?;
		assert_eq!(doc.unwrap().get_str("id")?, key);
		Ok(())
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

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}
}

impl MongoDBClient {
	fn collection(&self) -> Collection<Document> {
		self.0.collection("record")
	}

	fn to_doc<K>(key: K, mut val: Value) -> Result<Bson>
	where
		K: Into<Value> + Into<Bson>,
	{
		let obj = val.as_object_mut().unwrap();
		obj.insert("id".to_string(), key.into());
		Ok(bson::to_bson(&val)?)
	}

	async fn create<K>(&self, key: K, val: Value) -> Result<()>
	where
		K: Into<Value> + Into<Bson>,
	{
		let bson = Self::to_doc(key, val)?;
		let doc = bson.as_document().unwrap();
		let res = self.collection().insert_one(doc).await?;
		assert_ne!(res.inserted_id, Bson::Null);
		Ok(())
	}

	async fn read<K>(&self, key: K) -> Result<Option<Document>>
	where
		K: Into<Bson>,
	{
		let filter = doc! { "id": key };
		let doc = self.collection().find_one(filter).await?;
		assert!(doc.is_some());
		Ok(doc)
	}

	async fn update<K>(&self, key: K, val: Value) -> Result<()>
	where
		K: Into<Value> + Into<Bson> + Clone,
	{
		let bson = Self::to_doc(key.clone(), val)?;
		let doc = bson.as_document().unwrap();
		let filter = doc! { "id": key };
		let res = self.collection().replace_one(filter, doc).await?;
		assert_eq!(res.modified_count, 1);
		Ok(())
	}

	async fn delete<K>(&self, key: K) -> Result<()>
	where
		K: Into<Bson>,
	{
		let filter = doc! { "id": key };
		let res = self.collection().delete_one(filter).await?;
		assert_eq!(res.deleted_count, 1);
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
		// Consume documents function
		let consume = |mut cursor: Cursor<Document>| async move {
			let mut count = 0;
			while let Some(doc) = cursor.try_next().await? {
				black_box(doc);
				count += 1;
			}
			Ok(count)
		};
		// Perform the relevant projection scan type
		match scan.projection()? {
			Projection::Id => {
				let cursor = self
					.collection()
					.find(doc! {})
					.skip(s as u64)
					.limit(l as i64)
					.projection(doc! { "id": 1 })
					.await?;
				consume(cursor).await
			}
			Projection::Full => {
				let cursor = self.collection().find(doc! {}).skip(s as u64).limit(l as i64).await?;
				consume(cursor).await
			}
			Projection::Count => {
				let pipeline = vec![
					doc! { "$skip": s as i64 },
					doc! { "$limit": l as i64 },
					doc! { "$count": "count" },
				];
				let mut cursor = self.collection().aggregate(pipeline).await?;
				if let Some(result) = cursor.next().await {
					let doc: Document = result?;
					let count = doc.get_i32("count").unwrap_or(0);
					Ok(count as usize)
				} else {
					bail!("No row returned");
				}
			}
		}
	}
}
