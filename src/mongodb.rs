#![cfg(feature = "mongodb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, NOT_SUPPORTED_ERROR};
use crate::docker::DockerParams;
use crate::valueprovider::Columns;
use crate::{KeyType, Scan};
use anyhow::{bail, Result};
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::ClientOptions;
use mongodb::options::DatabaseOptions;
use mongodb::options::IndexOptions;
use mongodb::options::ReadConcern;
use mongodb::options::WriteConcern;
use mongodb::{bson, Client, Collection, IndexModel};
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

pub(crate) struct MongoDBClient(Collection<Document>);

async fn create_mongo_client<T>(endpoint: Option<String>) -> Result<Collection<T>>
where
	T: Send + Sync,
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
	Ok(db.collection("record"))
}

async fn mongo_startup<T>(collection: &Collection<T>) -> Result<()>
where
	T: Send + Sync,
{
	let index_options = IndexOptions::builder().unique(true).build();
	let index_model = IndexModel::builder().keys(doc! { "id": 1 }).options(index_options).build();
	collection.create_index(index_model).await?;
	Ok(())
}

impl BenchmarkClient for MongoDBClient {
	async fn startup(&self) -> Result<()> {
		mongo_startup(&self.0).await
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

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
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
}

impl MongoDBClient {
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
		let res = self.0.insert_one(doc).await?;
		assert_ne!(res.inserted_id, Bson::Null, "create");
		Ok(())
	}

	async fn read<K>(&self, key: K) -> Result<Option<Document>>
	where
		K: Into<Bson>,
	{
		let filter = doc! { "id": key };
		let doc = self.0.find_one(filter).await?;
		assert!(doc.is_some(), "read");
		Ok(doc)
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(0);
		let k = scan.keys_only.unwrap_or(false);
		let filter = doc! {};
		let mut cursor = if k {
			self.0.find(filter).skip(s as u64).limit(l as i64).projection(doc! { "id": 1 }).await?
		} else {
			self.0.find(filter).skip(s as u64).limit(l as i64).await?
		};
		let mut count = 0;
		while let Some(doc) = cursor.try_next().await? {
			black_box(doc);
			count += 1;
		}
		Ok(count)
	}

	async fn update<K>(&self, key: K, val: Value) -> Result<()>
	where
		K: Into<Value> + Into<Bson> + Clone,
	{
		let bson = Self::to_doc(key.clone(), val)?;
		let doc = bson.as_document().unwrap();
		let filter = doc! { "id": key };
		let res = self.0.replace_one(filter, doc).await?;
		assert_eq!(res.modified_count, 1, "update");
		Ok(())
	}

	async fn delete<K>(&self, key: K) -> Result<()>
	where
		K: Into<Bson>,
	{
		let filter = doc! { "id": key };
		let res = self.0.delete_one(filter).await?;
		assert_eq!(res.deleted_count, 1, "delete");
		Ok(())
	}
}
