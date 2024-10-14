#![cfg(feature = "mongodb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::docker::DockerParams;
use crate::valueprovider::Columns;
use crate::KeyType;
use anyhow::{bail, Result};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::ClientOptions;
use mongodb::options::DatabaseOptions;
use mongodb::options::IndexOptions;
use mongodb::options::ReadConcern;
use mongodb::options::WriteConcern;
use mongodb::{bson, Client, Collection, IndexModel};
use serde_json::Value;

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

	async fn create_u32(&self, _: u32, _: Value) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn create_string(&self, key: String, mut val: Value) -> Result<()> {
		val.as_object_mut().unwrap().insert("id".to_string(), Value::String(key));
		if let Bson::Document(doc) = bson::to_bson(&val)? {
			self.0.insert_one(doc).await?;
			Ok(())
		} else {
			bail!("Invalid document")
		}
	}

	async fn read_u32(&self, _: u32) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let filter = doc! { "id": key.clone() };
		let doc = self.0.find_one(filter).await?;
		assert_eq!(doc.unwrap().get_str("id")?, key);
		Ok(())
	}

	async fn update_u32(&self, _: u32, _: Value) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let filter = doc! { "id": key };
		if let Bson::Document(doc) = bson::to_bson(&val)? {
			let res = self.0.replace_one(filter, doc).await?;
			assert_eq!(res.modified_count, 1);
			Ok(())
		} else {
			bail!("Invalid document")
		}
	}

	async fn delete_u32(&self, _: u32) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		let filter = doc! { "id": key };
		let res = self.0.delete_one(filter).await?;
		assert_eq!(res.deleted_count, 1);
		Ok(())
	}
}
