#![cfg(feature = "mongodb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::docker::DockerParams;
use crate::KeyType;
use anyhow::Result;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::options::DatabaseOptions;
use mongodb::options::IndexOptions;
use mongodb::options::ReadConcern;
use mongodb::options::WriteConcern;
use mongodb::{Client, Collection, IndexModel};
use serde::{Deserialize, Serialize};

pub(crate) const MONGODB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "mongo",
	pre_args: "-p 127.0.0.1:27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=root",
	post_args: "",
};

pub(crate) struct MongoDBClientProvider {}

impl BenchmarkEngine<MongoDBClient> for MongoDBClientProvider {
	async fn setup(_: KeyType) -> Result<Self> {
		Ok(Self {})
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<MongoDBClient> {
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
		let collection = db.collection::<MongoDBRecord>("record");
		Ok(MongoDBClient(collection))
	}
}

#[derive(Debug, Serialize, Deserialize)]
struct MongoDBRecord {
	id: u32,
	text: String,
	integer: i32,
}

impl MongoDBRecord {
	fn new(key: u32, record: &Record) -> Self {
		Self {
			id: key,
			text: record.text.clone(),
			integer: record.integer,
		}
	}
}

pub(crate) struct MongoDBClient(Collection<MongoDBRecord>);

impl BenchmarkClient for MongoDBClient {
	async fn startup(&self) -> Result<()> {
		let index_options = IndexOptions::builder().unique(true).build();
		let index_model =
			IndexModel::builder().keys(doc! { "id": 1 }).options(index_options).build();
		self.0.create_index(index_model).await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, record: &Record) -> Result<()> {
		let doc = MongoDBRecord::new(key, record);
		self.0.insert_one(doc).await?;
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let filter = doc! { "id": key };
		let doc = self.0.find_one(filter).await?;
		assert_eq!(doc.unwrap().id, key);
		Ok(())
	}

	async fn update_u32(&self, key: u32, record: &Record) -> Result<()> {
		let doc = MongoDBRecord::new(key, record);
		let filter = doc! { "id": key };
		let res = self.0.replace_one(filter, doc).await?;
		assert_eq!(res.modified_count, 1);
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let filter = doc! { "id": key };
		let res = self.0.delete_one(filter).await?;
		assert_eq!(res.deleted_count, 1);
		Ok(())
	}
}
