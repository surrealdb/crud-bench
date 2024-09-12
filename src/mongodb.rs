use anyhow::Result;
use mongodb::bson::doc;
use mongodb::options::IndexOptions;
use mongodb::{options::ClientOptions, Client, Collection, IndexModel};
use serde::{Deserialize, Serialize};

use crate::benchmark::{BenchmarkClient, BenchmarkClientProvider, Record};
use crate::docker::DockerParams;

pub(crate) const MONGODB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "mongo",
	pre_args: "-p 127.0.0.1:27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=root",
	post_args: "",
};

#[derive(Default)]
pub(crate) struct MongoDBClientProvider {}

impl BenchmarkClientProvider<MongoDBClient> for MongoDBClientProvider {
	async fn create_client(&self) -> Result<MongoDBClient> {
		let client_options = ClientOptions::parse("mongodb://root:root@localhost:27017").await?;
		let client = Client::with_options(client_options)?;
		let db = client.database("crud-bench");
		let collection = db.collection::<MongoDBRecord>("record");
		Ok(MongoDBClient {
			collection,
		})
	}
}

#[derive(Debug, Serialize, Deserialize)]
struct MongoDBRecord {
	id: i32,
	text: String,
	integer: i32,
}

impl MongoDBRecord {
	fn new(key: i32, record: &Record) -> Self {
		Self {
			id: key,
			text: record.text.clone(),
			integer: record.integer,
		}
	}
}

pub(crate) struct MongoDBClient {
	collection: Collection<MongoDBRecord>,
}

impl BenchmarkClient for MongoDBClient {
	async fn prepare(&mut self) -> Result<()> {
		let index_options = IndexOptions::builder().unique(true).build();
		let index_model =
			IndexModel::builder().keys(doc! { "id": 1 }).options(index_options).build();
		self.collection.create_index(index_model, None).await?;
		Ok(())
	}

	async fn read(&mut self, key: i32) -> Result<()> {
		let filter = doc! { "id": key };
		let doc = self.collection.find_one(Some(filter), None).await?;
		assert_eq!(doc.unwrap().id, key);
		Ok(())
	}

	async fn create(&mut self, key: i32, record: &Record) -> Result<()> {
		let doc = MongoDBRecord::new(key, record);
		self.collection.insert_one(doc, None).await?;
		Ok(())
	}

	async fn update(&mut self, key: i32, record: &Record) -> Result<()> {
		let doc = MongoDBRecord::new(key, record);
		let filter = doc! { "id": key };
		let res = self.collection.replace_one(filter, doc, None).await?;
		assert_eq!(res.modified_count, 1);
		Ok(())
	}

	async fn delete(&mut self, key: i32) -> Result<()> {
		let filter = doc! { "id": key };
		let res = self.collection.delete_one(filter, None).await?;
		assert_eq!(res.deleted_count, 1);
		Ok(())
	}
}
