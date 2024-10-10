#![cfg(feature = "mongodb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::docker::DockerParams;
use crate::KeyType;
use anyhow::{bail, Result};
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

pub(crate) struct MongoDBClientIntegerProvider {}

impl BenchmarkEngine<MongoDBIntegerClient> for MongoDBClientIntegerProvider {
	async fn setup(_: KeyType) -> Result<Self> {
		Ok(Self {})
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<MongoDBIntegerClient> {
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
		let collection = db.collection::<MongoDBIntegerRecord>("record");
		Ok(MongoDBIntegerClient(collection))
	}
}

pub(crate) struct MongoDBClientStringProvider {}

impl BenchmarkEngine<MongoDBStringClient> for MongoDBClientStringProvider {
	async fn setup(_: KeyType) -> Result<Self> {
		Ok(Self {})
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<MongoDBStringClient> {
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
		let collection = db.collection::<MongoDBStringRecord>("record");
		Ok(MongoDBStringClient(collection))
	}
}

#[derive(Debug, Serialize, Deserialize)]
struct MongoDBIntegerRecord {
	id: u32,
	text: String,
	integer: i32,
}

impl MongoDBIntegerRecord {
	fn new(key: u32, record: &Record) -> Self {
		Self {
			id: key,
			text: record.text.clone(),
			integer: record.integer,
		}
	}
}

#[derive(Debug, Serialize, Deserialize)]
struct MongoDBStringRecord {
	id: String,
	text: String,
	integer: i32,
}

impl MongoDBStringRecord {
	fn new(key: String, record: &Record) -> Self {
		Self {
			id: key,
			text: record.text.clone(),
			integer: record.integer,
		}
	}
}

pub(crate) struct MongoDBStringClient(Collection<MongoDBStringRecord>);

impl BenchmarkClient for MongoDBStringClient {
	async fn startup(&self) -> Result<()> {
		let index_options = IndexOptions::builder().unique(true).build();
		let index_model =
			IndexModel::builder().keys(doc! { "id": 1 }).options(index_options).build();
		self.0.create_index(index_model).await?;
		Ok(())
	}

	async fn create_u32(&self, _: u32, _: &Record) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn create_string(&self, key: String, record: &Record) -> Result<()> {
		let doc = MongoDBStringRecord::new(key, record);
		self.0.insert_one(doc).await?;
		Ok(())
	}

	async fn read_u32(&self, _: u32) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let filter = doc! { "id": key.clone() };
		let doc = self.0.find_one(filter).await?;
		assert_eq!(doc.unwrap().id, key);
		Ok(())
	}

	async fn update_u32(&self, _: u32, _: &Record) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn update_string(&self, key: String, record: &Record) -> Result<()> {
		let doc = MongoDBStringRecord::new(key.clone(), record);
		let filter = doc! { "id": key };
		let res = self.0.replace_one(filter, doc).await?;
		assert_eq!(res.modified_count, 1);
		Ok(())
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

pub(crate) struct MongoDBIntegerClient(Collection<MongoDBIntegerRecord>);

impl BenchmarkClient for MongoDBIntegerClient {
	async fn startup(&self) -> Result<()> {
		let index_options = IndexOptions::builder().unique(true).build();
		let index_model =
			IndexModel::builder().keys(doc! { "id": 1 }).options(index_options).build();
		self.0.create_index(index_model).await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, record: &Record) -> Result<()> {
		let doc = MongoDBIntegerRecord::new(key, record);
		self.0.insert_one(doc).await?;
		Ok(())
	}

	async fn create_string(&self, _: String, _: &Record) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let filter = doc! { "id": key };
		let doc = self.0.find_one(filter).await?;
		assert_eq!(doc.unwrap().id, key);
		Ok(())
	}

	async fn read_string(&self, _: String) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn update_u32(&self, key: u32, record: &Record) -> Result<()> {
		let doc = MongoDBIntegerRecord::new(key, record);
		let filter = doc! { "id": key };
		let res = self.0.replace_one(filter, doc).await?;
		assert_eq!(res.modified_count, 1);
		Ok(())
	}

	async fn update_string(&self, _: String, _: &Record) -> Result<()> {
		bail!("Invalid MongoDBClient")
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		let filter = doc! { "id": key };
		let res = self.0.delete_one(filter).await?;
		assert_eq!(res.deleted_count, 1);
		Ok(())
	}

	async fn delete_string(&self, _: String) -> Result<()> {
		todo!()
	}
}
