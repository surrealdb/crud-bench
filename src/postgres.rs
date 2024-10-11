#![cfg(feature = "postgres")]

use tokio_postgres::{Client, NoTls};

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::docker::DockerParams;
use crate::KeyType;
use anyhow::Result;
use tokio_postgres::types::ToSql;

pub(crate) const POSTGRES_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "postgres",
	pre_args: "-p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres",
	post_args: "postgres -N 1024",
};

pub(crate) struct PostgresClientProvider(KeyType);

impl BenchmarkEngine<PostgresClient> for PostgresClientProvider {
	async fn setup(kt: KeyType) -> Result<Self> {
		Ok(Self(kt))
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<PostgresClient> {
		let url = endpoint.unwrap_or("host=localhost user=postgres password=postgres".to_owned());
		let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
		tokio::spawn(async move {
			if let Err(e) = connection.await {
				eprintln!("connection error: {}", e);
			}
		});
		Ok(PostgresClient {
			client,
			kt: self.0,
		})
	}
}

pub(crate) struct PostgresClient {
	client: Client,
	kt: KeyType,
}

impl BenchmarkClient for PostgresClient {
	async fn startup(&self) -> Result<()> {
		let id_type = match self.kt {
			KeyType::Integer => "SERIAL",
			KeyType::String26 => "VARCHAR(26)",
			KeyType::String90 => "VARCHAR(90)",
			KeyType::String506 => "VARCHAR(506)",
			KeyType::Uuid => {
				todo!()
			}
		};
		self.client
			.batch_execute(&format!(
				"
					CREATE TABLE record (
						id      {id_type} PRIMARY KEY,
						text    TEXT NOT NULL,
						integer    INTEGER NOT NULL
					)
				"
			))
			.await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, record: &Record) -> Result<()> {
		self.create_key(key as i32, record).await
	}

	async fn create_string(&self, key: String, record: &Record) -> Result<()> {
		self.create_key(key, record).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read_key(key as i32).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read_key(key).await
	}

	async fn update_u32(&self, key: u32, record: &Record) -> Result<()> {
		self.update_key(key as i32, record).await
	}

	async fn update_string(&self, key: String, record: &Record) -> Result<()> {
		self.update_key(key, record).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete_key(key as i32).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete_key(key).await
	}
}

impl PostgresClient {
	async fn create_key<T>(&self, key: T, record: &Record) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let res = self
			.client
			.execute(
				"INSERT INTO record (id, text, integer) VALUES ($1, $2, $3)",
				&[&key, &record.text, &record.integer],
			)
			.await?;
		assert_eq!(res, 1);
		Ok(())
	}
	async fn read_key<T>(&self, key: T) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let res =
			self.client.query("SELECT id, text, integer FROM record WHERE id=$1", &[&key]).await?;
		assert_eq!(res.len(), 1);
		Ok(())
	}

	async fn update_key<T>(&self, key: T, record: &Record) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let res = self
			.client
			.execute(
				"UPDATE record SET text=$1, integer=$2 WHERE id=$3",
				&[&record.text, &record.integer, &key],
			)
			.await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn delete_key<T>(&self, key: T) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let res = self.client.execute("DELETE FROM record WHERE id=$1", &[&key]).await?;
		assert_eq!(res, 1);
		Ok(())
	}
}
