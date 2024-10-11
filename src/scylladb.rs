#![cfg(feature = "scylladb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::docker::DockerParams;
use crate::valueprovider::Record;
use crate::KeyType;
use anyhow::Result;
use scylla::_macro_internal::SerializeValue;
use scylla::{Session, SessionBuilder};

pub(crate) const SCYLLADB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "scylladb/scylla",
	pre_args: "-p 9042:9042",
	post_args: "",
};

pub(crate) struct ScyllaDBClientProvider(KeyType);

impl BenchmarkEngine<ScylladbClient> for ScyllaDBClientProvider {
	async fn setup(kt: KeyType) -> Result<Self> {
		Ok(ScyllaDBClientProvider(kt))
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<ScylladbClient> {
		let node = endpoint.unwrap_or("127.0.0.1:9042".to_owned());
		let session = SessionBuilder::new().known_node(node).build().await?;
		Ok(ScylladbClient {
			session,
			kt: self.0,
		})
	}
}

pub(crate) struct ScylladbClient {
	session: Session,
	kt: KeyType,
}

impl BenchmarkClient for ScylladbClient {
	async fn startup(&self) -> Result<()> {
		self.session
			.query_unpaged(
				"
					CREATE KEYSPACE bench 
					WITH replication = { 'class': 'SimpleStrategy', 'replication_factor' : 1 } 
					AND durable_writes = true
				",
				(),
			)
			.await?;
		let id_type = match self.kt {
			KeyType::Integer => "int",
			KeyType::String26 | KeyType::String90 | KeyType::String506 => "TEXT",
			KeyType::Uuid => {
				todo!()
			}
		};
		self.session
			.query_unpaged(
				format!(
					"
					CREATE TABLE bench.record (
						id {id_type} PRIMARY KEY,
						text text,
						integer int
					)
				"
				),
				(),
			)
			.await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, record: Record) -> Result<()> {
		self.create_key(key as i32, record).await
	}

	async fn create_string(&self, key: String, record: Record) -> Result<()> {
		self.create_key(key, record).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read_key(key as i32).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read_key(key).await
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, record: Record) -> Result<()> {
		self.update_key(key as i32, record).await
	}

	async fn update_string(&self, key: String, record: Record) -> Result<()> {
		self.update_key(key, record).await
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete_key(key as i32).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete_key(key).await
	}
}

impl ScylladbClient {
	async fn create_key<T>(&self, key: T, record: Record) -> Result<()>
	where
		T: SerializeValue,
	{
		self.session
			.query_unpaged(
				"INSERT INTO bench.record (id, text, integer) VALUES (?, ?, ?)",
				(&key, &record.text, &record.integer),
			)
			.await?;
		Ok(())
	}

	async fn read_key<T>(&self, key: T) -> Result<()>
	where
		T: SerializeValue,
	{
		let res = self
			.session
			.query_unpaged("SELECT id, text, integer FROM bench.record WHERE id=?", (&key,))
			.await?;
		assert_eq!(res.rows_num()?, 1);
		Ok(())
	}

	async fn update_key<T>(&self, key: T, record: Record) -> Result<()>
	where
		T: SerializeValue,
	{
		self.session
			.query_unpaged(
				"UPDATE bench.record SET text=?, integer=? WHERE id=?",
				(&record.text, &record.integer, &key),
			)
			.await?;
		Ok(())
	}

	async fn delete_key<T>(&self, key: T) -> Result<()>
	where
		T: SerializeValue,
	{
		self.session.query_unpaged("DELETE FROM bench.record WHERE id=?", (&key,)).await?;
		Ok(())
	}
}
