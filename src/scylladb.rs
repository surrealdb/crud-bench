#![cfg(feature = "scylladb")]

use anyhow::Result;
use scylla::{Session, SessionBuilder};

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::docker::DockerParams;
use crate::KeyType;

pub(crate) const SCYLLADB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "scylladb/scylla",
	pre_args: "-p 9042:9042",
	post_args: "",
};

pub(crate) struct ScyllaDBClientProvider {}

impl BenchmarkEngine<ScylladbClient> for ScyllaDBClientProvider {
	async fn setup(_: KeyType) -> Result<Self> {
		Ok(ScyllaDBClientProvider {})
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<ScylladbClient> {
		let node = endpoint.unwrap_or("127.0.0.1:9042".to_owned());
		let session = SessionBuilder::new().known_node(node).build().await?;
		Ok(ScylladbClient(session))
	}
}

pub(crate) struct ScylladbClient(Session);

impl BenchmarkClient for ScylladbClient {
	async fn startup(&self) -> Result<()> {
		self.0
			.query_unpaged(
				"
					CREATE KEYSPACE bench 
					WITH replication = { 'class': 'SimpleStrategy', 'replication_factor' : 1 } 
					AND durable_writes = true
				",
				(),
			)
			.await?;
		self.0
			.query_unpaged(
				"
					CREATE TABLE bench.record (
						id int PRIMARY KEY,
						text text,
						integer int
					)
				",
				(),
			)
			.await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, record: &Record) -> Result<()> {
		let key = key as i32;
		self.0
			.query_unpaged(
				"INSERT INTO bench.record (id, text, integer) VALUES (?, ?, ?)",
				(&key, &record.text, &record.integer),
			)
			.await?;
		Ok(())
	}

	async fn create_string(&self, key: String, record: &Record) -> Result<()> {
		todo!()
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let key = key as i32;
		let res = self
			.0
			.query_unpaged("SELECT id, text, integer FROM bench.record WHERE id=?", (&key,))
			.await?;
		assert_eq!(res.rows_num()?, 1);
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		todo!()
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, record: &Record) -> Result<()> {
		let key = key as i32;
		self.0
			.query_unpaged(
				"UPDATE bench.record SET text=?, integer=? WHERE id=?",
				(&record.text, &record.integer, &key),
			)
			.await?;
		Ok(())
	}

	async fn update_string(&self, key: String, record: &Record) -> Result<()> {
		todo!()
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		let key = key as i32;
		self.0.query_unpaged("DELETE FROM bench.record WHERE id=?", (&key,)).await?;
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		todo!()
	}
}
