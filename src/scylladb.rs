#![cfg(feature = "scylladb")]

use anyhow::Result;
use scylla::{Session, SessionBuilder};

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::docker::DockerParams;

pub(crate) const SCYLLADB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "scylladb/scylla",
	pre_args: "-p 9042:9042",
	post_args: "",
};

#[derive(Default)]
pub(crate) struct ScyllaDBClientProvider {}

impl BenchmarkEngine<ScylladbClient> for ScyllaDBClientProvider {
	async fn create_client(&self, endpoint: Option<String>) -> Result<ScylladbClient> {
		let node = endpoint.unwrap_or("127.0.0.1:9042".to_owned());
		let session = SessionBuilder::new().known_node(node).build().await?;
		Ok(ScylladbClient {
			session,
		})
	}
}

pub(crate) struct ScylladbClient {
	session: Session,
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
		self.session
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

	async fn create(&self, key: i32, record: &Record) -> Result<()> {
		self.session
			.query_unpaged(
				"INSERT INTO bench.record (id, text, integer) VALUES (?, ?, ?)",
				(&key, &record.text, &record.integer),
			)
			.await?;
		Ok(())
	}

	async fn read(&self, key: i32) -> Result<()> {
		let res = self
			.session
			.query_unpaged("SELECT id, text, integer FROM bench.record WHERE id=?", (&key,))
			.await?;
		assert_eq!(res.rows_num()?, 1);
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update(&self, key: i32, record: &Record) -> Result<()> {
		self.session
			.query_unpaged(
				"UPDATE bench.record SET text=?, integer=? WHERE id=?",
				(&record.text, &record.integer, &key),
			)
			.await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete(&self, key: i32) -> Result<()> {
		self.session.query_unpaged("DELETE FROM bench.record WHERE id=?", (&key,)).await?;
		Ok(())
	}
}
