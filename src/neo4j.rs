#![cfg(feature = "neo4j")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::Neo4jDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
use neo4rs::query;
use neo4rs::BoltType;
use neo4rs::ConfigBuilder;
use neo4rs::Graph;
use serde_json::Value;
use std::hint::black_box;

pub(crate) const NEO4J_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "neo4j",
	pre_args: "--ulimit nofile=65536:65536 -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 -e NEO4J_AUTH=none",
	post_args: "",
};

pub(crate) struct Neo4jClientProvider {
	url: String,
	columns: Columns,
}

impl BenchmarkEngine<Neo4jClient> for Neo4jClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, columns: Columns, endpoint: Option<&str>) -> Result<Self> {
		Ok(Self {
			url: endpoint.unwrap_or("127.0.0.1:7687").to_owned(),
			columns,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<Neo4jClient> {
		Ok(Neo4jClient {
			graph: create_neo_client(&self.url).await?,
			columns: self.columns.clone(),
		})
	}
}

pub(crate) struct Neo4jClient {
	graph: Graph,
	columns: Columns,
}

async fn create_neo_client(url: &str) -> Result<Graph>
where
{
	let config = ConfigBuilder::default()
		.uri(url)
		.db("neo4j")
		.user("neo4j")
		.password("neo4j")
		.fetch_size(500)
		.max_connections(1)
		.build()?;
	Ok(Graph::connect(config).await?)
}

impl BenchmarkClient for Neo4jClient {
	async fn startup(&self) -> Result<()> {
		let stm = "CREATE INDEX FOR (r:Record) ON (r.id);";
		self.graph.execute(query(stm)).await?.next().await.ok();
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read(key).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read(key).await
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

impl Neo4jClient {
	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: Into<BoltType> + Sync,
	{
		let fields = Neo4jDialect::create_clause(&self.columns, val)?;
		let stm = format!("CREATE (r:Record {{ id: $id, {fields} }}) RETURN r.id");
		let stm = query(&stm).param("id", key);
		let mut res = self.graph.execute(stm).await.unwrap();
		assert!(matches!(res.next().await, Ok(Some(_))));
		assert!(matches!(res.next().await, Ok(None)));
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<()>
	where
		T: Into<BoltType> + Sync,
	{
		let stm = "MATCH (r:Record { id: $id }) RETURN r";
		let stm = query(stm).param("id", key);
		let mut res = self.graph.execute(stm).await.unwrap();
		assert!(matches!(black_box(res.next().await), Ok(Some(_))));
		assert!(matches!(res.next().await, Ok(None)));
		Ok(())
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: Into<BoltType> + Sync,
	{
		let fields = Neo4jDialect::update_clause(&self.columns, val)?;
		let stm = format!("MATCH (r:Record {{ id: $id }}) SET {fields} RETURN r.id");
		let stm = query(&stm).param("id", key);
		let mut res = self.graph.execute(stm).await.unwrap();
		assert!(matches!(res.next().await, Ok(Some(_))));
		assert!(matches!(res.next().await, Ok(None)));
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: Into<BoltType> + Sync,
	{
		let stm = "MATCH (r:Record { id: $id }) WITH r, r.id AS id DETACH DELETE r RETURN id";
		let stm = query(stm).param("id", key);
		let mut res = self.graph.execute(stm).await.unwrap();
		assert!(matches!(res.next().await, Ok(Some(_))));
		assert!(matches!(res.next().await, Ok(None)));
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
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("MATCH (r) SKIP {s} LIMIT {l} RETURN r.id");
				let mut res = self.graph.execute(query(&stm)).await.unwrap();
				let mut count = 0;
				while let Ok(Some(v)) = res.next().await {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let stm = format!("MATCH (r) SKIP {s} LIMIT {l} RETURN r");
				let mut res = self.graph.execute(query(&stm)).await.unwrap();
				let mut count = 0;
				while let Ok(Some(v)) = res.next().await {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let stm = format!("MATCH (r) SKIP {s} LIMIT {l} RETURN count(r) as count");
				let mut res = self.graph.execute(query(&stm)).await.unwrap();
				let count = res.next().await.unwrap().unwrap().get("count").unwrap();
				Ok(count)
			}
		}
	}
}
