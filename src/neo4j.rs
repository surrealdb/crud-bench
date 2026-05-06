#![cfg(feature = "neo4j")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::Neo4jDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::valueprovider::Columns;
use crate::{Benchmark, Index, KeyType, Projection, Scan};
use anyhow::{Result, anyhow, bail};
use flatten_json_object::{ArrayFormatting, Flattener};
use neo4rs::BoltType;
use neo4rs::ConfigBuilder;
use neo4rs::Graph;
use neo4rs::query;
use serde_json::{Value, json};
use std::hint::black_box;

pub const DEFAULT: &str = "127.0.0.1:7687";

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	DockerParams {
		image: "neo4j",
		pre_args: match options.sync {
			true => {
				// Neo4j does not have the ability to configure
				// per-transaction on-disk sync control, so the
				// closest option when sync is true, is to
				// checkpoint after every transaction, and to
				// checkpoint in the background every second
				"--ulimit nofile=65536:65536 -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 -e NEO4J_AUTH=none -e NEO4J_dbms_checkpoint_interval_time=1s -e NEO4J_dbms_checkpoint_interval_tx=1".to_string()
			}
			false => {
				// Neo4j does not have the ability to configure
				// per-transaction on-disk sync control, so the
				// closest option when sync is false, is to
				// checkpoint in the background every second,
				// and to checkpoint every 10,000 transactions
				"--ulimit nofile=65536:65536 -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 -e NEO4J_AUTH=none -e NEO4J_dbms_checkpoint_interval_time=1s -e NEO4J_dbms_checkpoint_interval_tx=10000".to_string()
			}
		},
		post_args: "".to_string(),
	}
}

pub(crate) struct Neo4jClientProvider {
	graph: Graph,
}

impl BenchmarkEngine<Neo4jClient> for Neo4jClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Get the custom endpoint if specified
		let url = options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned();
		// Create a new client with a connection pool.
		// The Neo4j client supports connection pooling
		// and the recommended advice is to use a single
		// graph connection and share that with all async
		// tasks. Therefore we create a single connection
		// pool and share it with all of the crud-bench
		// clients. The Neo4j driver correctly limits the
		// number of connections to the number specified
		// in the `max_connections` option.
		let config = ConfigBuilder::default()
			.uri(url)
			.db("neo4j")
			.user("neo4j")
			.password("neo4j")
			.fetch_size(500)
			.max_connections(options.clients as usize)
			.build()?;
		// Create the client
		Ok(Self {
			graph: Graph::connect(config).await?,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<Neo4jClient> {
		Ok(Neo4jClient {
			graph: self.graph.clone(),
		})
	}
}

pub(crate) struct Neo4jClient {
	graph: Graph,
}

impl BenchmarkClient for Neo4jClient {
	// The return type when reading a row
	type ReadRow = serde_json::Value;

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

	async fn read_u32(&self, key: u32) -> Result<Value> {
		self.read(key).await
	}

	async fn read_string(&self, key: String) -> Result<Value> {
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

	async fn build_index(&self, spec: &Index, name: &str) -> Result<()> {
		// Get the fields
		let fields = spec.fields.iter().map(|f| format!("r.{f}")).collect::<Vec<_>>().join(", ");
		// Check if an index type is specified
		let stmt = match &spec.index_type {
			Some(kind) if kind == "fulltext" => {
				format!("CREATE FULLTEXT INDEX {name} FOR (r:Record) ON EACH [{fields}]")
			}
			_ => {
				format!("CREATE INDEX {name} FOR (r:Record) ON ({fields})")
			}
		};
		// Create the index
		self.graph.execute(query(&stmt)).await?.next().await?;
		// Wait for the index to finish building in the background.
		// Neo4j indexes build asynchronously, so we need to wait
		// for the index to be fully online before proceeding.
		self.graph.execute(query("CALL db.awaitIndexes()")).await?.next().await?;
		// All ok
		Ok(())
	}

	async fn drop_index(&self, name: &str) -> Result<()> {
		let stmt = format!("DROP INDEX {name} IF EXISTS");
		self.graph.execute(query(&stmt)).await?.next().await?;
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		self.scan(scan, ctx).await
	}

	async fn scan_string(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		self.scan(scan, ctx).await
	}

	async fn batch_create_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, Value)> + Send,
	) -> Result<()> {
		let rows: Vec<Value> = key_vals
			.map(|(k, v)| Self::flattened_row_with_id(json!(k), v))
			.collect::<Result<Vec<_>>>()?;
		if rows.is_empty() {
			return Ok(());
		}
		let bolt =
			BoltType::try_from(Value::Array(rows)).map_err(|e| anyhow!("neo4j BoltType: {e}"))?;
		let mut res = self
			.graph
			.execute(
				query("UNWIND $rows AS row CREATE (r:Record) SET r += row").param("rows", bolt),
			)
			.await?;
		while res.next().await?.is_some() {}
		Ok(())
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, Value)> + Send,
	) -> Result<()> {
		let rows: Vec<Value> = key_vals
			.map(|(k, v)| Self::flattened_row_with_id(Value::String(k), v))
			.collect::<Result<Vec<_>>>()?;
		if rows.is_empty() {
			return Ok(());
		}
		let bolt =
			BoltType::try_from(Value::Array(rows)).map_err(|e| anyhow!("neo4j BoltType: {e}"))?;
		let mut res = self
			.graph
			.execute(
				query("UNWIND $rows AS row CREATE (r:Record) SET r += row").param("rows", bolt),
			)
			.await?;
		while res.next().await?.is_some() {}
		Ok(())
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		let ids: Vec<Value> = keys.map(|k| json!(k)).collect();
		if ids.is_empty() {
			return Ok(());
		}
		let bolt =
			BoltType::try_from(Value::Array(ids)).map_err(|e| anyhow!("neo4j BoltType: {e}"))?;
		let mut res = self
			.graph
			.execute(
				query("UNWIND $ids AS id MATCH (r:Record { id: id }) RETURN r").param("ids", bolt),
			)
			.await?;
		let mut n = 0;
		while res.next().await?.is_some() {
			n += 1;
		}
		assert!(n > 0);
		Ok(())
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		let ids: Vec<Value> = keys.map(Value::String).collect();
		if ids.is_empty() {
			return Ok(());
		}
		let bolt =
			BoltType::try_from(Value::Array(ids)).map_err(|e| anyhow!("neo4j BoltType: {e}"))?;
		let mut res = self
			.graph
			.execute(
				query("UNWIND $ids AS id MATCH (r:Record { id: id }) RETURN r").param("ids", bolt),
			)
			.await?;
		let mut n = 0;
		while res.next().await?.is_some() {
			n += 1;
		}
		assert!(n > 0);
		Ok(())
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, Value)> + Send,
	) -> Result<()> {
		let rows: Vec<Value> = key_vals
			.map(|(k, v)| Self::flattened_row_with_id(json!(k), v))
			.collect::<Result<Vec<_>>>()?;
		if rows.is_empty() {
			return Ok(());
		}
		let bolt =
			BoltType::try_from(Value::Array(rows)).map_err(|e| anyhow!("neo4j BoltType: {e}"))?;
		let mut res = self
			.graph
			.execute(
				query(
					"UNWIND $rows AS row MATCH (r:Record { id: row.id }) SET r += row RETURN r.id",
				)
				.param("rows", bolt),
			)
			.await?;
		while res.next().await?.is_some() {}
		Ok(())
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, Value)> + Send,
	) -> Result<()> {
		let rows: Vec<Value> = key_vals
			.map(|(k, v)| Self::flattened_row_with_id(Value::String(k), v))
			.collect::<Result<Vec<_>>>()?;
		if rows.is_empty() {
			return Ok(());
		}
		let bolt =
			BoltType::try_from(Value::Array(rows)).map_err(|e| anyhow!("neo4j BoltType: {e}"))?;
		let mut res = self
			.graph
			.execute(
				query(
					"UNWIND $rows AS row MATCH (r:Record { id: row.id }) SET r += row RETURN r.id",
				)
				.param("rows", bolt),
			)
			.await?;
		while res.next().await?.is_some() {}
		Ok(())
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		let ids: Vec<Value> = keys.map(|k| json!(k)).collect();
		if ids.is_empty() {
			return Ok(());
		}
		let bolt =
			BoltType::try_from(Value::Array(ids)).map_err(|e| anyhow!("neo4j BoltType: {e}"))?;
		let mut res = self
			.graph
			.execute(
				query("UNWIND $ids AS id MATCH (r:Record { id: id }) DETACH DELETE r")
					.param("ids", bolt),
			)
			.await?;
		while res.next().await?.is_some() {}
		Ok(())
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		let ids: Vec<Value> = keys.map(Value::String).collect();
		if ids.is_empty() {
			return Ok(());
		}
		let bolt =
			BoltType::try_from(Value::Array(ids)).map_err(|e| anyhow!("neo4j BoltType: {e}"))?;
		let mut res = self
			.graph
			.execute(
				query("UNWIND $ids AS id MATCH (r:Record { id: id }) DETACH DELETE r")
					.param("ids", bolt),
			)
			.await?;
		while res.next().await?.is_some() {}
		Ok(())
	}
}

impl Neo4jClient {
	fn flattened_row_with_id(id: Value, val: Value) -> Result<Value> {
		let val = Flattener::new()
			.set_key_separator("_")
			.set_array_formatting(ArrayFormatting::Surrounded {
				start: "_".to_string(),
				end: "".to_string(),
			})
			.set_preserve_empty_arrays(false)
			.set_preserve_empty_objects(false)
			.flatten(&val)?;
		let mut m = val.as_object().cloned().ok_or_else(|| anyhow!("expected JSON object"))?;
		m.insert("id".into(), id);
		Ok(Value::Object(m))
	}

	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: Into<BoltType> + Sync,
	{
		let fields = Neo4jDialect::create_clause(val)?;
		let stm = format!("CREATE (r:Record {{ id: $id, {fields} }}) RETURN r.id");
		let stm = query(&stm).param("id", key);
		let mut res = self.graph.execute(stm).await.unwrap();
		assert!(matches!(res.next().await, Ok(Some(_))));
		assert!(matches!(res.next().await, Ok(None)));
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<Value>
	where
		T: Into<BoltType> + Sync,
	{
		let stm = "MATCH (r:Record { id: $id }) RETURN r";
		let stm = query(stm).param("id", key);
		let mut res = self.graph.execute(stm).await.unwrap();
		let row = black_box(res.next().await).unwrap().unwrap();
		let val: Value = row.get("r").unwrap();
		assert!(matches!(res.next().await, Ok(None)));
		Ok(black_box(val))
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: Into<BoltType> + Sync,
	{
		let fields = Neo4jDialect::update_clause(val)?;
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

	async fn scan(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		// Neo4j requires a full-text index to exist
		if ctx == ScanContext::WithoutIndex
			&& let Some(index) = &scan.with_index
			&& let Some(kind) = &index.index_type
			&& kind == "fulltext"
		{
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Ordered full-text scans are not supported
		if scan.order_by.is_some()
			&& let Some(index) = &scan.with_index
			&& let Some(kind) = &index.index_type
			&& kind == "fulltext"
		{
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.map(|s| format!("SKIP {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = Neo4jDialect::filter_clause(scan)?;
		let o = Neo4jDialect::order_by_clause(scan)?;
		let p = scan.projection()?;
		let n = &scan.id;
		// Check if this is a fulltext scan
		let fts = scan
			.with_index
			.as_ref()
			.and_then(|idx| idx.index_type.as_ref())
			.map(|t| t == "fulltext")
			.unwrap_or(false);
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = match fts {
					true => format!(
						"CALL db.index.fulltext.queryNodes('{n}', '{c}') YIELD node as r WITH r {o} {s} {l} RETURN r.id"
					),
					false => format!("MATCH (r) {c} WITH r {o} {s} {l} RETURN r.id"),
				};
				let mut res = self.graph.execute(query(&stm)).await.unwrap();
				let mut count = 0;
				while let Ok(Some(v)) = res.next().await {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let stm = match fts {
					true => format!(
						"CALL db.index.fulltext.queryNodes('{n}', '{c}') YIELD node as r WITH r {o} {s} {l} RETURN r"
					),
					false => format!("MATCH (r) {c} WITH r {o} {s} {l} RETURN r"),
				};
				let mut res = self.graph.execute(query(&stm)).await.unwrap();
				let mut count = 0;
				while let Ok(Some(v)) = res.next().await {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let stm = match fts {
					true => format!(
						"CALL db.index.fulltext.queryNodes('{n}', '{c}') YIELD node as r WITH r {s} {l} RETURN count(r) as count"
					),
					false => format!("MATCH (r) {c} WITH r {s} {l} RETURN count(r) as count"),
				};
				let mut res = self.graph.execute(query(&stm)).await.unwrap();
				let count = res.next().await.unwrap().unwrap().get("count").unwrap();
				Ok(count)
			}
		}
	}
}
