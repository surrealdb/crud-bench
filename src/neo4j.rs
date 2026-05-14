#![cfg(feature = "neo4j")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::Neo4jDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, Index, KeyType, Projection, Scan};
use anyhow::{Result, anyhow, bail};
use neo4rs::BoltList;
use neo4rs::BoltMap;
use neo4rs::BoltNode;
use neo4rs::BoltNull;
use neo4rs::BoltString;
use neo4rs::BoltType;
use neo4rs::ConfigBuilder;
use neo4rs::Graph;
use neo4rs::query;
use std::hint::black_box;

/// Key separator matching the legacy `flatten_json_object` neo4j config.
const PROP_KEY_SEPARATOR: &str = "_";
/// Prefix/suffix wrapping array indexes (`tags_0` with start `"_"`, end `""`).
const ARRAY_INDEX_START: &str = "_";
const ARRAY_INDEX_END: &str = "";

pub const DEFAULT: &str = "127.0.0.1:7687";

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	// Per-tx fsync control is not exposed; checkpoint cadence is the closest knob.
	let checkpoint = match options.sync {
		true => "-e NEO4J_dbms_checkpoint_interval_time=1s -e NEO4J_dbms_checkpoint_interval_tx=1",
		false => {
			"-e NEO4J_dbms_checkpoint_interval_time=1s -e NEO4J_dbms_checkpoint_interval_tx=10000"
		}
	};
	// JVM heap and pagecache default to a few hundred MB; on a heavy benchmark this
	// triggers GC thrash long before disk I/O. When optimised, split the recommended
	// cache budget between heap (1/3) and pagecache (2/3).
	let memory = match options.optimised {
		true => {
			let cache_gb = Config::new().cache_gb.max(2);
			let heap_gb = (cache_gb / 3).max(1);
			let pagecache_gb = cache_gb - heap_gb;
			format!(
				"-e NEO4J_server_memory_heap_initial__size={heap_gb}g \
				 -e NEO4J_server_memory_heap_max__size={heap_gb}g \
				 -e NEO4J_server_memory_pagecache_size={pagecache_gb}g \
				 -e NEO4J_db_memory_transaction_total_max=0 \
				 -e NEO4J_db_memory_transaction_max=0"
			)
		}
		false => String::new(),
	};
	DockerParams {
		image: "neo4j",
		pre_args: format!(
			"--ulimit nofile=65536:65536 \
			 -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 \
			 -e NEO4J_AUTH=none {checkpoint} {memory}"
		),
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
	type ReadRow = BenchValue;

	async fn startup(&self) -> Result<()> {
		let stm = "CREATE INDEX FOR (r:Record) ON (r.id);";
		self.graph.execute(query(stm)).await?.next().await.ok();
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.create(key.into(), val).await
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.create(BoltType::from(key), val).await
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		self.read(key).await
	}

	async fn read_string(&self, key: String) -> Result<BenchValue> {
		self.read(key).await
	}

	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.update(key, val).await
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.update(key, val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key).await
	}

	async fn build_index(&self, spec: &Index, name: &str) -> Result<()> {
		// Reject wildcard array specs (`tags.*`). Cypher has no btree equivalent
		// for array-element indexing, and records are flattened so there is no
		// single property to index. Other dialects work around this with JSON
		// or multi-valued indexes; Neo4j has no such facility, so bail and let
		// the bench framework skip this scan.
		if spec.fields.iter().any(|f| f.contains(".*")) {
			bail!(NOT_SUPPORTED_ERROR);
		}
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
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals).await
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals).await
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_read(keys).await
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_read(keys).await
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals).await
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_delete(keys).await
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_delete(keys).await
	}
}

/// Maps integer / string benchmark keys onto Bolt `$id` parameters.
trait IntoNeo4jBoltId {
	fn into_neo4j_id_bolt(self) -> BoltType;
}

impl IntoNeo4jBoltId for u32 {
	fn into_neo4j_id_bolt(self) -> BoltType {
		BoltType::from(i64::from(self))
	}
}

impl IntoNeo4jBoltId for String {
	fn into_neo4j_id_bolt(self) -> BoltType {
		BoltType::from(self)
	}
}

/// Leaf `BenchValue` → Bolt (same Bolt wire shapes as the previous encode path produced through
/// `BenchValue::to_json` and neo4rs).
fn bench_leaf_to_bolt(v: &BenchValue) -> Result<BoltType> {
	Ok(match v {
		BenchValue::Null => BoltType::Null(BoltNull),
		BenchValue::Bool(b) => BoltType::from(*b),
		BenchValue::Int(i) => BoltType::from(*i),
		BenchValue::UInt(u) => {
			BoltType::try_from(*u).map_err(|_| anyhow!("Neo4j integer overflows i64: {u}"))?
		}
		BenchValue::Float(f) => {
			if !f.is_finite() {
				BoltType::Null(BoltNull)
			} else {
				BoltType::from(*f)
			}
		}
		BenchValue::Decimal(d) => BoltType::from(d.to_string()),
		BenchValue::String(s) => BoltType::from(s.as_str()),
		BenchValue::Bytes(b) => BoltType::from(b.clone()),
		BenchValue::Uuid(u) => BoltType::from(u.to_string()),
		BenchValue::DateTime(dt) => BoltType::from(dt.to_rfc3339()),
		BenchValue::Array(_) | BenchValue::Object(_) => {
			bail!("internal: expected scalar leaf BenchValue")
		}
	})
}

fn try_insert_prop(map: &mut BoltMap, key: String, v: BoltType) -> Result<()> {
	let bk = BoltString::from(key.as_str());
	if map.value.contains_key(&bk) {
		bail!("flattened Neo4j property key `{}` collision", bk.value);
	}
	map.put(bk, v);
	Ok(())
}

/// Flattens a top-level bench object matching `flatten_json_object` neo4j settings (`_`,
/// `_index` array syntax, omit empty composites).
fn flatten_dispatch_value(
	v: &BenchValue,
	parent_key: String,
	depth: u32,
	out: &mut BoltMap,
) -> Result<()> {
	match v {
		BenchValue::Object(m) => {
			if !m.is_empty() {
				flatten_object_into(m.as_slice(), parent_key.as_str(), depth, out)?;
			}
			Ok(())
		}
		BenchValue::Array(a) => {
			if !a.is_empty() {
				flatten_array_into(a.as_slice(), parent_key.as_str(), depth, out)?;
			}
			Ok(())
		}
		_ => try_insert_prop(out, parent_key, bench_leaf_to_bolt(v)?),
	}
}

fn flatten_object_into(
	fields: &[(String, BenchValue)],
	parent_prefix: &str,
	depth: u32,
	out: &mut BoltMap,
) -> Result<()> {
	for (k, v) in fields {
		let pk = if depth > 0 {
			format!("{parent_prefix}{PROP_KEY_SEPARATOR}{k}")
		} else {
			k.clone()
		};
		flatten_dispatch_value(v, pk, depth + 1, out)?;
	}
	Ok(())
}

fn flatten_array_into(
	elems: &[BenchValue],
	parent_prefix: &str,
	depth: u32,
	out: &mut BoltMap,
) -> Result<()> {
	for (i, el) in elems.iter().enumerate() {
		let idx_key = format!("{parent_prefix}{ARRAY_INDEX_START}{i}{ARRAY_INDEX_END}");
		flatten_dispatch_value(el, idx_key, depth + 1, out)?;
	}
	Ok(())
}

fn flatten_record_root(fields: &[(String, BenchValue)], out: &mut BoltMap) -> Result<()> {
	if fields.is_empty() {
		return Ok(());
	}
	flatten_object_into(fields, "", 0, out)
}

/// Full row map for CREATE / batch rows: flattened payload + Neo4j identity property.
fn bolt_props_with_id(val: BenchValue, id_bt: BoltType) -> Result<BoltMap> {
	let mut map = BoltMap::new();
	if let BenchValue::Object(fields) = &val {
		flatten_record_root(fields, &mut map)?;
		map.put(BoltString::from("id"), id_bt);
		Ok(map)
	} else {
		bail!("expected BenchValue object for Neo4j row payload")
	}
}

/// Props for `SET r += $props`: flattened updates only (id matched separately).
fn bolt_update_props_only(val: BenchValue) -> Result<BoltMap> {
	let mut map = BoltMap::new();
	if let BenchValue::Object(fields) = &val {
		flatten_record_root(fields, &mut map)?;
		Ok(map)
	} else {
		bail!("expected BenchValue object for Neo4j update payload")
	}
}

/// Bolt value for one Neo4j **property**. Graph properties are not arbitrary nested maps here;
/// writes use flattened scalars only (see `flatten_record_root`).
fn neo4j_prop_value_to_bench(bt: BoltType) -> Result<BenchValue> {
	match bt {
		BoltType::Null(_) => Ok(BenchValue::Null),
		BoltType::Boolean(b) => Ok(BenchValue::Bool(b.value)),
		BoltType::Integer(i) => Ok(BenchValue::Int(i.value)),
		BoltType::Float(f) => Ok(BenchValue::Float(f.value)),
		BoltType::String(s) => Ok(BenchValue::String(s.value)),
		BoltType::Bytes(b) => Ok(BenchValue::Bytes(b.value.to_vec())),
		BoltType::List(l) => {
			let mut out = Vec::with_capacity(l.value.len());
			for el in l.value.into_iter() {
				out.push(neo4j_prop_value_to_bench(el)?);
			}
			Ok(BenchValue::Array(out))
		}
		BoltType::Map(_) => bail!(
			"Neo4j does not expose nested map-valued graph properties here; flatten when writing Record nodes"
		),
		BoltType::Node(_)
		| BoltType::Relation(_)
		| BoltType::UnboundedRelation(_)
		| BoltType::Point2D(_)
		| BoltType::Point3D(_)
		| BoltType::Path(_)
		| BoltType::Duration(_)
		| BoltType::Date(_)
		| BoltType::Time(_)
		| BoltType::LocalTime(_)
		| BoltType::DateTime(_)
		| BoltType::LocalDateTime(_)
		| BoltType::DateTimeZoneId(_) => {
			bail!("Neo4j read returned unsupported Bolt type for benchmark payloads")
		}
	}
}

/// Turn the node's **flat** `properties` Bolt map into [`BenchValue::Object`].
fn bolt_map_payload_to_object(m: BoltMap) -> Result<BenchValue> {
	let pairs: Vec<(String, BenchValue)> = m
		.value
		.into_iter()
		.map(|(k, bt)| neo4j_prop_value_to_bench(bt).map(|bv| (k.value, bv)))
		.collect::<Result<_>>()?;
	Ok(BenchValue::Object(pairs))
}

fn bolt_list_of_maps(rows: Vec<BoltMap>) -> BoltType {
	BoltType::List(BoltList::from(rows.into_iter().map(BoltType::Map).collect::<Vec<BoltType>>()))
}

fn bolt_list_ids(ids: Vec<BoltType>) -> BoltType {
	BoltType::List(BoltList::from(ids))
}

impl Neo4jClient {
	async fn create(&self, id_bt: BoltType, val: BenchValue) -> Result<()> {
		let props_map = bolt_props_with_id(val, id_bt)?;
		let bolt = BoltType::Map(props_map);
		// Cypher must RETURN a row so the result stream is non-empty;
		// without RETURN the stream yields zero records even on success
		let mut res = self
			.graph
			.execute(query("CREATE (r:Record) SET r = $props RETURN r.id").param("props", bolt))
			.await?;
		assert!(matches!(res.next().await, Ok(Some(_))));
		assert!(matches!(res.next().await, Ok(None)));
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<BenchValue>
	where
		T: Into<BoltType> + Sync,
	{
		let stm = "MATCH (r:Record { id: $id }) RETURN r";
		let stm = query(stm).param("id", key);
		let mut res = self.graph.execute(stm).await.unwrap();
		let row = black_box(res.next().await).unwrap().unwrap();
		let node: BoltNode = row.get("r").unwrap();
		let val = bolt_map_payload_to_object(node.properties)?;
		assert!(matches!(res.next().await, Ok(None)));
		Ok(black_box(val))
	}

	async fn update<T>(&self, id: T, val: BenchValue) -> Result<()>
	where
		T: Into<BoltType> + Sync,
	{
		let id: BoltType = id.into();
		let props_bt = BoltType::Map(bolt_update_props_only(val)?);
		let mut res = self
			.graph
			.execute(
				query("MATCH (r:Record { id: $id }) SET r += $props RETURN r.id")
					.param("id", id)
					.param("props", props_bt),
			)
			.await?;
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
				let count: i64 = res.next().await.unwrap().unwrap().get("count").unwrap();
				assert!(usize::try_from(count).is_ok());
				Ok(count as usize)
			}
		}
	}

	async fn batch_create<K>(
		&self,
		key_vals: impl Iterator<Item = (K, BenchValue)> + Send,
	) -> Result<()>
	where
		K: IntoNeo4jBoltId,
	{
		// Construct the records
		let rows: Vec<BoltMap> = key_vals
			.map(|(k, v)| bolt_props_with_id(v, k.into_neo4j_id_bolt()))
			.collect::<Result<Vec<_>>>()?;
		if rows.is_empty() {
			return Ok(());
		}
		let bolt = bolt_list_of_maps(rows);
		// Construct the Cypher query
		let cypher = "UNWIND $rows AS row CREATE (r:Record) SET r += row";
		// Execute the Cypher query
		let mut res = self.graph.execute(query(cypher).param("rows", bolt)).await?;
		while res.next().await?.is_some() {}
		// All ok
		Ok(())
	}

	async fn batch_read<K>(&self, keys: impl Iterator<Item = K> + Send) -> Result<()>
	where
		K: IntoNeo4jBoltId,
	{
		// Construct the node ids
		let ids: Vec<BoltType> = keys.map(|k| k.into_neo4j_id_bolt()).collect();
		if ids.is_empty() {
			return Ok(());
		}
		let bolt = bolt_list_ids(ids);
		// Construct the Cypher query
		let cypher = "UNWIND $ids AS id MATCH (r:Record { id: id }) RETURN r";
		// Execute the Cypher query
		let mut res = self.graph.execute(query(cypher).param("ids", bolt)).await?;
		let mut n = 0;
		while res.next().await?.is_some() {
			n += 1;
		}
		// Check the response
		assert!(n > 0);
		// All ok
		Ok(())
	}

	async fn batch_update<K>(
		&self,
		key_vals: impl Iterator<Item = (K, BenchValue)> + Send,
	) -> Result<()>
	where
		K: IntoNeo4jBoltId,
	{
		// Construct the records
		let rows: Vec<BoltMap> = key_vals
			.map(|(k, v)| bolt_props_with_id(v, k.into_neo4j_id_bolt()))
			.collect::<Result<Vec<_>>>()?;
		if rows.is_empty() {
			return Ok(());
		}
		let bolt = bolt_list_of_maps(rows);
		// Construct the Cypher query
		let cypher = "UNWIND $rows AS row MATCH (r:Record { id: row.id }) SET r += row RETURN r.id";
		// Execute the Cypher query
		let mut res = self.graph.execute(query(cypher).param("rows", bolt)).await?;
		while res.next().await?.is_some() {}
		Ok(())
	}

	async fn batch_delete<K>(&self, keys: impl Iterator<Item = K> + Send) -> Result<()>
	where
		K: IntoNeo4jBoltId,
	{
		// Construct the node ids
		let ids: Vec<BoltType> = keys.map(|k| k.into_neo4j_id_bolt()).collect();
		if ids.is_empty() {
			return Ok(());
		}
		let bolt = bolt_list_ids(ids);
		// Construct the Cypher query
		let cypher = "UNWIND $ids AS id MATCH (r:Record { id: id }) DETACH DELETE r";
		// Execute the Cypher query
		let mut res = self.graph.execute(query(cypher).param("ids", bolt)).await?;
		while res.next().await?.is_some() {}
		// All ok
		Ok(())
	}
}
