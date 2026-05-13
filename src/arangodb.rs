#![cfg(feature = "arangodb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::ArangoDBDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use arangors::aql::AqlQuery;
use arangors::client::reqwest::ReqwestClient;
use arangors::document::Document;
use arangors::document::options::InsertOptions;
use arangors::document::options::RemoveOptions;
use arangors::{Collection, Connection, Database, GenericConnection};
use serde_json::{Value, json};
use std::hint::black_box;
use std::time::Duration;

pub const DEFAULT: &str = "http://127.0.0.1:8529";

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	DockerParams {
		image: "arangodb",
		pre_args: "--ulimit nofile=65536:65536 -p 127.0.0.1:8529:8529 -e ARANGO_NO_AUTH=1".to_string(),
		post_args: match options.optimised {
			true => {
				let cache_gb = Config::new().cache_gb.max(1);
				let block_cache_bytes = cache_gb * 1024 * 1024 * 1024 / 2;
				let total_write_buffer_bytes = cache_gb * 1024 * 1024 * 1024 / 4;
				format!(
					"--server.scheduler-queue-size 8192 \
					 --server.prio1-size 8192 \
					 --server.prio2-size 8192 \
					 --server.maximal-queue-size 8192 \
					 --rocksdb.block-cache-size {block_cache_bytes} \
					 --rocksdb.total-write-buffer-size {total_write_buffer_bytes} \
					 --rocksdb.enable-pipelined-write true \
					 --rocksdb.max-background-jobs 16 \
					 --rocksdb.max-write-buffer-number 8 \
					 --cache.size {block_cache_bytes}"
				)
			}
			false => "".to_string(),
		},
	}
}

pub(crate) struct ArangoDBClientProvider {
	sync: bool,
	key: KeyType,
	url: String,
}

impl BenchmarkEngine<ArangoDBClient> for ArangoDBClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		Ok(Self {
			sync: options.sync,
			key: kt,
			url: options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned(),
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<ArangoDBClient> {
		let (conn, db, co) = create_arango_client(&self.url).await?;
		Ok(ArangoDBClient {
			sync: self.sync,
			keytype: self.key,
			connection: conn,
			database: db,
			collection: co,
		})
	}
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		Some(Duration::from_secs(15))
	}
}

pub(crate) struct ArangoDBClient {
	sync: bool,
	keytype: KeyType,
	connection: GenericConnection<ReqwestClient>,
	database: Database<ReqwestClient>,
	collection: Collection<ReqwestClient>,
}

async fn create_arango_client(
	url: &str,
) -> Result<(GenericConnection<ReqwestClient>, Database<ReqwestClient>, Collection<ReqwestClient>)>
{
	// Create the connection to the database
	let conn = Connection::establish_without_auth(url).await.unwrap();
	// Create the benchmarking database
	let db = match conn.create_database("crud-bench").await {
		Err(_) => conn.db("crud-bench").await.unwrap(),
		Ok(db) => db,
	};
	// Create the becnhmark record collection
	let co = match db.create_collection("record").await {
		Err(_) => db.collection("record").await.unwrap(),
		Ok(db) => db,
	};
	Ok((conn, db, co))
}

impl BenchmarkClient for ArangoDBClient {
	// The return type when reading a row
	type ReadRow = BenchValue;

	async fn startup(&self) -> Result<()> {
		// Ensure we drop the database first.
		// We can drop the database initially
		// because the other clients will be
		// created subsequently, and will then
		// create the database as necessary.
		self.connection.drop_database("crud-bench").await?;
		// Everything ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.create(key.to_string(), val).await,
		}
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.create(key, val).await,
		}
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.read(key.to_string()).await,
		}
	}

	async fn read_string(&self, key: String) -> Result<BenchValue> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.read(key).await,
		}
	}

	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.update(key.to_string(), val).await,
		}
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.update(key, val).await,
		}
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.delete(key.to_string()).await,
		}
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.delete(key).await,
		}
	}

	async fn scan_u32(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.scan(scan).await,
		}
	}

	async fn scan_string(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.scan(scan).await,
		}
	}

	async fn batch_create_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => {
				let pairs = key_vals.map(|(k, v)| (k.to_string(), v)).collect::<Vec<_>>();
				self.batch_create_pairs(pairs).await
			}
		}
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.batch_create_pairs(key_vals.collect()).await,
		}
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => {
				let ks = keys.map(|k| k.to_string()).collect::<Vec<_>>();
				self.batch_read_keys(ks).await
			}
		}
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.batch_read_keys(keys.collect()).await,
		}
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => {
				let pairs = key_vals.map(|(k, v)| (k.to_string(), v)).collect::<Vec<_>>();
				self.batch_update_pairs(pairs).await
			}
		}
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.batch_update_pairs(key_vals.collect()).await,
		}
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => {
				let ks = keys.map(|k| k.to_string()).collect::<Vec<_>>();
				self.batch_delete_keys(ks).await
			}
		}
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		match self.keytype {
			KeyType::String506 => bail!(NOT_SUPPORTED_ERROR),
			_ => self.batch_delete_keys(keys.collect()).await,
		}
	}
}

impl ArangoDBClient {
	async fn batch_create_pairs(&self, pairs: Vec<(String, BenchValue)>) -> Result<()> {
		if pairs.is_empty() {
			return Ok(());
		}
		let docs: Vec<Value> =
			pairs.into_iter().map(|(k, v)| Self::to_doc(k, v)).collect::<Result<Vec<_>>>()?;
		let aql = AqlQuery::builder()
			.query(
				"FOR doc IN @docs INSERT doc INTO record OPTIONS { waitForSync: @sync } RETURN 1",
			)
			.bind_var("docs", Value::Array(docs))
			.bind_var("sync", json!(self.sync))
			.build();
		let _: Vec<Value> = self.database.aql_query(aql).await?;
		Ok(())
	}

	async fn batch_read_keys(&self, keys: Vec<String>) -> Result<()> {
		if keys.is_empty() {
			return Ok(());
		}
		let aql = AqlQuery::builder()
			.query("FOR k IN @keys LET d = DOCUMENT('record', k) FILTER d != null RETURN d")
			.bind_var("keys", Value::Array(keys.into_iter().map(Value::String).collect()))
			.build();
		let res: Vec<Value> = self.database.aql_query(aql).await?;
		assert!(!res.is_empty());
		Ok(())
	}

	async fn batch_update_pairs(&self, pairs: Vec<(String, BenchValue)>) -> Result<()> {
		if pairs.is_empty() {
			return Ok(());
		}
		let docs: Vec<Value> =
			pairs.into_iter().map(|(k, v)| Self::to_doc(k, v)).collect::<Result<Vec<_>>>()?;
		let aql = AqlQuery::builder()
			.query(
				r#"FOR doc IN @docs INSERT doc INTO record OPTIONS { overwriteMode: "replace", waitForSync: @sync } RETURN 1"#,
			)
			.bind_var("docs", Value::Array(docs))
			.bind_var("sync", json!(self.sync))
			.build();
		let _: Vec<Value> = self.database.aql_query(aql).await?;
		Ok(())
	}

	async fn batch_delete_keys(&self, keys: Vec<String>) -> Result<()> {
		if keys.is_empty() {
			return Ok(());
		}
		let aql = AqlQuery::builder()
			.query(
				"FOR k IN @keys REMOVE {_key: k} IN record OPTIONS { waitForSync: @sync } RETURN 1",
			)
			.bind_var("keys", Value::Array(keys.into_iter().map(Value::String).collect()))
			.bind_var("sync", json!(self.sync))
			.build();
		let _: Vec<Value> = self.database.aql_query(aql).await?;
		Ok(())
	}

	fn to_doc(key: String, val: BenchValue) -> Result<Value> {
		let mut json = val.to_json();
		let obj = json
			.as_object_mut()
			.ok_or_else(|| anyhow::anyhow!("expected object payload for arangodb row"))?;
		obj.insert("_key".to_string(), Value::String(key));
		Ok(json)
	}

	async fn create(&self, key: String, val: BenchValue) -> Result<()> {
		let json = Self::to_doc(key, val)?;
		let opt = InsertOptions::builder()
			.wait_for_sync(self.sync)
			.return_new(false)
			.overwrite(false)
			.build();
		self.collection.create_document(json, opt).await?;
		Ok(())
	}

	async fn read(&self, key: String) -> Result<BenchValue> {
		let doc: Document<Value> = self.collection.document(&key).await?;
		assert!(doc.document.is_object());
		assert_eq!(doc.document.get("_key").unwrap().as_str().unwrap(), key);
		Ok(black_box(BenchValue::from(&doc.document)))
	}

	async fn update(&self, key: String, val: BenchValue) -> Result<()> {
		let json = Self::to_doc(key, val)?;
		let opt = InsertOptions::builder()
			.wait_for_sync(self.sync)
			.return_new(false)
			.overwrite(true)
			.build();
		self.collection.create_document(json, opt).await?;
		Ok(())
	}

	async fn delete(&self, key: String) -> Result<()> {
		let opt = RemoveOptions::builder().wait_for_sync(self.sync).build();
		self.collection.remove_document::<Value>(&key, opt, None).await?;
		Ok(())
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let l = match (scan.start, scan.limit) {
			(Some(s), Some(l)) => format!("LIMIT {s}, {l}"),
			(Some(s), None) => format!("LIMIT {s}, 1000000000"),
			(None, Some(l)) => format!("LIMIT {l}"),
			(None, None) => "".to_string(),
		};
		let c = ArangoDBDialect::filter_clause(scan)?;
		let o = ArangoDBDialect::sort_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("FOR r IN record {c} {o} {l} RETURN {{ _id: r._id }}");
				let res: Vec<Value> = { self.database.aql_str(&stm).await.unwrap() };
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let stm = format!("FOR r IN record {c} {o} {l} RETURN r");
				let res: Vec<Value> = { self.database.aql_str(&stm).await.unwrap() };
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let stm =
					format!("FOR r IN record {c} {l} COLLECT WITH COUNT INTO count RETURN count");
				let res: Vec<Value> = { self.database.aql_str(&stm).await.unwrap() };
				let count = res.first().unwrap().as_i64().unwrap();
				Ok(count as usize)
			}
		}
	}
}
