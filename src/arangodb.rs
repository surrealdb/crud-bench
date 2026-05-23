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
use arangors::client::ClientExt;
use arangors::document::Document;
use arangors::document::options::InsertOptions;
use arangors::document::options::RemoveOptions;
use arangors::{ClientError, Collection, Database, GenericConnection};
use async_trait::async_trait;
use http::HeaderMap;
use serde_json::{Value, json};
use std::convert::TryInto;
use std::hint::black_box;
use std::time::Duration;

pub const DEFAULT: &str = "http://127.0.0.1:8529";

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	DockerParams {
		image: "arangodb",
		pre_args: "--ulimit nofile=65536:65536 -p 127.0.0.1:8529:8529 -e ARANGO_NO_AUTH=1"
			.to_string(),
		post_args: match options.optimised {
			true => {
				let cache_gb = Config::new().cache_gb.max(1);
				let block_cache_bytes = cache_gb * 1024 * 1024 * 1024 / 2;
				let total_write_buffer_bytes = cache_gb * 1024 * 1024 * 1024 / 4;
				// With the HTTP/2 client we only open one TCP connection per
				// `-c` client and multiplex all `-t` worker tasks onto it as
				// independent streams, so the accept-queue burst that
				// previously required a much larger I/O thread pool is gone.
				// We still bump it modestly above the default of 1 to keep
				// the read/write loops from becoming a serialisation point.
				let io_threads = num_cpus::get().clamp(2, 4);
				format!(
					"--server.io-threads {io_threads} \
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

/// A drop-in replacement for `arangors::client::reqwest::ReqwestClient`
/// that builds the underlying `reqwest::Client` with HTTP/2 prior knowledge
/// enabled. ArangoDB 3.x detects the HTTP/2 connection preface on plain
/// TCP, so all `-t` worker tasks per benchmark client share a single TCP
/// connection and travel as independent HTTP/2 streams. This removes the
/// HTTP/1.1 "one TCP socket per in-flight request" amplification that
/// previously forced us to serialise calls behind a per-client mutex.
#[derive(Debug, Clone)]
pub(crate) struct Http2ReqwestClient {
	/// Underlying `reqwest` HTTP client, configured for HTTP/2 multiplexing.
	client: reqwest::Client,
	/// Default headers (e.g. authorization) merged into every request.
	headers: HeaderMap,
}

#[async_trait]
impl ClientExt for Http2ReqwestClient {
	/// Build a client that forces HTTP/2 prior knowledge on every request.
	fn new<U: Into<Option<HeaderMap>>>(headers: U) -> Result<Self, ClientError> {
		// Configure reqwest for HTTP/2 multiplexing
		let client = reqwest::Client::builder()
			.gzip(true)
			.http2_prior_knowledge()
			.http2_adaptive_window(true)
			.http2_keep_alive_interval(Duration::from_secs(15))
			.http2_keep_alive_while_idle(true)
			.pool_idle_timeout(None)
			.build()
			.map_err(|e| ClientError::HttpClient(format!("{:?}", e)))?;
		// Default headers applied to every outgoing request
		let headers = headers.into().unwrap_or_default();
		Ok(Self {
			client,
			headers,
		})
	}
	fn headers(&mut self) -> &mut HeaderMap {
		&mut self.headers
	}
	/// Forward a request through the underlying HTTP/2 client and rebuild
	/// the response in the form arangors expects.
	async fn request(
		&self,
		mut request: http::Request<String>,
	) -> Result<http::Response<String>, ClientError> {
		// Merge the connection-level default headers with the per-request
		// headers, giving precedence to whatever arangors set explicitly
		let req_headers = request.headers_mut();
		for (header, value) in self.headers.iter() {
			if !req_headers.contains_key(header) {
				req_headers.insert(header, value.clone());
			}
		}
		// Convert into a reqwest::Request and execute it
		let req = request.try_into().unwrap();
		let resp = self
			.client
			.execute(req)
			.await
			.map_err(|e| ClientError::HttpClient(format!("{:?}", e)))?;
		// Capture metadata before consuming the response body
		let status_code = resp.status();
		let headers = resp.headers().clone();
		let version = resp.version();
		let content = resp.text().await.map_err(|e| ClientError::HttpClient(format!("{:?}", e)))?;
		// Reassemble an http::Response with the original status/headers
		let mut build = http::Response::builder();
		for header in headers.iter() {
			build = build.header(header.0, header.1);
		}
		build
			.status(status_code)
			.version(version)
			.body(content)
			.map_err(|e| ClientError::HttpClient(format!("{:?}", e)))
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
		// No per-client mutex is needed: the `Http2ReqwestClient` opens a
		// single HTTP/2 connection per benchmark client and multiplexes
		// all `-t` worker tasks as independent streams. `Database` and
		// `Collection` are `Clone + Send + Sync` and share the same
		// underlying client, so concurrent calls fan out into concurrent
		// HTTP/2 streams without a TCP-open burst.
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
	connection: GenericConnection<Http2ReqwestClient>,
	database: Database<Http2ReqwestClient>,
	collection: Collection<Http2ReqwestClient>,
}

async fn create_arango_client(
	url: &str,
) -> Result<(
	GenericConnection<Http2ReqwestClient>,
	Database<Http2ReqwestClient>,
	Collection<Http2ReqwestClient>,
)> {
	// Create the connection to the database (HTTP/2 prior knowledge)
	let conn = GenericConnection::<Http2ReqwestClient>::establish_without_auth(url).await.unwrap();
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
				let res: Vec<Value> = self.database.aql_str(&stm).await.unwrap();
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
				let res: Vec<Value> = self.database.aql_str(&stm).await.unwrap();
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
				let res: Vec<Value> = self.database.aql_str(&stm).await.unwrap();
				let count = res.first().unwrap().as_i64().unwrap();
				Ok(count as usize)
			}
		}
	}
}
