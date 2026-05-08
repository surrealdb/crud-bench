#![cfg(feature = "mongodb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::MongoDBDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, Index, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use futures::{StreamExt, TryStreamExt};
use mongodb::IndexModel;
use mongodb::Namespace;
use mongodb::bson::{Bson, Document, doc, spec::BinarySubtype};
use mongodb::options::ClientOptions;
use mongodb::options::DatabaseOptions;
use mongodb::options::IndexOptions;
use mongodb::options::ReadConcern;
use mongodb::options::{Acknowledgment, ReplaceOneModel, WriteConcern, WriteModel};
use mongodb::{Client, Collection, Cursor, Database};
use std::hint::black_box;
use std::str::FromStr;
use std::time::Duration;

pub const DEFAULT: &str = "mongodb://root:root@127.0.0.1:27017";

/// Native MongoDB row for single-row reads; converts to [`BenchValue`] only via [`From`]/[`Into`].
pub(crate) struct Row(pub Document);

impl From<Row> for BenchValue {
	fn from(row: Row) -> Self {
		bson_to_bench_value(&Bson::Document(row.0))
	}
}

/// Recursively convert a [`Bson`] tree to a [`BenchValue`], preserving native
/// MongoDB types (UUID and binary subtypes, Decimal128, datetime).
fn bson_to_bench_value(b: &Bson) -> BenchValue {
	match b {
		Bson::Null => BenchValue::Null,
		Bson::Boolean(b) => BenchValue::Bool(*b),
		Bson::Int32(i) => BenchValue::Int(*i as i64),
		Bson::Int64(i) => BenchValue::Int(*i),
		Bson::Double(f) => BenchValue::Float(*f),
		Bson::String(s) => BenchValue::String(s.clone()),
		Bson::Decimal128(d) => match rust_decimal::Decimal::from_str(&d.to_string()) {
			Ok(dec) => BenchValue::Decimal(dec),
			Err(_) => BenchValue::String(d.to_string()),
		},
		Bson::DateTime(dt) => {
			match chrono::DateTime::<chrono::Utc>::from_timestamp_millis(dt.timestamp_millis()) {
				Some(dt) => BenchValue::DateTime(dt),
				None => BenchValue::Null,
			}
		}
		Bson::Binary(bin) => match bin.subtype {
			BinarySubtype::Uuid | BinarySubtype::UuidOld => {
				match uuid::Uuid::from_slice(bin.bytes.as_slice()) {
					Ok(u) => BenchValue::Uuid(u),
					Err(_) => BenchValue::Bytes(bin.bytes.clone()),
				}
			}
			_ => BenchValue::Bytes(bin.bytes.clone()),
		},
		Bson::Array(a) => BenchValue::Array(a.iter().map(bson_to_bench_value).collect()),
		Bson::Document(d) => {
			BenchValue::Object(d.iter().map(|(k, v)| (k.clone(), bson_to_bench_value(v))).collect())
		}
		Bson::ObjectId(oid) => BenchValue::String(oid.to_hex()),
		Bson::Symbol(s) => BenchValue::String(s.clone()),
		Bson::JavaScriptCode(s) => BenchValue::String(s.clone()),
		Bson::JavaScriptCodeWithScope(c) => BenchValue::String(c.code.clone()),
		Bson::Timestamp(ts) => BenchValue::Int(ts.time as i64),
		Bson::RegularExpression(r) => BenchValue::String(r.pattern.clone()),
		Bson::Undefined | Bson::MaxKey | Bson::MinKey | Bson::DbPointer(_) => BenchValue::Null,
	}
}

/// Recursively convert a [`BenchValue`] into a [`Bson`] preserving native
/// MongoDB types where the destination supports them.
fn bench_to_bson(v: &BenchValue) -> Bson {
	match v {
		BenchValue::Null => Bson::Null,
		BenchValue::Bool(b) => Bson::Boolean(*b),
		BenchValue::Int(i) => Bson::Int64(*i),
		BenchValue::UInt(u) => match i64::try_from(*u) {
			Ok(i) => Bson::Int64(i),
			Err(_) => Bson::String(u.to_string()),
		},
		BenchValue::Float(f) => Bson::Double(*f),
		BenchValue::Decimal(d) => match mongodb::bson::Decimal128::from_str(&d.to_string()) {
			Ok(dec) => Bson::Decimal128(dec),
			Err(_) => Bson::String(d.to_string()),
		},
		BenchValue::String(s) => Bson::String(s.clone()),
		BenchValue::Bytes(b) => Bson::Binary(mongodb::bson::Binary {
			subtype: BinarySubtype::Generic,
			bytes: b.clone(),
		}),
		BenchValue::Uuid(u) => Bson::Binary(mongodb::bson::Binary {
			subtype: BinarySubtype::Uuid,
			bytes: u.as_bytes().to_vec(),
		}),
		BenchValue::DateTime(dt) => {
			Bson::DateTime(mongodb::bson::DateTime::from_millis(dt.timestamp_millis()))
		}
		BenchValue::Array(a) => Bson::Array(a.iter().map(bench_to_bson).collect()),
		BenchValue::Object(o) => {
			let mut doc = Document::new();
			for (k, v) in o {
				doc.insert(k.clone(), bench_to_bson(v));
			}
			Bson::Document(doc)
		}
	}
}

/// Calculate MongoDB specific memory allocation
fn calculate_mongodb_memory() -> u64 {
	// Load the system memory
	let memory = Config::new();
	// Use ~80% of recommended cache allocation
	(memory.cache_gb * 4 / 5).max(1)
}

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	// Calculate memory allocation
	let cache_gb = calculate_mongodb_memory();
	// Return Docker parameters
	DockerParams {
		image: "mongo",
		pre_args: "--ulimit nofile=65536:65536 -p 127.0.0.1:27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=root".to_string(),
		post_args: match options.optimised {
			// Optimised configuration
			true => format!("mongod --wiredTigerCacheSizeGB {cache_gb}"),
			// Default configuration
			false => "".to_string(),
		},
	}
}

pub(crate) struct MongoDBClientProvider {
	sync: bool,
	client: Client,
}

impl BenchmarkEngine<MongoDBClient> for MongoDBClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Get the custom endpoint if specified
		let url = options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned();
		// Create a new client with a connection pool.
		// The MongoDB client does not correctly limit
		// the number of connections in the connection
		// pool. Therefore we create a single connection
		// pool and share it with all of the crud-bench
		// clients. This follows the recommended advice
		// for using the MongoDB driver. Note that this
		// still creates 2 more connections than has
		// been specified in the `max_pool_size` option.
		let mut opts = ClientOptions::parse(url).await?;
		opts.max_pool_size = Some(options.clients);
		opts.min_pool_size = None;
		// Set server selection timeout to 60 seconds (default 30s)
		opts.server_selection_timeout = Some(Duration::from_secs(60));
		// Set server connect timeout to 30 seconds (default 10s)
		opts.connect_timeout = Some(Duration::from_secs(30));
		// Reduce monitoring heartbeats for batch operations (default 10s)
		opts.heartbeat_freq = Some(Duration::from_secs(30));
		// Create the client provider
		Ok(Self {
			sync: options.sync,
			client: Client::with_options(opts)?,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<MongoDBClient> {
		let db = self.client.database_with_options(
			"crud-bench",
			DatabaseOptions::builder()
				// Configure the write concern options
				.write_concern(
					// Configure the write options
					WriteConcern::builder()
						// Ensure that all writes are written,
						// replicated, and acknowledged by the
						// majority of nodes in the cluster.
						.w(Acknowledgment::Majority)
						// Configure journal durability based on sync setting.
						// When `true`: writes are acknowledged only after
						// being written to the on-disk journal (full durability).
						// When `false`: writes are acknowledged after being
						// written to memory (faster, less durable).
						.journal(self.sync)
						// Finalise the write options
						.build(),
				)
				// Configure the read concern options
				.read_concern(ReadConcern::majority())
				// Finalise the database configuration
				.build(),
		);
		Ok(MongoDBClient {
			db,
			sync: self.sync,
		})
	}
}

pub(crate) struct MongoDBClient {
	db: Database,
	sync: bool,
}

impl BenchmarkClient for MongoDBClient {
	// The return type when reading a row
	type ReadRow = Row;

	async fn compact(&self) -> Result<()> {
		// For a database compaction
		self.db
			.run_command(doc! {
				"compact": "record",
				"dryRun": false,
				"force": true,
			})
			.await?;
		// Ok
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.create(key, val).await
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<Row> {
		let doc = self.read(&key).await?;
		assert!(doc.is_some());
		Ok(black_box(Row(doc.unwrap())))
	}

	async fn read_string(&self, key: String) -> Result<Row> {
		let doc = self.read(&key).await?;
		assert!(doc.is_some());
		Ok(black_box(Row(doc.unwrap())))
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
		// Define the index document
		let mut doc = Document::new();
		// Check if an index type is specified
		match &spec.index_type {
			Some(kind) if kind == "fulltext" => {
				// Create a text index
				for field in &spec.fields {
					doc.insert(field, "text");
				}
			}
			Some(kind) => {
				// Other index types (e.g., "2d", "2dsphere", "hashed")
				for field in &spec.fields {
					doc.insert(field, kind.as_str());
				}
			}
			None => {
				// Standard ascending index
				for field in &spec.fields {
					doc.insert(field, 1);
				}
			}
		};
		// Define the index options
		let mut options = IndexOptions::default();
		options.name = Some(name.to_string());
		if let Some(unique) = spec.unique {
			options.unique = Some(unique);
		}
		// Create the index model
		let index_model = IndexModel::builder().keys(doc).options(options).build();
		// Create the index
		self.collection().create_index(index_model).await?;
		// All ok
		Ok(())
	}

	async fn drop_index(&self, name: &str) -> Result<()> {
		self.collection().drop_index(name).await?;
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
		self.batch_create(key_vals.collect()).await
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals.collect()).await
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_read(keys.collect()).await
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_read(keys.collect()).await
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals.collect()).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals.collect()).await
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_delete(keys.collect()).await
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_delete(keys.collect()).await
	}
}

impl MongoDBClient {
	fn collection(&self) -> Collection<Document> {
		self.db.collection("record")
	}

	fn to_doc<K>(key: K, val: BenchValue) -> Result<Document>
	where
		K: Into<Bson>,
	{
		let mut doc = match bench_to_bson(&val) {
			Bson::Document(d) => d,
			_ => bail!("expected object payload for row"),
		};
		doc.insert("_id".to_string(), key.into());
		Ok(doc)
	}

	async fn create<K>(&self, key: K, val: BenchValue) -> Result<()>
	where
		K: Into<Bson>,
	{
		let doc = Self::to_doc(key, val)?;
		let res = self.collection().insert_one(&doc).await?;
		assert_ne!(res.inserted_id, Bson::Null);
		Ok(())
	}

	async fn read<K>(&self, key: K) -> Result<Option<Document>>
	where
		K: Into<Bson>,
	{
		let filter = doc! { "_id": key };
		let doc = self.collection().find_one(filter).await?;
		Ok(doc)
	}

	async fn update<K>(&self, key: K, val: BenchValue) -> Result<()>
	where
		K: Into<Bson> + Clone,
	{
		let filter = doc! { "_id": key.clone() };
		let doc = Self::to_doc(key, val)?;
		let res = self.collection().replace_one(filter, &doc).await?;
		assert_eq!(res.matched_count, 1);
		Ok(())
	}

	async fn delete<K>(&self, key: K) -> Result<()>
	where
		K: Into<Bson>,
	{
		let filter = doc! { "_id": key };
		let res = self.collection().delete_one(filter).await?;
		assert_eq!(res.deleted_count, 1);
		Ok(())
	}

	async fn batch_create<K>(&self, key_vals: Vec<(K, BenchValue)>) -> Result<()>
	where
		K: Into<Bson>,
	{
		let mut docs = Vec::with_capacity(key_vals.len());
		for (key, val) in key_vals {
			docs.push(Self::to_doc(key, val)?);
		}
		let docs_len = docs.len();
		let res = self.collection().insert_many(docs).await?;
		assert_eq!(res.inserted_ids.len(), docs_len);
		Ok(())
	}

	async fn batch_read<K>(&self, keys: Vec<K>) -> Result<()>
	where
		K: Into<Bson>,
	{
		let keys_len = keys.len();
		let ids: Vec<Bson> = keys.into_iter().map(|k| k.into()).collect();
		let filter = doc! { "_id": { "$in": ids } };
		let cursor = self.collection().find(filter).await?;
		let docs: Vec<Document> = cursor.try_collect().await?;
		assert_eq!(docs.len(), keys_len);
		for doc in docs {
			black_box(doc);
		}
		Ok(())
	}

	async fn batch_update<K>(&self, key_vals: Vec<(K, BenchValue)>) -> Result<()>
	where
		K: Into<Bson> + Clone,
	{
		let namespace = Namespace {
			db: self.db.name().to_string(),
			coll: "record".to_string(),
		};
		let mut docs = Vec::with_capacity(key_vals.len());
		for (key, val) in key_vals {
			let filter = doc! { "_id": Into::<Bson>::into(key.clone()) };
			let replacement = Self::to_doc(key, val)?;
			let model = ReplaceOneModel::builder()
				.namespace(namespace.clone())
				.filter(filter)
				.replacement(replacement)
				.build();
			docs.push(WriteModel::ReplaceOne(model));
		}
		let docs_len = docs.len();
		let res = self
			.db
			.client()
			.bulk_write(docs)
			.write_concern(
				// Configure the write options
				WriteConcern::builder()
					// Ensure that all writes are written,
					// replicated, and acknowledged by the
					// majority of nodes in the cluster.
					.w(Acknowledgment::Majority)
					// Configure journal durability based on sync setting.
					// When `true`: writes are acknowledged only after
					// being written to the on-disk journal (full durability).
					// When `false`: writes are acknowledged after being
					// written to memory (faster, less durable).
					.journal(self.sync)
					// Finalise the write options
					.build(),
			)
			.await?;
		assert_eq!(res.matched_count, docs_len as i64);
		Ok(())
	}

	async fn batch_delete<K>(&self, keys: Vec<K>) -> Result<()>
	where
		K: Into<Bson>,
	{
		let keys_len = keys.len();
		let ids: Vec<Bson> = keys.into_iter().map(|k| k.into()).collect();
		let filter = doc! { "_id": { "$in": ids } };
		let res = self.collection().delete_many(filter).await?;
		assert_eq!(res.deleted_count, keys_len as u64);
		Ok(())
	}

	async fn scan(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		// MongoDB requires a full-text index to use a $text query
		if ctx == ScanContext::WithoutIndex
			&& let Some(index) = &scan.with_index
			&& let Some(kind) = &index.index_type
			&& kind == "fulltext"
		{
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(i64::MAX as usize);
		let c = MongoDBDialect::filter_clause(scan)?;
		let o = MongoDBDialect::sort_document(scan)?;
		let p = scan.projection()?;
		// Consume documents function
		let consume = |mut cursor: Cursor<Document>| async move {
			let mut count = 0;
			while let Some(doc) = cursor.try_next().await? {
				black_box(doc);
				count += 1;
			}
			Ok(count)
		};
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				consume(match o {
					Some(o) => {
						self.collection()
							.find(c)
							.sort(o)
							.skip(s as u64)
							.limit(l as i64)
							.projection(doc! { "_id": 1 })
							.await?
					}
					None => {
						self.collection()
							.find(c)
							.skip(s as u64)
							.limit(l as i64)
							.projection(doc! { "_id": 1 })
							.await?
					}
				})
				.await
			}
			Projection::Full => {
				consume(match o {
					Some(o) => {
						self.collection()
							.find(c)
							.sort(o)
							.skip(s as u64)
							.limit(l as i64)
							.projection(doc! { "_id": 1 })
							.await?
					}
					None => {
						self.collection()
							.find(c)
							.skip(s as u64)
							.limit(l as i64)
							.projection(doc! { "_id": 1 })
							.await?
					}
				})
				.await
			}
			Projection::Count => {
				let pipeline = vec![
					doc! { "$match": c },
					doc! { "$skip": s as i64 },
					doc! { "$limit": l as i64 },
					doc! { "$count": "count" },
				];
				let mut cursor = self.collection().aggregate(pipeline).await?;
				if let Some(result) = cursor.next().await {
					let doc: Document = result?;
					let count = doc.get_i32("count").unwrap_or(0);
					Ok(count as usize)
				} else {
					bail!("No row returned");
				}
			}
		}
	}
}
