#![cfg(feature = "mongodb")]

use crate::dialect::MongoDBDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::memory::Config;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use futures::{StreamExt, TryStreamExt};
use mongodb::Namespace;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::ClientOptions;
use mongodb::options::DatabaseOptions;
use mongodb::options::ReadConcern;
use mongodb::options::{Acknowledgment, ReplaceOneModel, WriteConcern, WriteModel};
use mongodb::{Client, Collection, Cursor, Database, bson};
use serde_json::Value;
use std::hint::black_box;
use std::time::Duration;

pub const DEFAULT: &str = "mongodb://root:root@127.0.0.1:27017";

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

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let doc = self.read(key).await?;
		assert_eq!(doc.unwrap().get("_id").unwrap().as_i64().unwrap() as u32, key);
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		let doc = self.read(&key).await?;
		assert_eq!(doc.unwrap().get_str("_id")?, key);
		Ok(())
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

	async fn batch_create_u32(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut key_vals_vec = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			key_vals_vec.push((key, val));
		}
		self.batch_create(key_vals_vec).await
	}

	async fn batch_create_string(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut key_vals_vec = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			key_vals_vec.push((key, val));
		}
		self.batch_create(key_vals_vec).await
	}

	async fn batch_read_u32(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = u32> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key);
		}
		self.batch_read(keys_vec).await
	}

	async fn batch_read_string(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = String> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key);
		}
		self.batch_read(keys_vec).await
	}

	async fn batch_update_u32(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut key_vals_vec = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			key_vals_vec.push((key, val));
		}
		self.batch_update(key_vals_vec).await
	}

	async fn batch_update_string(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut key_vals_vec = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			key_vals_vec.push((key, val));
		}
		self.batch_update(key_vals_vec).await
	}

	async fn batch_delete_u32(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = u32> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key);
		}
		self.batch_delete(keys_vec).await
	}

	async fn batch_delete_string(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = String> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key);
		}
		self.batch_delete(keys_vec).await
	}
}

impl MongoDBClient {
	fn collection(&self) -> Collection<Document> {
		self.db.collection("record")
	}

	fn to_doc<K>(key: K, mut val: Value) -> Result<Bson>
	where
		K: Into<Value> + Into<Bson>,
	{
		let obj = val.as_object_mut().unwrap();
		obj.insert("_id".to_string(), key.into());
		Ok(bson::to_bson(&val)?)
	}

	async fn create<K>(&self, key: K, val: Value) -> Result<()>
	where
		K: Into<Value> + Into<Bson>,
	{
		let bson = Self::to_doc(key, val)?;
		let doc = bson.as_document().unwrap();
		let res = self.collection().insert_one(doc).await?;
		assert_ne!(res.inserted_id, Bson::Null);
		Ok(())
	}

	async fn read<K>(&self, key: K) -> Result<Option<Document>>
	where
		K: Into<Bson>,
	{
		let filter = doc! { "_id": key };
		let doc = self.collection().find_one(filter).await?;
		assert!(doc.is_some());
		Ok(doc)
	}

	async fn update<K>(&self, key: K, val: Value) -> Result<()>
	where
		K: Into<Value> + Into<Bson> + Clone,
	{
		let filter = doc! { "_id": key.clone() };
		let bson = Self::to_doc(key, val)?;
		let doc = bson.as_document().unwrap();
		let res = self.collection().replace_one(filter, doc).await?;
		assert_eq!(res.modified_count, 1);
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

	async fn batch_create<K>(&self, key_vals: Vec<(K, Value)>) -> Result<()>
	where
		K: Into<Value> + Into<Bson>,
	{
		let mut docs = Vec::with_capacity(key_vals.len());
		for (key, val) in key_vals {
			let bson = Self::to_doc(key, val)?;
			docs.push(bson.as_document().unwrap().clone());
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

	async fn batch_update<K>(&self, key_vals: Vec<(K, Value)>) -> Result<()>
	where
		K: Into<Value> + Into<Bson> + Clone,
	{
		let namespace = Namespace {
			db: self.db.name().to_string(),
			coll: "record".to_string(),
		};
		let mut docs = Vec::with_capacity(key_vals.len());
		for (key, val) in key_vals {
			let filter = doc! { "_id": Into::<Bson>::into(key.clone()) };
			let bson = Self::to_doc(key, val)?;
			let replacement = bson.as_document().unwrap().clone();
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
		assert_eq!(res.modified_count, docs_len as i64);
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

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(i64::MAX as usize);
		let c = MongoDBDialect::filter_clause(scan)?;
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
				let cursor = self
					.collection()
					.find(c)
					.skip(s as u64)
					.limit(l as i64)
					.projection(doc! { "_id": 1 })
					.await?;
				consume(cursor).await
			}
			Projection::Full => {
				let cursor = self.collection().find(c).skip(s as u64).limit(l as i64).await?;
				consume(cursor).await
			}
			Projection::Count => {
				let pipeline = vec![
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
