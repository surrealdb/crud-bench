#![cfg(feature = "keydb")]

use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::redis::RedisClient;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Scan};
use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use serde_json::Value;
use std::hint::black_box;
use tokio::sync::Mutex;

pub const DEFAULT: &str = "redis://:root@127.0.0.1:6379/";

pub(crate) const KEYDB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "eqalpha/keydb",
	pre_args: "-p 127.0.0.1:6379:6379",
	post_args: "keydb-server --requirepass root",
};

pub(crate) struct KeydbClientProvider {
	url: String,
}

impl BenchmarkEngine<KeydbClient> for KeydbClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		Ok(KeydbClientProvider {
			url: options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned(),
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<KeydbClient> {
		let client = Client::open(self.url.as_str())?;
		let conn_record = Mutex::new(client.get_multiplexed_async_connection().await?);
		let conn_iter = Mutex::new(client.get_multiplexed_async_connection().await?);
		Ok(KeydbClient {
			conn_record,
			conn_iter,
		})
	}
}

pub(crate) struct KeydbClient {
	conn_record: Mutex<MultiplexedConnection>,
	conn_iter: Mutex<MultiplexedConnection>,
}

impl BenchmarkClient for KeydbClient {
	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_u32(&self, key: u32) -> Result<()> {
		let val: Vec<u8> = self.conn_record.lock().await.get(key).await?;
		assert!(!val.is_empty());
		black_box(val);
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_string(&self, key: String) -> Result<()> {
		let val: Vec<u8> = self.conn_record.lock().await.get(key).await?;
		assert!(!val.is_empty());
		black_box(val);
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn_record.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.conn_record.lock().await.del(key).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_string(&self, key: String) -> Result<()> {
		self.conn_record.lock().await.del(key).await?;
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		let mut conn_iter = self.conn_iter.lock().await;
		let mut conn_record = self.conn_record.lock().await;
		RedisClient::scan_bytes(&mut conn_iter, &mut conn_record, scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan_u32(scan).await
	}
}
