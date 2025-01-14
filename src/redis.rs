#![cfg(feature = "redis")]

use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType};
use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use serde_json::Value;
use std::hint::black_box;
use tokio::sync::Mutex;

pub const DEFAULT: &str = "redis://:root@127.0.0.1:6379/";

pub(crate) const REDIS_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "redis",
	pre_args: "-p 127.0.0.1:6379:6379",
	post_args: "redis-server --requirepass root",
};

pub(crate) struct RedisClientProvider {
	url: String,
}

impl BenchmarkEngine<RedisClient> for RedisClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		Ok(Self {
			url: options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned(),
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<RedisClient> {
		let client = Client::open(self.url.as_str())?;
		let conn = Mutex::new(client.get_multiplexed_async_connection().await?);
		Ok(RedisClient {
			conn,
		})
	}
}

pub(crate) struct RedisClient {
	conn: Mutex<MultiplexedConnection>,
}

impl BenchmarkClient for RedisClient {
	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		let val: Vec<u8> = self.conn.lock().await.get(key).await?;
		assert!(!val.is_empty());
		black_box(val);
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_string(&self, key: String) -> Result<()> {
		let val: Vec<u8> = self.conn.lock().await.get(key).await?;
		assert!(!val.is_empty());
		black_box(val);
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		let val = bincode::serialize(&val)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.conn.lock().await.del(key).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_string(&self, key: String) -> Result<()> {
		self.conn.lock().await.del(key).await?;
		Ok(())
	}
}
