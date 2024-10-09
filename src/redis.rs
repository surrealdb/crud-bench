#![cfg(feature = "redis")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::docker::DockerParams;
use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use tokio::sync::Mutex;

pub(crate) const REDIS_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "redis",
	pre_args: "-p 127.0.0.1:6379:6379",
	post_args: "redis-server --requirepass root",
};

#[derive(Default)]
pub(crate) struct RedisClientProvider {}

impl BenchmarkEngine<RedisClient> for RedisClientProvider {
	async fn create_client(&self, endpoint: Option<String>) -> Result<RedisClient> {
		let url = endpoint.unwrap_or("redis://:root@127.0.0.1:6379/".to_owned());
		let client = Client::open(url)?;
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
	async fn create(&self, key: u32, record: &Record) -> Result<()> {
		let val = bincode::serialize(record)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	async fn read(&self, key: u32) -> Result<()> {
		let val: Vec<u8> = self.conn.lock().await.get(key).await?;
		assert!(!val.is_empty());
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update(&self, key: u32, record: &Record) -> Result<()> {
		let val = bincode::serialize(record)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete(&self, key: u32) -> Result<()> {
		self.conn.lock().await.del(key).await?;
		Ok(())
	}
}
