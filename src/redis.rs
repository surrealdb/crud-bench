use anyhow::Result;
use redis::aio::Connection;
use redis::{AsyncCommands, Client};

use crate::benchmark::{BenchmarkClient, BenchmarkClientProvider, Record};
use crate::docker::DockerParams;

pub(crate) const REDIS_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "redis",
	pre_args: "-p 127.0.0.1:6379:6379",
	post_args: "redis-server --requirepass root",
};

#[derive(Default)]
pub(crate) struct RedisClientProvider {}

impl BenchmarkClientProvider<RedisClient> for RedisClientProvider {
	async fn create_client(&self) -> Result<RedisClient> {
		let url = "redis://:root@127.0.0.1:6379/";
		let client = Client::open(url)?;
		let conn = client.get_async_connection().await?;
		Ok(RedisClient {
			conn,
		})
	}
}

pub(crate) struct RedisClient {
	conn: Connection,
}

impl BenchmarkClient for RedisClient {
	async fn read(&mut self, key: i32) -> Result<()> {
		let val: Vec<u8> = self.conn.get(key).await?;
		assert!(!val.is_empty());
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create(&mut self, key: i32, record: &Record) -> Result<()> {
		let val = serde_json::to_vec(record)?;
		self.conn.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update(&mut self, key: i32, record: &Record) -> Result<()> {
		let val = serde_json::to_vec(record)?;
		self.conn.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete(&mut self, key: i32) -> Result<()> {
		self.conn.del(key).await?;
		Ok(())
	}
}
