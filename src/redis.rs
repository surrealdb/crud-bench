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
	async fn prepare(&mut self) -> Result<()> {
		Ok(())
	}

	async fn create(&mut self, key: i32, record: &Record) -> Result<()> {
		let json = serde_json::to_string(record)?;
		self.conn.set(key, json).await?;
		Ok(())
	}

	async fn read(&mut self, key: i32) -> Result<()> {
		let json: String = self.conn.get(key).await?;
		assert!(json.starts_with("{\"text\":\""), "{}", json);
		Ok(())
	}

	async fn update(&mut self, key: i32, record: &Record) -> Result<()> {
		self.create(key, record).await
	}

	async fn delete(&mut self, key: i32) -> Result<()> {
		self.conn.del(key).await?;
		Ok(())
	}
}
