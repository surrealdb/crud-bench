#![cfg(feature = "dragonfly")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::docker::DockerParams;
use crate::valueprovider::Record;
use crate::KeyType;
use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use tokio::sync::Mutex;

pub(crate) const DRAGONFLY_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "docker.dragonflydb.io/dragonflydb/dragonfly",
	pre_args: "-p 127.0.0.1:6379:6379 --ulimit memlock=-1",
	post_args: "--requirepass root",
};

pub(crate) struct DragonflyClientProvider {}

impl BenchmarkEngine<DragonflyClient> for DragonflyClientProvider {
	async fn setup(_: KeyType) -> Result<Self> {
		Ok(Self {})
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<DragonflyClient> {
		let url = endpoint.unwrap_or("redis://:root@127.0.0.1:6379/".to_owned());
		let client = Client::open(url)?;
		let conn = Mutex::new(client.get_multiplexed_async_connection().await?);
		Ok(DragonflyClient {
			conn,
		})
	}
}

pub(crate) struct DragonflyClient {
	conn: Mutex<MultiplexedConnection>,
}

impl BenchmarkClient for DragonflyClient {
	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_u32(&self, key: u32, record: Record) -> Result<()> {
		let val = bincode::serialize(&record)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn create_string(&self, key: String, record: Record) -> Result<()> {
		let val = bincode::serialize(&record)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_u32(&self, key: u32) -> Result<()> {
		let val: Vec<u8> = self.conn.lock().await.get(key).await?;
		assert!(!val.is_empty());
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_string(&self, key: String) -> Result<()> {
		let val: Vec<u8> = self.conn.lock().await.get(key).await?;
		assert!(!val.is_empty());
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, record: Record) -> Result<()> {
		let val = bincode::serialize(&record)?;
		self.conn.lock().await.set(key, val).await?;
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_string(&self, key: String, record: Record) -> Result<()> {
		let val = bincode::serialize(&record)?;
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
