#![cfg(feature = "dragonfly")]

use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::KeyType;
use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use serde_json::Value;
use std::hint::black_box;
use tokio::sync::Mutex;

pub(crate) const DRAGONFLY_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "docker.dragonflydb.io/dragonflydb/dragonfly",
	pre_args: "-p 127.0.0.1:6379:6379 --ulimit memlock=-1",
	post_args: "--requirepass root",
};

pub(crate) struct DragonflyClientProvider {
	url: String,
}

impl BenchmarkEngine<DragonflyClient> for DragonflyClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_kt: KeyType, _columns: Columns, endpoint: Option<&str>) -> Result<Self> {
		Ok(Self {
			url: endpoint.unwrap_or("redis://:root@127.0.0.1:6379/").to_owned(),
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<DragonflyClient> {
		let client = Client::open(self.url.as_str())?;
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

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_u32(&self, key: u32) -> Result<()> {
		let val: Vec<u8> = self.conn.lock().await.get(key).await?;
		assert!(!val.is_empty());
		black_box(bincode::deserialize::<Value>(&val)?);
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_string(&self, key: String) -> Result<()> {
		let val: Vec<u8> = self.conn.lock().await.get(key).await?;
		assert!(!val.is_empty());
		black_box(bincode::deserialize::<Value>(&val)?);
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
