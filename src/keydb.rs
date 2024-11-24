#![cfg(feature = "keydb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::docker::DockerParams;
use crate::valueprovider::Columns;
use crate::KeyType;
use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use serde_json::Value;
use tokio::sync::Mutex;

pub(crate) const KEYDB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "eqalpha/keydb",
	pre_args: "-p 127.0.0.1:6379:6379",
	post_args: "keydb-server --requirepass root",
};

pub(crate) struct KeydbClientProvider {}

impl BenchmarkEngine<KeydbClient> for KeydbClientProvider {
	async fn setup(_kt: KeyType, _columns: Columns) -> Result<Self> {
		Ok(KeydbClientProvider {})
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<KeydbClient> {
		let url = endpoint.unwrap_or("redis://:root@127.0.0.1:6379/".to_owned());
		let client = Client::open(url)?;
		let conn = Mutex::new(client.get_multiplexed_async_connection().await?);
		Ok(KeydbClient {
			conn,
		})
	}
}

pub(crate) struct KeydbClient {
	conn: Mutex<MultiplexedConnection>,
}

impl BenchmarkClient for KeydbClient {
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
		Ok(())
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn read_string(&self, key: String) -> Result<()> {
		let val: Vec<u8> = self.conn.lock().await.get(key).await?;
		assert!(!val.is_empty());
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
