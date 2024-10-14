use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::KeyType;
use anyhow::{bail, Result};
use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) enum MapDatabase {
	Integer(Arc<DashMap<u32, Value>>),
	String(Arc<DashMap<String, Value>>),
}

impl From<KeyType> for MapDatabase {
	fn from(t: KeyType) -> Self {
		match t {
			KeyType::Integer => Self::Integer(DashMap::new().into()),
			KeyType::String26 | KeyType::String90 | KeyType::String506 => {
				Self::String(DashMap::new().into())
			}
			KeyType::Uuid => todo!(),
		}
	}
}

pub(crate) struct MapClientProvider(MapDatabase);

impl BenchmarkEngine<MapClient> for MapClientProvider {
	async fn setup(kt: KeyType, _columns: Columns) -> Result<Self> {
		Ok(Self(kt.into()))
	}

	async fn create_client(&self, _: Option<String>) -> Result<MapClient> {
		Ok(MapClient(self.0.clone()))
	}
}

pub(crate) struct MapClient(MapDatabase);

impl BenchmarkClient for MapClient {
	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		if let MapDatabase::Integer(m) = &self.0 {
			assert!(m.insert(key, val).is_none());
		} else {
			bail!("Invalid MapDatabase variant");
		}
		Ok(())
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		if let MapDatabase::String(m) = &self.0 {
			assert!(m.insert(key, val).is_none());
		} else {
			bail!("Invalid MapDatabase variant");
		}
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		if let MapDatabase::Integer(m) = &self.0 {
			assert!(m.get(&key).is_some());
		} else {
			bail!("Invalid MapDatabase variant");
		}
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		if let MapDatabase::String(m) = &self.0 {
			assert!(m.get(&key).is_some());
		} else {
			bail!("Invalid MapDatabase variant");
		}
		Ok(())
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		if let MapDatabase::Integer(m) = &self.0 {
			assert!(m.insert(key, val).is_some());
		} else {
			bail!("Invalid MapDatabase variant");
		}
		Ok(())
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		if let MapDatabase::String(m) = &self.0 {
			assert!(m.insert(key, val).is_some());
		} else {
			bail!("Invalid MapDatabase variant");
		}
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		if let MapDatabase::Integer(m) = &self.0 {
			assert!(m.remove(&key).is_some());
		} else {
			bail!("Invalid MapDatabase variant");
		}
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		if let MapDatabase::String(m) = &self.0 {
			assert!(m.remove(&key).is_some());
		} else {
			bail!("Invalid MapDatabase variant");
		}
		Ok(())
	}
}
