use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
use dashmap::DashMap;
use serde_json::Value;
use std::hash::Hash;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

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
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, _columns: Columns, _endpoint: Option<&str>) -> Result<Self> {
		Ok(Self(kt.into()))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<MapClient> {
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

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		if let MapDatabase::Integer(m) = &self.0 {
			Self::scan(m, scan).await
		} else {
			bail!("Invalid MapDatabase variant");
		}
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		if let MapDatabase::String(m) = &self.0 {
			Self::scan(m, scan).await
		} else {
			bail!("Invalid MapDatabase variant");
		}
	}
}

impl MapClient {
	async fn scan<T>(m: &DashMap<T, Value>, scan: &Scan) -> Result<usize>
	where
		T: Eq + Hash,
	{
		// Contional scans are not supported
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(usize::MAX);
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => Ok(m
				.iter()
				.skip(s) // Skip the first `offset` entries
				.take(l) // Take the next `limit` entries
				.map(|v| -> Result<_> {
					black_box(v);
					Ok(())
				})
				.count()),
			Projection::Full => Ok(m
				.iter()
				.skip(s) // Skip the first `offset` entries
				.take(l) // Take the next `limit` entries
				.map(|v| -> Result<_> {
					black_box(v);
					Ok(())
				})
				.count()),
			Projection::Count => Ok(m
				.iter()
				.skip(s) // Skip the first `offset` entries
				.take(l) // Take the next `limit` entries
				.count()),
		}
	}
}
