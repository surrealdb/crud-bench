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
		let projection = scan.projection()?;
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		match projection {
			Projection::Id => bail!(NOT_SUPPORTED_ERROR),
			Projection::Full => {
				let mut count = 0;
				if let Some(start) = scan.start {
					if let Some(limit) = scan.limit {
						m.iter().skip(start).take(limit).for_each(|e| {
							black_box(e);
							count += 1;
						})
					} else {
						m.iter().skip(start).for_each(|e| {
							black_box(e);
							count += 1;
						})
					}
				} else if let Some(limit) = scan.limit {
					m.iter().take(limit).for_each(|e| {
						black_box(e);
						count += 1;
					})
				} else {
					m.iter().for_each(|e| {
						black_box(e);
						count += 1;
					})
				};
				Ok(count)
			}
			Projection::Count => {
				let c = if let Some(start) = scan.start {
					if let Some(limit) = scan.limit {
						m.iter().skip(start).take(limit).count()
					} else {
						m.iter().skip(start).count()
					}
				} else if let Some(limit) = scan.limit {
					m.iter().take(limit).count()
				} else {
					m.iter().count()
				};
				Ok(c)
			}
		}
	}
}
