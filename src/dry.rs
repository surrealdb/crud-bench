use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use crate::KeyType;
use anyhow::{bail, Result};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) enum DryDatabase {
	Integer(Arc<DashMap<u32, Record>>),
	_String(Arc<DashMap<String, Record>>),
}

impl From<KeyType> for DryDatabase {
	fn from(t: KeyType) -> Self {
		match t {
			KeyType::Integer => Self::Integer(DashMap::new().into()),
			KeyType::String16 | KeyType::String68 => Self::Integer(DashMap::new().into()),
			KeyType::Uuid => todo!(),
		}
	}
}

pub(crate) struct DryClientProvider(DryDatabase);

impl BenchmarkEngine<DryClient> for DryClientProvider {
	async fn setup(kt: KeyType) -> Result<Self> {
		Ok(Self(kt.into()))
	}

	async fn create_client(&self, _: Option<String>) -> Result<DryClient> {
		Ok(DryClient(self.0.clone()))
	}
}

pub(crate) struct DryClient(DryDatabase);

impl BenchmarkClient for DryClient {
	async fn create_u32(&self, key: u32, record: &Record) -> Result<()> {
		if let DryDatabase::Integer(m) = &self.0 {
			assert!(m.insert(key, record.clone()).is_none());
		} else {
			bail!("Invalid DryDatabase variant");
		}
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		if let DryDatabase::Integer(m) = &self.0 {
			assert!(m.get(&key).is_some());
		} else {
			bail!("Invalid DryDatabase variant");
		}
		Ok(())
	}

	async fn update_u32(&self, key: u32, record: &Record) -> Result<()> {
		if let DryDatabase::Integer(m) = &self.0 {
			assert!(m.insert(key, record.clone()).is_some());
		} else {
			bail!("Invalid DryDatabase variant");
		}
		Ok(())
	}
	async fn delete_u32(&self, sample: u32) -> Result<()> {
		if let DryDatabase::Integer(m) = &self.0 {
			assert!(m.remove(&sample).is_some());
		} else {
			bail!("Invalid DryDatabase variant");
		}
		Ok(())
	}
}
