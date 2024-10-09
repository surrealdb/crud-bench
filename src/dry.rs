use crate::benchmark::{BenchmarkClient, BenchmarkEngine, Record};
use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;

pub(crate) type DryDatabase = Arc<DashMap<u32, Record>>;

#[derive(Default)]
pub(crate) struct DryClientProvider {
	database: DryDatabase,
}

impl BenchmarkEngine<DryClient> for DryClientProvider {
	async fn create_client(&self, _: Option<String>) -> Result<DryClient> {
		Ok(DryClient {
			database: self.database.clone(),
		})
	}
}

pub(crate) struct DryClient {
	database: DryDatabase,
}

impl BenchmarkClient for DryClient {
	async fn create(&self, sample: u32, record: &Record) -> Result<()> {
		assert!(self.database.insert(sample, record.clone()).is_none());
		Ok(())
	}

	async fn read(&self, sample: u32) -> Result<()> {
		assert!(self.database.get(&sample).is_some());
		Ok(())
	}

	async fn update(&self, sample: u32, record: &Record) -> Result<()> {
		assert!(self.database.insert(sample, record.clone()).is_some());
		Ok(())
	}
	async fn delete(&self, sample: u32) -> Result<()> {
		assert!(self.database.remove(&sample).is_some());
		Ok(())
	}
}
