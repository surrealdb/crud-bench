use crate::benchmark::{BenchmarkClient, BenchmarkClientProvider, Record};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) type DryDatabase = Arc<RwLock<HashMap<i32, Record>>>;

#[derive(Default)]
pub(crate) struct DryClientProvider {
	database: DryDatabase,
}

impl BenchmarkClientProvider<DryClient> for DryClientProvider {
	async fn create_client(&self) -> Result<DryClient> {
		Ok(DryClient {
			database: self.database.clone(),
		})
	}
}

pub(crate) struct DryClient {
	database: DryDatabase,
}

impl BenchmarkClient for DryClient {
	async fn create(&mut self, sample: i32, record: &Record) -> Result<()> {
		assert!(self.database.write().await.insert(sample, record.clone()).is_none());
		Ok(())
	}

	async fn read(&mut self, sample: i32) -> Result<()> {
		assert!(self.database.read().await.get(&sample).is_some());
		Ok(())
	}

	async fn update(&mut self, sample: i32, record: &Record) -> Result<()> {
		assert!(self.database.write().await.insert(sample, record.clone()).is_some());
		Ok(())
	}
	async fn delete(&mut self, sample: i32) -> Result<()> {
		assert!(self.database.write().await.remove(&sample).is_some());
		Ok(())
	}
}
