use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Record;
use crate::KeyType;
use anyhow::Result;
use std::hint::black_box;

pub(crate) struct DryClientProvider {}

impl BenchmarkEngine<DryClient> for DryClientProvider {
	async fn setup(_kt: KeyType) -> Result<Self> {
		Ok(Self {})
	}

	async fn create_client(&self, _endpoint: Option<String>) -> Result<DryClient> {
		Ok(DryClient {})
	}
}

pub(crate) struct DryClient {}

impl BenchmarkClient for DryClient {
	async fn create_u32(&self, key: u32, record: Record) -> Result<()> {
		black_box((key, record));
		Ok(())
	}

	async fn create_string(&self, key: String, record: Record) -> Result<()> {
		black_box((key, record));
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		black_box(key);
		Ok(())
	}

	async fn read_string(&self, key: String) -> Result<()> {
		black_box(key);
		Ok(())
	}

	async fn update_u32(&self, key: u32, record: Record) -> Result<()> {
		black_box((key, record));
		Ok(())
	}

	async fn update_string(&self, key: String, record: Record) -> Result<()> {
		black_box((key, record));
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		black_box(key);
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		black_box(key);
		Ok(())
	}
}
