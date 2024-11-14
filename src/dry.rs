use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::Columns;
use crate::{KeyType, Scan};
use anyhow::Result;
use serde_json::Value;
use std::hint::black_box;

pub(crate) struct DryClientProvider {}

impl BenchmarkEngine<DryClient> for DryClientProvider {
	async fn setup(_kt: KeyType, _columns: Columns) -> Result<Self> {
		Ok(Self {})
	}

	async fn create_client(&self, _endpoint: Option<String>) -> Result<DryClient> {
		Ok(DryClient {})
	}
}

pub(crate) struct DryClient {}

impl BenchmarkClient for DryClient {
	async fn scan(&self, scan: &Scan) -> Result<()> {
		black_box(scan);
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		black_box((key, val));
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

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		black_box((key, val));
		Ok(())
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		black_box((key, val));
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
