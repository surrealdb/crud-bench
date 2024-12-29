use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::keyprovider::{IntegerKeyProvider, KeyProvider, StringKeyProvider};
use crate::valueprovider::Columns;
use crate::{KeyType, Scan};
use anyhow::{bail, Result};
use serde_json::Value;
use std::future::Future;

/// A trait for a database benchmark implementation
/// setting up a database, and creating clients.
pub(crate) trait BenchmarkEngine<C>: Sized
where
	C: BenchmarkClient + Send,
{
	async fn setup(kt: KeyType, columns: Columns, endpoint: Option<&str>) -> Result<Self>;
	async fn create_client(&self) -> Result<C>;
}

/// A trait for a database benchmark implementation for
/// running benchmark tests for a client or connection.
pub(crate) trait BenchmarkClient: Sync + Send + 'static {
	/// Initialise the store at startup
	async fn startup(&self) -> Result<()> {
		Ok(())
	}

	/// Cleanup the store at shutdown
	async fn shutdown(&self) -> Result<()> {
		Ok(())
	}

	/// Compact the store for performance
	async fn compact(&self) -> Result<()> {
		Ok(())
	}

	/// Create a single entry with the current client
	fn create(
		&self,
		n: u32,
		val: Value,
		kp: &mut KeyProvider,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => self.create_u32(p.key(n), val).await,
				KeyProvider::UnorderedInteger(p) => self.create_u32(p.key(n), val).await,
				KeyProvider::OrderedString(p) => self.create_string(p.key(n), val).await,
				KeyProvider::UnorderedString(p) => self.create_string(p.key(n), val).await,
			}
		}
	}

	/// Read a single entry with the current client
	fn read(&self, n: u32, kp: &mut KeyProvider) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => self.read_u32(p.key(n)).await,
				KeyProvider::UnorderedInteger(p) => self.read_u32(p.key(n)).await,
				KeyProvider::OrderedString(p) => self.read_string(p.key(n)).await,
				KeyProvider::UnorderedString(p) => self.read_string(p.key(n)).await,
			}
		}
	}

	/// Update a single entry with the current client
	fn update(
		&self,
		n: u32,
		val: Value,
		kp: &mut KeyProvider,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => self.update_u32(p.key(n), val).await,
				KeyProvider::UnorderedInteger(p) => self.update_u32(p.key(n), val).await,
				KeyProvider::OrderedString(p) => self.update_string(p.key(n), val).await,
				KeyProvider::UnorderedString(p) => self.update_string(p.key(n), val).await,
			}
		}
	}

	/// Delete a single entry with the current client
	fn delete(&self, n: u32, kp: &mut KeyProvider) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => self.delete_u32(p.key(n)).await,
				KeyProvider::UnorderedInteger(p) => self.delete_u32(p.key(n)).await,
				KeyProvider::OrderedString(p) => self.delete_string(p.key(n)).await,
				KeyProvider::UnorderedString(p) => self.delete_string(p.key(n)).await,
			}
		}
	}

	/// Scan a range of entries with the current client
	fn scan(&self, scan: &Scan, kp: &KeyProvider) -> impl Future<Output = Result<()>> + Send {
		async move {
			let result = match kp {
				KeyProvider::OrderedInteger(_) | KeyProvider::UnorderedInteger(_) => {
					self.scan_u32(scan).await?
				}
				KeyProvider::OrderedString(_) | KeyProvider::UnorderedString(_) => {
					self.scan_string(scan).await?
				}
			};
			if let Some(expect) = scan.expect {
				assert_eq!(
					expect, result,
					"Expected a length of {expect} but found {result} for {}",
					scan.name
				);
			}
			Ok(())
		}
	}

	/// Create a single entry with a numeric id
	fn create_u32(&self, key: u32, val: Value) -> impl Future<Output = Result<()>> + Send;

	/// Create a single entry with a string id
	fn create_string(&self, key: String, val: Value) -> impl Future<Output = Result<()>> + Send;

	/// Read a single entry with a numeric id
	fn read_u32(&self, key: u32) -> impl Future<Output = Result<()>> + Send;

	/// Read a single entry with a string id
	fn read_string(&self, key: String) -> impl Future<Output = Result<()>> + Send;

	/// Update a single entry with a numeric id
	fn update_u32(&self, key: u32, val: Value) -> impl Future<Output = Result<()>> + Send;

	/// Update a single entry with a string id
	fn update_string(&self, key: String, val: Value) -> impl Future<Output = Result<()>> + Send;

	/// Delete a single entry with a numeric id
	fn delete_u32(&self, key: u32) -> impl Future<Output = Result<()>> + Send;

	/// Delete a single entry with a string id
	fn delete_string(&self, key: String) -> impl Future<Output = Result<()>> + Send;

	/// Scan a range of entries with numeric ids
	fn scan_u32(&self, _scan: &Scan) -> impl Future<Output = Result<usize>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Scan a range of entries with string ids
	fn scan_string(&self, _scan: &Scan) -> impl Future<Output = Result<usize>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}
}
