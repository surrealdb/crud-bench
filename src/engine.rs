use crate::Benchmark;
use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::keyprovider::{IntegerKeyProvider, KeyProvider, StringKeyProvider};
use crate::valueprovider::Columns;
use crate::{BatchOperation, KeyType, Scan};
use anyhow::{Result, bail};
use serde_json::Value;
use std::future::Future;
use std::time::Duration;

/// A trait for a database benchmark implementation
/// setting up a database, and creating clients.
pub(crate) trait BenchmarkEngine<C>: Sized
where
	C: BenchmarkClient + Send,
{
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, columns: Columns, endpoint: &Benchmark) -> Result<Self>;
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<C>;
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		Some(Duration::from_secs(5))
	}
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

	/// Perform a batch create operation
	fn batch_create(
		&self,
		n: u32,
		batch_op: &BatchOperation,
		kp: &mut KeyProvider,
		vp: &mut crate::valueprovider::ValueProvider,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => {
					let pairs_iter = generate_integer_key_values_iter(n, batch_op, p, vp);
					self.batch_create_u32(pairs_iter).await
				}
				KeyProvider::UnorderedInteger(p) => {
					let pairs_iter = generate_integer_key_values_iter(n, batch_op, p, vp);
					self.batch_create_u32(pairs_iter).await
				}
				KeyProvider::OrderedString(p) => {
					let pairs_iter = generate_string_key_values_iter(n, batch_op, p, vp);
					self.batch_create_string(pairs_iter).await
				}
				KeyProvider::UnorderedString(p) => {
					let pairs_iter = generate_string_key_values_iter(n, batch_op, p, vp);
					self.batch_create_string(pairs_iter).await
				}
			}
		}
	}

	/// Perform a batch read operation
	fn batch_read(
		&self,
		n: u32,
		batch_op: &BatchOperation,
		kp: &mut KeyProvider,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => {
					let keys_iter = generate_integer_keys_iter(n, batch_op, p);
					self.batch_read_u32(keys_iter).await
				}
				KeyProvider::UnorderedInteger(p) => {
					let keys_iter = generate_integer_keys_iter(n, batch_op, p);
					self.batch_read_u32(keys_iter).await
				}
				KeyProvider::OrderedString(p) => {
					let keys_iter = generate_string_keys_iter(n, batch_op, p);
					self.batch_read_string(keys_iter).await
				}
				KeyProvider::UnorderedString(p) => {
					let keys_iter = generate_string_keys_iter(n, batch_op, p);
					self.batch_read_string(keys_iter).await
				}
			}
		}
	}

	/// Perform a batch update operation
	fn batch_update(
		&self,
		n: u32,
		batch_op: &BatchOperation,
		kp: &mut KeyProvider,
		vp: &mut crate::valueprovider::ValueProvider,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => {
					let pairs_iter = generate_integer_key_values_iter(n, batch_op, p, vp);
					self.batch_update_u32(pairs_iter).await
				}
				KeyProvider::UnorderedInteger(p) => {
					let pairs_iter = generate_integer_key_values_iter(n, batch_op, p, vp);
					self.batch_update_u32(pairs_iter).await
				}
				KeyProvider::OrderedString(p) => {
					let pairs_iter = generate_string_key_values_iter(n, batch_op, p, vp);
					self.batch_update_string(pairs_iter).await
				}
				KeyProvider::UnorderedString(p) => {
					let pairs_iter = generate_string_key_values_iter(n, batch_op, p, vp);
					self.batch_update_string(pairs_iter).await
				}
			}
		}
	}

	/// Perform a batch delete operation
	fn batch_delete(
		&self,
		n: u32,
		batch_op: &BatchOperation,
		kp: &mut KeyProvider,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => {
					let keys_iter = generate_integer_keys_iter(n, batch_op, p);
					self.batch_delete_u32(keys_iter).await
				}
				KeyProvider::UnorderedInteger(p) => {
					let keys_iter = generate_integer_keys_iter(n, batch_op, p);
					self.batch_delete_u32(keys_iter).await
				}
				KeyProvider::OrderedString(p) => {
					let keys_iter = generate_string_keys_iter(n, batch_op, p);
					self.batch_delete_string(keys_iter).await
				}
				KeyProvider::UnorderedString(p) => {
					let keys_iter = generate_string_keys_iter(n, batch_op, p);
					self.batch_delete_string(keys_iter).await
				}
			}
		}
	}

	/// Perform a batch create operation with numeric keys
	fn batch_create_u32(
		&self,
		_key_value_pairs: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> impl Future<Output = Result<()>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Perform a batch create operation with string keys
	fn batch_create_string(
		&self,
		_key_value_pairs: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> impl Future<Output = Result<()>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Perform a batch read operation with numeric keys
	fn batch_read_u32(
		&self,
		_keys: impl Iterator<Item = u32> + Send,
	) -> impl Future<Output = Result<()>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Perform a batch read operation with string keys
	fn batch_read_string(
		&self,
		_keys: impl Iterator<Item = String> + Send,
	) -> impl Future<Output = Result<()>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Perform a batch update operation with numeric keys
	fn batch_update_u32(
		&self,
		_key_value_pairs: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> impl Future<Output = Result<()>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Perform a batch update operation with string keys
	fn batch_update_string(
		&self,
		_key_value_pairs: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> impl Future<Output = Result<()>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Perform a batch delete operation with numeric keys
	fn batch_delete_u32(
		&self,
		_keys: impl Iterator<Item = u32> + Send,
	) -> impl Future<Output = Result<()>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Perform a batch delete operation with string keys
	fn batch_delete_string(
		&self,
		_keys: impl Iterator<Item = String> + Send,
	) -> impl Future<Output = Result<()>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}
}

/// Iterator for generating integer keys lazily
struct IntegerKeysIter<'a> {
	n: u32,
	batch_size: usize,
	current: usize,
	kp: &'a mut dyn IntegerKeyProvider,
}

impl<'a> Iterator for IntegerKeysIter<'a> {
	type Item = u32;

	fn next(&mut self) -> Option<Self::Item> {
		if self.current < self.batch_size {
			let sample_idx = self.n * self.batch_size as u32 + self.current as u32;
			let key = self.kp.key(sample_idx);
			self.current += 1;
			Some(key)
		} else {
			None
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.batch_size - self.current;
		(remaining, Some(remaining))
	}
}

impl<'a> ExactSizeIterator for IntegerKeysIter<'a> {}

/// Iterator for generating string keys lazily
struct StringKeysIter<'a> {
	n: u32,
	batch_size: usize,
	current: usize,
	kp: &'a mut dyn StringKeyProvider,
}

impl<'a> Iterator for StringKeysIter<'a> {
	type Item = String;

	fn next(&mut self) -> Option<Self::Item> {
		if self.current < self.batch_size {
			let sample_idx = self.n * self.batch_size as u32 + self.current as u32;
			let key = self.kp.key(sample_idx);
			self.current += 1;
			Some(key)
		} else {
			None
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.batch_size - self.current;
		(remaining, Some(remaining))
	}
}

impl<'a> ExactSizeIterator for StringKeysIter<'a> {}

/// Iterator for generating integer key-value pairs lazily
struct IntegerKeyValuesIter<'a> {
	n: u32,
	batch_size: usize,
	current: usize,
	kp: &'a mut dyn IntegerKeyProvider,
	vp: &'a mut crate::valueprovider::ValueProvider,
}

impl<'a> Iterator for IntegerKeyValuesIter<'a> {
	type Item = (u32, serde_json::Value);

	fn next(&mut self) -> Option<Self::Item> {
		if self.current < self.batch_size {
			let sample_idx = self.n * self.batch_size as u32 + self.current as u32;
			let key = self.kp.key(sample_idx);
			let value = self.vp.generate_value::<crate::dialect::DefaultDialect>();
			self.current += 1;
			Some((key, value))
		} else {
			None
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.batch_size - self.current;
		(remaining, Some(remaining))
	}
}

impl<'a> ExactSizeIterator for IntegerKeyValuesIter<'a> {}

/// Iterator for generating string key-value pairs lazily
struct StringKeyValuesIter<'a> {
	n: u32,
	batch_size: usize,
	current: usize,
	kp: &'a mut dyn StringKeyProvider,
	vp: &'a mut crate::valueprovider::ValueProvider,
}

impl<'a> Iterator for StringKeyValuesIter<'a> {
	type Item = (String, serde_json::Value);

	fn next(&mut self) -> Option<Self::Item> {
		if self.current < self.batch_size {
			let sample_idx = self.n * self.batch_size as u32 + self.current as u32;
			let key = self.kp.key(sample_idx);
			let value = self.vp.generate_value::<crate::dialect::DefaultDialect>();
			self.current += 1;
			Some((key, value))
		} else {
			None
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.batch_size - self.current;
		(remaining, Some(remaining))
	}
}

impl<'a> ExactSizeIterator for StringKeyValuesIter<'a> {}

/// Helper function to create an iterator for integer keys
fn generate_integer_keys_iter<'a>(
	n: u32,
	batch_op: &BatchOperation,
	kp: &'a mut dyn IntegerKeyProvider,
) -> IntegerKeysIter<'a> {
	IntegerKeysIter {
		n,
		batch_size: batch_op.batch_size,
		current: 0,
		kp,
	}
}

/// Helper function to create an iterator for string keys
fn generate_string_keys_iter<'a>(
	n: u32,
	batch_op: &BatchOperation,
	kp: &'a mut dyn StringKeyProvider,
) -> StringKeysIter<'a> {
	StringKeysIter {
		n,
		batch_size: batch_op.batch_size,
		current: 0,
		kp,
	}
}

/// Helper function to create an iterator for integer key-value pairs
fn generate_integer_key_values_iter<'a>(
	n: u32,
	batch_op: &BatchOperation,
	kp: &'a mut dyn IntegerKeyProvider,
	vp: &'a mut crate::valueprovider::ValueProvider,
) -> IntegerKeyValuesIter<'a> {
	IntegerKeyValuesIter {
		n,
		batch_size: batch_op.batch_size,
		current: 0,
		kp,
		vp,
	}
}

/// Helper function to create an iterator for string key-value pairs
fn generate_string_key_values_iter<'a>(
	n: u32,
	batch_op: &BatchOperation,
	kp: &'a mut dyn StringKeyProvider,
	vp: &'a mut crate::valueprovider::ValueProvider,
) -> StringKeyValuesIter<'a> {
	StringKeyValuesIter {
		n,
		batch_size: batch_op.batch_size,
		current: 0,
		kp,
		vp,
	}
}
