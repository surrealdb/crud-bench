#![cfg(feature = "mariadb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::{Dialect, MySqlDialect};
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, Index, KeyType, Projection, Scan};
use anyhow::{Result, bail};
use mysql_async::consts;
use mysql_async::prelude::Queryable;
use mysql_async::prelude::ToValue;
use mysql_async::{Conn, Opts, Row};
use serde_json::{Map, Value};
use std::hint::black_box;
use std::sync::Arc;
use tokio::sync::Mutex;

pub const DEFAULT: &str = "mysql://root:mariadb@127.0.0.1:3306/bench";

/// Calculate MariaDB specific memory allocation
fn calculate_mariadb_memory() -> (u64, u64, u64) {
	// Load the system memory
	let memory = Config::new();
	// Use ~100% of recommended cache allocation
	let buffer_pool_gb = memory.cache_gb;
	// Use ~10% of buffer pool, min 1GB, max 8GB
	let log_file_gb = (memory.cache_gb / 10).clamp(1, 8);
	// Use 1 buffer pool instance per 2GB, max 64
	let buffer_pool_instances = (buffer_pool_gb / 2).clamp(1, 64);
	// Return configuration
	(buffer_pool_gb, log_file_gb, buffer_pool_instances)
}

/// Returns the Docker parameters required to run a MariaDB instance for benchmarking,
/// with configuration optimized based on the provided benchmark options.
pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	// Calculate memory allocation
	let (buffer_pool_gb, log_file_gb, buffer_pool_instances) = calculate_mariadb_memory();
	DockerParams {
		image: "mariadb",
		pre_args: "--ulimit nofile=65536:65536 -p 127.0.0.1:3306:3306 -e MARIADB_ROOT_PASSWORD=mariadb -e MARIADB_DATABASE=bench".to_string(),
		post_args: match options.optimised {
			true => format!(
				"--max-connections=1024 \
				--innodb-buffer-pool-size={buffer_pool_gb}G \
				--innodb-buffer-pool-instances={buffer_pool_instances} \
				--innodb-log-file-size={log_file_gb}G \
				--innodb-log-buffer-size=256M \
				--innodb-flush-method=O_DIRECT \
				--innodb-io-capacity=2000 \
				--innodb-io-capacity-max=4000 \
				--innodb-read-io-threads=8 \
				--innodb-write-io-threads=8 \
				--innodb-thread-concurrency=32 \
				--innodb-purge-threads=4 \
				--table-open-cache=4000 \
				--sort-buffer-size=32M \
				--read-buffer-size=8M \
				--join-buffer-size=32M \
				--tmp-table-size=1G \
				--max-heap-table-size=1G \
				--innodb-adaptive-hash-index=ON \
				--innodb-use-native-aio=1 \
				--innodb-doublewrite=OFF \
				--sync_binlog={} \
				--innodb-flush-log-at-trx-commit={}",
				if options.sync {
					"1"
				} else {
					"0"
				},
				if options.sync {
					"1"
				} else {
					"0"
				}
			),
			false => format!(
				"--max-connections=1024 \
				--sync_binlog={} \
				--innodb-flush-log-at-trx-commit={}",
				if options.sync {
					"1"
				} else {
					"0"
				},
				if options.sync {
					"1"
				} else {
					"0"
				}
			),
		},
	}
}

pub(crate) struct MariadbClientProvider(KeyType, Columns, String);

impl BenchmarkEngine<MariadbClient> for MariadbClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, columns: Columns, options: &Benchmark) -> Result<Self> {
		// Get the custom endpoint if specified
		let url = options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned();
		// Create the client provider
		Ok(Self(kt, columns, url))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<MariadbClient> {
		let conn = Conn::new(Opts::from_url(&self.2)?).await?;
		Ok(MariadbClient {
			conn: Arc::new(Mutex::new(conn)),
			kt: self.0,
			columns: self.1.clone(),
		})
	}
}

pub(crate) struct MariadbClient {
	conn: Arc<Mutex<Conn>>,
	kt: KeyType,
	columns: Columns,
}

impl BenchmarkClient for MariadbClient {
	async fn startup(&self) -> Result<()> {
		let id_type = match self.kt {
			KeyType::Integer => "SERIAL",
			KeyType::String26 => "VARCHAR(26)",
			KeyType::String90 => "VARCHAR(90)",
			KeyType::String250 => "VARCHAR(250)",
			KeyType::String506 => "VARCHAR(506)",
			KeyType::Uuid => {
				todo!()
			}
		};
		let fields = self
			.columns
			.0
			.iter()
			.map(|(n, t)| {
				let n = MySqlDialect::escape_field(n.clone());
				match t {
					ColumnType::String => format!("{n} TEXT NOT NULL"),
					ColumnType::Integer => format!("{n} INTEGER NOT NULL"),
					ColumnType::Object => format!("{n} JSON NOT NULL"),
					ColumnType::Float => format!("{n} REAL NOT NULL"),
					ColumnType::DateTime => format!("{n} TIMESTAMP NOT NULL"),
					ColumnType::Uuid => format!("{n} UUID NOT NULL"),
					ColumnType::Bool => format!("{n} BOOL NOT NULL"),
				}
			})
			.collect::<Vec<String>>()
			.join(", ");
		let stm = format!(
			"DROP TABLE IF EXISTS record; CREATE TABLE record ( id {id_type} PRIMARY KEY, {fields}) ENGINE=InnoDB;"
		);
		self.conn.lock().await.query_drop(&stm).await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create(key as u64, val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read(key as u64).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read(key).await
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		self.update(key as u64, val).await
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		self.update(key, val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key as u64).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key).await
	}

	async fn build_index(&self, spec: &Index, name: &str) -> Result<()> {
		// Get the unique flag
		let unique = if spec.unique.unwrap_or(false) {
			"UNIQUE"
		} else {
			""
		}
		.to_string();
		// Get the fields
		let fields = spec.fields.join(", ");
		// Check if an index type is specified
		let stmt = match &spec.index_type {
			Some(kind) if kind == "fulltext" => {
				format!("CREATE FULLTEXT INDEX {name} ON record ({fields})")
			}
			Some(kind) => {
				format!("CREATE INDEX {name} USING {kind} ON record ({fields})")
			}
			None => {
				format!("CREATE {unique} INDEX {name} ON record ({fields})")
			}
		};
		// Create the index
		self.conn.lock().await.query_drop(&stmt).await?;
		// All ok
		Ok(())
	}

	async fn drop_index(&self, name: &str) -> Result<()> {
		let stmt = format!("DROP INDEX {name} ON record");
		self.conn.lock().await.query_drop(&stmt).await?;
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		self.scan(scan, ctx).await
	}

	async fn scan_string(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		self.scan(scan, ctx).await
	}

	async fn batch_create_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals.map(|(k, v)| (k as u64, v)).collect()).await
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals.collect()).await
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_read(keys.map(|k| k as u64).collect()).await
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_read(keys.collect()).await
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals.map(|(k, v)| (k as u64, v)).collect()).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals.collect()).await
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_delete(keys.map(|k| k as u64).collect()).await
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_delete(keys.collect()).await
	}
}

impl MariadbClient {
	fn consume(&self, mut row: Row) -> Result<Value> {
		let mut val: Map<String, Value> = Map::new();
		for (i, c) in row.columns().iter().enumerate() {
			val.insert(
				c.name_str().to_string(),
				match c.column_type() {
					consts::ColumnType::MYSQL_TYPE_TINY => {
						let v: Option<bool> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_SHORT => {
						let v: Option<bool> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_VARCHAR => {
						let v: Option<String> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_VAR_STRING => {
						let v: Option<String> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_STRING => {
						let v: Option<String> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_LONG => {
						let v: Option<i32> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_LONGLONG => {
						let v: Option<i64> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_FLOAT => {
						let v: Option<f32> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_DOUBLE => {
						let v: Option<f64> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_BLOB => {
						let v: Option<String> = row.take(i);
						Value::from(v)
					}
					consts::ColumnType::MYSQL_TYPE_JSON => {
						let v: Option<serde_json::Value> = row.take(i);
						Value::from(v)
					}
					c => {
						todo!("Not yet implemented {c:?}")
					}
				},
			);
		}
		Ok(val.into())
	}

	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: ToValue + Sync,
	{
		let (fields, values) = MySqlDialect::create_clause(&self.columns, val);
		let stm = format!("INSERT INTO record (id, {fields}) VALUES (?, {values})");
		let _: Vec<Row> = self.conn.lock().await.exec(stm, (key.to_value(),)).await?;
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<()>
	where
		T: ToValue + Sync,
	{
		let stm = "SELECT * FROM record WHERE id=?";
		let res: Vec<Row> = self.conn.lock().await.exec(stm, (key.to_value(),)).await?;
		assert_eq!(res.len(), 1);
		black_box(self.consume(res.into_iter().next().unwrap())?);
		Ok(())
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: ToValue + Sync,
	{
		let fields = MySqlDialect::update_clause(&self.columns, val);
		let stm = format!("UPDATE record SET {fields} WHERE id=?");
		let _: Vec<Row> = self.conn.lock().await.exec(stm, (key.to_value(),)).await?;
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: ToValue + Sync,
	{
		let stm = "DELETE FROM record WHERE id=?";
		let _: Vec<Row> = self.conn.lock().await.exec(stm, (key.to_value(),)).await?;
		Ok(())
	}

	async fn scan(&self, scan: &Scan, ctx: ScanContext) -> Result<usize> {
		// MariaDB requires a full-text index to run a MATCH query
		if ctx == ScanContext::WithoutIndex
			&& let Some(index) = &scan.index
			&& let Some(kind) = &index.index_type
			&& kind == "fulltext"
		{
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.map(|s| format!("OFFSET {}", s)).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {}", s)).unwrap_or_default();
		let c = MySqlDialect::filter_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("SELECT id FROM record {c} {l} {s}");
				let res: Vec<Row> = self.conn.lock().await.query(stm).await?;
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(self.consume(v).unwrap());
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let stm = format!("SELECT * FROM record {c} {l} {s}");
				let res: Vec<Row> = self.conn.lock().await.query(stm).await?;
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(self.consume(v).unwrap());
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let stm = format!("SELECT COUNT(*) FROM (SELECT id FROM record {c} {l} {s}) AS T");
				let res: Vec<Row> = self.conn.lock().await.query(stm).await?;
				let count: i64 = res.first().unwrap().get(0).unwrap();
				Ok(count as usize)
			}
		}
	}

	/// Helper function to execute a statement and verify the affected rows count
	async fn exec_and_verify(
		&self,
		stm: String,
		params: Vec<mysql_async::Value>,
		expected_count: usize,
		operation: &str,
	) -> Result<()> {
		let mut conn = self.conn.lock().await;
		let result = conn.exec_iter(stm, params).await?;
		let affected = result.affected_rows();
		drop(result);
		if affected != expected_count as u64 {
			return Err(anyhow::anyhow!(
				"{}: expected {} rows affected, got {}",
				operation,
				expected_count,
				affected
			));
		}
		Ok(())
	}

	async fn batch_create<T>(&self, key_vals: Vec<(T, Value)>) -> Result<()>
	where
		T: ToValue + Sync,
	{
		if key_vals.is_empty() {
			return Ok(());
		}
		let expected_count = key_vals.len();
		let columns = self
			.columns
			.0
			.iter()
			.map(|(name, _)| MySqlDialect::escape_field(name.clone()))
			.collect::<Vec<String>>()
			.join(", ");
		let placeholders = (0..expected_count)
			.map(|_| {
				let value_placeholders =
					(0..self.columns.0.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
				format!("(?, {value_placeholders})")
			})
			.collect::<Vec<_>>()
			.join(", ");
		let stm = format!("INSERT INTO record (id, {columns}) VALUES {placeholders}");
		let mut params: Vec<mysql_async::Value> = Vec::new();
		for (key, val) in key_vals {
			params.push(key.to_value());
			if let Value::Object(map) = val {
				for (name, _) in &self.columns.0 {
					if let Some(v) = map.get(name) {
						params.push(match v {
							Value::Null => mysql_async::Value::NULL,
							Value::Bool(b) => mysql_async::Value::Int(*b as i64),
							Value::Number(n) => {
								if let Some(i) = n.as_i64() {
									mysql_async::Value::Int(i)
								} else if let Some(f) = n.as_f64() {
									mysql_async::Value::Double(f)
								} else {
									mysql_async::Value::NULL
								}
							}
							Value::String(s) => mysql_async::Value::Bytes(s.as_bytes().to_vec()),
							Value::Array(_) | Value::Object(_) => mysql_async::Value::Bytes(
								serde_json::to_string(v).unwrap().as_bytes().to_vec(),
							),
						});

					} else {
						return Err(anyhow::anyhow!("Missing value for column {}", name));
					}
				}
			}
		}
		self.exec_and_verify(stm, params, expected_count, "batch_create").await
	}

	async fn batch_read<T>(&self, keys: Vec<T>) -> Result<()>
	where
		T: ToValue + Sync,
	{
		if keys.is_empty() {
			return Ok(());
		}
		let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
		let stm = format!("SELECT * FROM record WHERE id IN ({placeholders})");
		let params: Vec<mysql_async::Value> = keys.iter().map(|k| k.to_value()).collect();
		let res: Vec<Row> = self.conn.lock().await.exec(stm, params).await?;
		assert_eq!(res.len(), keys.len());
		for row in res {
			black_box(self.consume(row).unwrap());
		}
		Ok(())
	}

	async fn batch_update<T>(&self, key_vals: Vec<(T, Value)>) -> Result<()>
	where
		T: ToValue + Sync,
	{
		if key_vals.is_empty() {
			return Ok(());
		}
		let expected_count = key_vals.len();
		let columns = self
			.columns
			.0
			.iter()
			.map(|(name, _)| MySqlDialect::escape_field(name.clone()))
			.collect::<Vec<String>>();
		let case_statements = columns
			.iter()
			.map(|col| {
				let when_clauses =
					(0..key_vals.len()).map(|_| "WHEN id = ? THEN ?").collect::<Vec<_>>().join(" ");
				format!("{col} = CASE {when_clauses} ELSE {col} END")
			})
			.collect::<Vec<_>>()
			.join(", ");
		let id_placeholders = key_vals.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
		let stm = format!("UPDATE record SET {case_statements} WHERE id IN ({id_placeholders})");
		let mut params: Vec<mysql_async::Value> = Vec::new();
		for (name, _) in &self.columns.0 {
			for (key, val) in &key_vals {
				params.push(key.to_value());
				if let Value::Object(map) = val
					&& let Some(v) = map.get(name)
				{
					params.push(match v {
						Value::Null => mysql_async::Value::NULL,
						Value::Bool(b) => mysql_async::Value::Int(*b as i64),
						Value::Number(n) => {
							if let Some(i) = n.as_i64() {
								mysql_async::Value::Int(i)
							} else if let Some(f) = n.as_f64() {
								mysql_async::Value::Double(f)
							} else {
								mysql_async::Value::NULL
							}
						}
						Value::String(s) => mysql_async::Value::Bytes(s.as_bytes().to_vec()),
						Value::Array(_) | Value::Object(_) => mysql_async::Value::Bytes(
							serde_json::to_string(v).unwrap().as_bytes().to_vec(),
						),
					});
				} else {
						return Err(anyhow::anyhow!("Missing value for column {}", name));
					}
			}
		}
		for (key, _) in &key_vals {
			params.push(key.to_value());
		}
		self.exec_and_verify(stm, params, expected_count, "batch_update").await
	}

	async fn batch_delete<T>(&self, keys: Vec<T>) -> Result<()>
	where
		T: ToValue + Sync,
	{
		if keys.is_empty() {
			return Ok(());
		}
		let expected_count = keys.len();
		let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
		let stm = format!("DELETE FROM record WHERE id IN ({placeholders})");
		let params: Vec<mysql_async::Value> = keys.iter().map(|k| k.to_value()).collect();
		self.exec_and_verify(stm, params, expected_count, "batch_delete").await
	}
}
