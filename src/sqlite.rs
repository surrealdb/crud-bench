#![cfg(feature = "sqlite")]

use crate::dialect::{AnsiSqlDialect, Dialect};
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::memory::Config;
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, Index, KeyType, Projection, Scan};
use anyhow::Result;
use serde_json::{Map, Value as Json};
use std::borrow::Cow;
use std::cmp::max;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tokio_rusqlite::Connection;
use tokio_rusqlite::types::ToSqlOutput;
use tokio_rusqlite::types::Value;

const DATABASE_DIR: &str = "sqlite";

const MIN_CACHE_SIZE: u64 = 512 * 1024 * 1024;

// We can't just return `tokio_rusqlite::Row` because it's not Send/Sync
type Row = Vec<(String, Value)>;

/// Calculate SQLite specific memory allocation
fn calculate_sqlite_memory() -> u64 {
	// Load the system memory
	let memory = Config::new();
	// Get the total cache size in bytes
	let cache = memory.cache_gb * 1024 * 1024 * 1024;
	// Ensure minimum cache size of 512MB
	max(cache, MIN_CACHE_SIZE)
}

pub(crate) struct SqliteClientProvider {
	conn: Arc<Connection>,
	kt: KeyType,
	columns: Columns,
	sync: bool,
}

impl BenchmarkEngine<SqliteClient> for SqliteClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, columns: Columns, options: &Benchmark) -> Result<Self> {
		// Remove the database directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Recreate the database directory
		std::fs::create_dir(DATABASE_DIR)?;
		// Switch to the new directory
		let path = format!("{DATABASE_DIR}/db");
		// Create the connection
		let conn = Connection::open(&path).await?;
		// Create the store
		Ok(Self {
			conn: Arc::new(conn),
			kt,
			columns,
			sync: options.sync,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<SqliteClient> {
		Ok(SqliteClient {
			conn: self.conn.clone(),
			kt: self.kt,
			columns: self.columns.clone(),
			sync: self.sync,
		})
	}
}

pub(crate) struct SqliteClient {
	conn: Arc<Connection>,
	kt: KeyType,
	columns: Columns,
	sync: bool,
}

impl BenchmarkClient for SqliteClient {
	async fn shutdown(&self) -> Result<()> {
		// Remove the database directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Ok
		Ok(())
	}

	async fn startup(&self) -> Result<()> {
		// Calculate the size of the page cache
		let cache = calculate_sqlite_memory() / 16384;
		// Configure SQLite with optimized settings
		let stmt = format!(
			"
			PRAGMA synchronous = {};
			PRAGMA journal_mode = WAL;
			PRAGMA page_size = 16384;
			PRAGMA cache_size = {cache};
			PRAGMA locking_mode = EXCLUSIVE;
		",
			if self.sync {
				"ON"
			} else {
				"NORMAL"
			}
		);
		self.execute_batch(Cow::Owned(stmt)).await?;
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
				let n = AnsiSqlDialect::escape_field(n.clone());
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
			.join(",");
		let stmt = format!(
			"
		    DROP TABLE IF EXISTS record;
		    CREATE TABLE record ( id {id_type} PRIMARY KEY, {fields});
		"
		);
		self.execute_batch(Cow::Owned(stmt)).await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Json) -> Result<()> {
		self.create(key.into(), val).await
	}

	async fn create_string(&self, key: String, val: Json) -> Result<()> {
		self.create(key.into(), val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read(key.into()).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read(key.into()).await
	}

	async fn update_u32(&self, key: u32, val: Json) -> Result<()> {
		self.update(key.into(), val).await
	}

	async fn update_string(&self, key: String, val: Json) -> Result<()> {
		self.update(key.into(), val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key.into()).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key.into()).await
	}

	fn build_index(&self, spec: &Index, name: &str) -> impl Future<Output = Result<()>> + Send {
		let fields = spec.fields.join(", ");
		let index_name = name.to_string();
		let unique = if spec.unique.unwrap_or(false) {
			"UNIQUE"
		} else {
			""
		}
		.to_string();
		let stmt = format!("CREATE {unique} INDEX {index_name} ON record ({fields})");
		async move {
			self.execute_batch(Cow::Owned(stmt)).await?;
			Ok(())
		}
	}

	fn drop_index(&self, name: &str) -> impl Future<Output = Result<()>> + Send {
		let stmt = format!("DROP INDEX IF EXISTS {name}");
		async move {
			self.execute_batch(Cow::Owned(stmt)).await?;
			Ok(())
		}
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn batch_create_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals.collect()).await
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals.collect()).await
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_read(keys.collect()).await
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_read(keys.collect()).await
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals.collect()).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals.collect()).await
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		self.batch_delete(keys.collect()).await
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		self.batch_delete(keys.collect()).await
	}
}

impl SqliteClient {
	async fn execute_batch(&self, query: Cow<'static, str>) -> Result<()> {
		self.conn.call(move |conn| conn.execute_batch(query.as_ref()).map_err(Into::into)).await?;
		Ok(())
	}

	async fn execute(
		&self,
		query: Cow<'static, str>,
		params: ToSqlOutput<'static>,
	) -> Result<usize> {
		self.conn
			.call(move |conn| conn.execute(query.as_ref(), [&params]).map_err(Into::into))
			.await
			.map_err(Into::into)
	}

	async fn query(
		&self,
		stmt: Cow<'static, str>,
		params: Option<ToSqlOutput<'static>>,
	) -> Result<Vec<Row>> {
		self.conn
			.call(move |conn| {
				let mut stmt = conn.prepare(stmt.as_ref())?;
				let mut rows = match params {
					Some(params) => stmt.query([&params])?,
					None => stmt.query(())?,
				};
				let mut vec = Vec::new();
				while let Some(row) = rows.next()? {
					let names = row.as_ref().column_names();
					let mut map = Vec::with_capacity(names.len());
					for (i, name) in names.into_iter().enumerate() {
						map.push((name.to_owned(), row.get(i)?));
					}
					vec.push(map);
				}
				Ok(vec)
			})
			.await
			.map_err(Into::into)
	}

	async fn create(&self, key: ToSqlOutput<'static>, val: Json) -> Result<()> {
		let (fields, values) = AnsiSqlDialect::create_clause(&self.columns, val);
		let stmt = format!("INSERT INTO record (id, {fields}) VALUES ($1, {values})");
		let res = self.execute(Cow::Owned(stmt), key).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn read(&self, key: ToSqlOutput<'static>) -> Result<()> {
		let stm = "SELECT * FROM record WHERE id=$1";
		let res = self.query(Cow::Borrowed(stm), Some(key)).await?;
		assert_eq!(res.len(), 1);
		Ok(())
	}

	async fn update(&self, key: ToSqlOutput<'static>, val: Json) -> Result<()> {
		let fields = AnsiSqlDialect::update_clause(&self.columns, val);
		let stmt = format!("UPDATE record SET {fields} WHERE id=$1");
		let res = self.execute(Cow::Owned(stmt), key).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn delete(&self, key: ToSqlOutput<'static>) -> Result<()> {
		let stmt = "DELETE FROM record WHERE id=$1";
		let res = self.execute(Cow::Borrowed(stmt), key).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	fn consume(&self, row: Row) -> Json {
		let mut val = Map::new();
		for (key, value) in row {
			val.insert(
				key,
				match value {
					Value::Null => Json::Null,
					Value::Integer(int) => int.into(),
					Value::Real(float) => float.into(),
					Value::Text(text) => text.into(),
					Value::Blob(vec) => vec.into(),
				},
			);
		}
		val.into()
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.map(|s| format!("OFFSET {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = AnsiSqlDialect::filter_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("SELECT id FROM record {c} {l} {s}");
				let res = self.query(Cow::Owned(stm), None).await?;
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(self.consume(v));
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let stm = format!("SELECT * FROM record {c} {l} {s}");
				let res = self.query(Cow::Owned(stm), None).await?;
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(self.consume(v));
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let stm = format!("SELECT COUNT(*) FROM (SELECT id FROM record {c} {l} {s})");
				let res = self.query(Cow::Owned(stm), None).await?;
				let Value::Integer(count) = res.first().unwrap().first().unwrap().1 else {
					panic!("Unexpected response type `{res:?}`");
				};
				Ok(count as usize)
			}
		}
	}

	async fn batch_create<T>(&self, key_vals: Vec<(T, Json)>) -> Result<()>
	where
		T: Into<ToSqlOutput<'static>> + Send + 'static,
	{
		// Fetch the columns to insert
		let columns = self
			.columns
			.0
			.iter()
			.map(|(name, _)| AnsiSqlDialect::escape_field(name.clone()))
			.collect::<Vec<String>>()
			.join(", ");

		let conn = self.conn.clone();
		let column_defs = self.columns.clone();

		conn.call(move |conn| {
			// Store the records to insert
			let mut inserts = Vec::with_capacity(key_vals.len());
			// Store all parameter values as owned types
			let mut all_params: Vec<Box<dyn tokio_rusqlite::types::ToSql>> = Vec::new();
			// Store the parameter index
			let mut param_index = 1;

			// Iterate over the key-value pairs
			for (key, val) in key_vals {
				// Add the id parameter
				let mut row = vec![format!("${param_index}")];
				param_index += 1;
				all_params.push(Box::new(key.into()));

				// Process the columns
				if let Json::Object(obj) = val {
					for (column, column_type) in &column_defs.0 {
						// Add the column placeholder
						row.push(format!("${param_index}"));
						param_index += 1;

						// Add the column value with proper type conversion
						if let Some(value) = obj.get(column) {
							let param = convert_json_to_sqlite_param(column_type, value)
								.expect("Failed to convert JSON value to SQL parameter");
							all_params.push(param);
						} else {
							panic!("Missing value for column {column}");
						}
					}
				}
				// Add the row to the inserts
				inserts.push(format!("({})", row.join(", ")));
			}

			// Build the INSERT statement
			let stmt = format!("INSERT INTO record (id, {columns}) VALUES {}", inserts.join(", "));

			// Convert boxed values to references for execute
			let params: Vec<&dyn tokio_rusqlite::types::ToSql> =
				all_params.iter().map(|p| p.as_ref()).collect();

			// Execute the statement
			let count = conn.execute(&stmt, params.as_slice())?;
			assert_eq!(count, inserts.len());
			Ok(())
		})
		.await
		.map_err(Into::into)
	}

	async fn batch_read<T>(&self, keys: Vec<T>) -> Result<()>
	where
		T: Into<ToSqlOutput<'static>> + Send + 'static,
	{
		let conn = self.conn.clone();

		conn.call(move |conn| {
			// Build the IN clause with positional parameters
			let ids = (1..=keys.len()).map(|i| format!("${i}")).collect::<Vec<String>>().join(", ");

			// Convert keys to ToSql parameters
			let mut all_params: Vec<Box<dyn tokio_rusqlite::types::ToSql>> = Vec::new();
			for key in keys {
				all_params.push(Box::new(key.into()));
			}

			// Build and execute the SELECT statement
			let stmt = format!("SELECT * FROM record WHERE id IN ({ids})");
			let params: Vec<&dyn tokio_rusqlite::types::ToSql> =
				all_params.iter().map(|p| p.as_ref()).collect();

			let mut prepared_stmt = conn.prepare(&stmt)?;
			let mut rows = prepared_stmt.query(params.as_slice())?;

			let mut count = 0;
			while let Some(row) = rows.next()? {
				// Consume the row
				let names = row.as_ref().column_names();
				let mut map = Vec::with_capacity(names.len());
				for (i, name) in names.into_iter().enumerate() {
					map.push((name.to_owned(), row.get(i)?));
				}

				let mut val = Map::new();
				for (key, value) in map {
					val.insert(
						key,
						match value {
							Value::Null => Json::Null,
							Value::Integer(int) => int.into(),
							Value::Real(float) => float.into(),
							Value::Text(text) => text.into(),
							Value::Blob(vec) => vec.into(),
						},
					);
				}
				black_box(Json::from(val));
				count += 1;
			}

			assert_eq!(count, all_params.len());
			Ok(())
		})
		.await
		.map_err(Into::into)
	}

	async fn batch_update<T>(&self, key_vals: Vec<(T, Json)>) -> Result<()>
	where
		T: Into<ToSqlOutput<'static>> + Send + 'static,
	{
		let conn = self.conn.clone();
		let column_defs = self.columns.clone();

		conn.call(move |conn| {
			// For SQLite, we'll use multiple UPDATE statements in a transaction
			// since SQLite doesn't support UPDATE FROM as elegantly as PostgreSQL

			// Start a transaction
			conn.execute_batch("BEGIN")?;

			let result = (|| {
				for (key, val) in key_vals {
					if let Json::Object(obj) = val {
						// Build the SET clause
						let mut set_parts = Vec::new();
						let mut params: Vec<Box<dyn tokio_rusqlite::types::ToSql>> = Vec::new();
						let mut param_index = 1;

						for (column, column_type) in &column_defs.0 {
							let escaped_col = AnsiSqlDialect::escape_field(column.clone());
							set_parts.push(format!("{escaped_col} = ${param_index}"));
							param_index += 1;

							if let Some(value) = obj.get(column) {
								let param = convert_json_to_sqlite_param(column_type, value)
									.expect("Failed to convert JSON value to SQL parameter");
								params.push(param);
							} else {
								panic!("Missing value for column {column}");
							}
						}

						// Add the key parameter at the end
						params.push(Box::new(key.into()));

						// Build and execute the UPDATE statement
						let stmt = format!(
							"UPDATE record SET {} WHERE id = ${param_index}",
							set_parts.join(", ")
						);
						let param_refs: Vec<&dyn tokio_rusqlite::types::ToSql> =
							params.iter().map(|p| p.as_ref()).collect();

						let count = conn.execute(&stmt, param_refs.as_slice())?;
						assert_eq!(count, 1);
					}
				}
				Ok(())
			})();

			// Commit or rollback based on result
			match result {
				Ok(_) => {
					conn.execute_batch("COMMIT")?;
					Ok(())
				}
				Err(e) => {
					conn.execute_batch("ROLLBACK").ok();
					Err(e)
				}
			}
		})
		.await
		.map_err(Into::into)
	}

	async fn batch_delete<T>(&self, keys: Vec<T>) -> Result<()>
	where
		T: Into<ToSqlOutput<'static>> + Send + 'static,
	{
		let conn = self.conn.clone();

		conn.call(move |conn| {
			// Build the IN clause with positional parameters
			let ids = (1..=keys.len()).map(|i| format!("${i}")).collect::<Vec<String>>().join(", ");

			// Convert keys to ToSql parameters
			let mut all_params: Vec<Box<dyn tokio_rusqlite::types::ToSql>> = Vec::new();
			for key in keys {
				all_params.push(Box::new(key.into()));
			}

			// Build and execute the DELETE statement
			let stmt = format!("DELETE FROM record WHERE id IN ({ids})");
			let params: Vec<&dyn tokio_rusqlite::types::ToSql> =
				all_params.iter().map(|p| p.as_ref()).collect();

			let count = conn.execute(&stmt, params.as_slice())?;
			assert_eq!(count, all_params.len());
			Ok(())
		})
		.await
		.map_err(Into::into)
	}
}

/// Convert a JSON value to a SQLite parameter based on column type
fn convert_json_to_sqlite_param(
	column_type: &ColumnType,
	json_value: &Json,
) -> Result<Box<dyn tokio_rusqlite::types::ToSql>> {
	match column_type {
		ColumnType::Integer => {
			if let Some(int_val) = json_value.as_i64() {
				Ok(Box::new(int_val))
			} else {
				Err(anyhow::anyhow!("Expected integer"))
			}
		}
		ColumnType::Float => {
			if let Some(float_val) = json_value.as_f64() {
				Ok(Box::new(float_val))
			} else {
				Err(anyhow::anyhow!("Expected float"))
			}
		}
		ColumnType::Bool => {
			if let Some(bool_val) = json_value.as_bool() {
				Ok(Box::new(bool_val))
			} else {
				Err(anyhow::anyhow!("Expected boolean"))
			}
		}
		ColumnType::String => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow::anyhow!("Expected string"))
			}
		}
		ColumnType::Object => {
			// SQLite stores JSON as text
			Ok(Box::new(json_value.to_string()))
		}
		ColumnType::DateTime => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow::anyhow!("Expected datetime string"))
			}
		}
		ColumnType::Uuid => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow::anyhow!("Expected UUID string"))
			}
		}
	}
}
