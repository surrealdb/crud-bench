#![cfg(feature = "sqlite")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::{AnsiSqlDialect, Dialect, SqliteDialect};
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::memory::Config;
use crate::util::sql::bench_to_sqlite_param;
use crate::value::BenchValue;
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, Index, KeyType, Projection, Scan};
use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::borrow::Cow;
use std::cmp::max;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tokio_rusqlite::types::ToSqlOutput;
use tokio_rusqlite::types::Value;
use tokio_rusqlite::{Connection, rusqlite};
use uuid::Uuid;

const DATABASE_DIR: &str = "sqlite";

const MIN_CACHE_SIZE: u64 = 512 * 1024 * 1024;

// We can't just return `tokio_rusqlite::Row` because it's not Send/Sync
type Row = Vec<(String, Value)>;

/// Map a SQLite [`Value`] to [`BenchValue`] using schema types (BOOL is INTEGER in SQLite).
fn sqlite_cell_to_bench(columns: &Columns, key: &str, value: Value) -> BenchValue {
	let ty = columns.0.iter().find(|(n, _)| n == key).map(|(_, t)| t);
	match (ty, value) {
		(Some(ColumnType::Bool), Value::Integer(i)) => BenchValue::Bool(i != 0),
		(Some(ColumnType::DateTime), Value::Text(s)) => match s.parse::<DateTime<Utc>>() {
			Ok(dt) => BenchValue::DateTime(dt),
			Err(_) => BenchValue::String(s),
		},
		(Some(ColumnType::Uuid), Value::Text(s)) => match Uuid::parse_str(&s) {
			Ok(u) => BenchValue::Uuid(u),
			Err(_) => BenchValue::String(s),
		},
		(Some(ColumnType::Decimal), Value::Text(s)) => match s.parse::<Decimal>() {
			Ok(d) => BenchValue::Decimal(d),
			Err(_) => BenchValue::String(s),
		},
		(_, Value::Null) => BenchValue::Null,
		(_, Value::Integer(int)) => BenchValue::Int(int),
		(_, Value::Real(float)) => BenchValue::Float(float),
		(_, Value::Text(text)) => BenchValue::String(text),
		(_, Value::Blob(vec)) => BenchValue::Bytes(vec),
	}
}

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
	// The return type when reading a row
	type ReadRow = BenchValue;

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
					ColumnType::Array => format!("{n} JSON NOT NULL"),
					ColumnType::Float => format!("{n} REAL NOT NULL"),
					ColumnType::DateTime => format!("{n} TIMESTAMP NOT NULL"),
					ColumnType::Uuid => format!("{n} UUID NOT NULL"),
					ColumnType::Decimal => format!("{n} TEXT NOT NULL"),
					ColumnType::Bool => format!("{n} BOOL NOT NULL"),
					ColumnType::Bytes => format!("{n} BLOB NOT NULL"),
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

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.create(key.into(), val).await
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.create(key.into(), val).await
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		self.read(key.into()).await
	}

	async fn read_string(&self, key: String) -> Result<BenchValue> {
		self.read(key.into()).await
	}

	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.update(key.into(), val).await
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.update(key.into(), val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key.into()).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key.into()).await
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
		let fields = SqliteDialect::btree_index_key_list(&self.columns, spec);
		// Check if an index type is specified
		let stmt = match &spec.index_type {
			Some(kind) if kind == "fulltext" => {
				bail!(NOT_SUPPORTED_ERROR)
			}
			_ => {
				format!("CREATE {unique} INDEX {name} ON record ({fields})")
			}
		};
		// Create the index
		self.execute_batch(Cow::Owned(stmt)).await?;
		// All ok
		Ok(())
	}

	async fn drop_index(&self, name: &str) -> Result<()> {
		let stmt = format!("DROP INDEX IF EXISTS {name}");
		self.execute_batch(Cow::Owned(stmt)).await?;
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
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_create(key_vals.collect()).await
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
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
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		self.batch_update(key_vals.collect()).await
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
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
		self.conn.call(move |conn| conn.execute_batch(query.as_ref())).await?;
		Ok(())
	}

	async fn execute(
		&self,
		query: Cow<'static, str>,
		params: ToSqlOutput<'static>,
	) -> Result<usize> {
		self.conn
			.call(move |conn| conn.execute(query.as_ref(), [&params]))
			.await
			.map_err(Into::into)
	}

	async fn execute_params(
		&self,
		query: Cow<'static, str>,
		params: Vec<Box<dyn tokio_rusqlite::types::ToSql + Send + Sync>>,
	) -> Result<usize> {
		self.conn
			.call(move |conn| {
				let refs: Vec<&dyn tokio_rusqlite::types::ToSql> = params
					.iter()
					.map(|p| p.as_ref() as &dyn tokio_rusqlite::types::ToSql)
					.collect();
				conn.execute(query.as_ref(), refs.as_slice())
			})
			.await
			.map_err(Into::into)
	}

	async fn query(
		&self,
		stmt: Cow<'static, str>,
		params: Option<ToSqlOutput<'static>>,
	) -> Result<Vec<Row>> {
		self.conn
			.call(move |conn| -> rusqlite::Result<Vec<Row>> {
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

	fn consume(&self, row: Row) -> BenchValue {
		let mut val: Vec<(String, BenchValue)> = Vec::with_capacity(row.len());
		for (key, value) in row {
			let bv = sqlite_cell_to_bench(&self.columns, &key, value);
			val.push((key, bv));
		}
		BenchValue::Object(val)
	}

	async fn create(&self, key: ToSqlOutput<'static>, val: BenchValue) -> Result<()> {
		let obj = val.into_object()?;
		let columns = self
			.columns
			.0
			.iter()
			.map(|(name, _)| AnsiSqlDialect::escape_field(name.clone()))
			.collect::<Vec<String>>()
			.join(", ");
		let placeholders = (2..=1 + self.columns.0.len())
			.map(|i| format!("${i}"))
			.collect::<Vec<String>>()
			.join(", ");
		let stm = format!("INSERT INTO record (id, {columns}) VALUES ($1, {placeholders})");
		let mut params: Vec<Box<dyn tokio_rusqlite::types::ToSql + Send + Sync>> =
			vec![Box::new(key)];
		for (column, column_type) in &self.columns.0 {
			let v = obj
				.iter()
				.find(|(k, _)| k == column)
				.map(|(_, v)| v)
				.ok_or_else(|| anyhow!("Missing value for column {column}"))?;
			params.push(bench_to_sqlite_param(column_type, v)?);
		}
		let res = self.execute_params(Cow::Owned(stm), params).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn read(&self, key: ToSqlOutput<'static>) -> Result<BenchValue> {
		let stm = "SELECT * FROM record WHERE id=$1";
		let mut res = self.query(Cow::Borrowed(stm), Some(key)).await?;
		assert_eq!(res.len(), 1);
		let row = res.pop().expect("one row");
		Ok(black_box(self.consume(row)))
	}

	async fn update(&self, key: ToSqlOutput<'static>, val: BenchValue) -> Result<()> {
		let obj = val.into_object()?;
		let n = self.columns.0.len();
		let set = self
			.columns
			.0
			.iter()
			.enumerate()
			.map(|(i, (name, _))| {
				format!("{} = ${}", AnsiSqlDialect::escape_field(name.clone()), i + 1)
			})
			.collect::<Vec<String>>()
			.join(", ");
		let stm = format!("UPDATE record SET {set} WHERE id = ${}", n + 1);
		let mut params: Vec<Box<dyn tokio_rusqlite::types::ToSql + Send + Sync>> = Vec::new();
		for (column, column_type) in &self.columns.0 {
			let v = obj
				.iter()
				.find(|(k, _)| k == column)
				.map(|(_, v)| v)
				.ok_or_else(|| anyhow!("Missing value for column {column}"))?;
			params.push(bench_to_sqlite_param(column_type, v)?);
		}
		params.push(Box::new(key));
		let res = self.execute_params(Cow::Owned(stm), params).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn delete(&self, key: ToSqlOutput<'static>) -> Result<()> {
		let stm = "DELETE FROM record WHERE id=$1";
		let res = self.execute(Cow::Borrowed(stm), key).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn scan(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		// SQLite doesn't yet support full-text indexes
		if let Some(index) = &scan.with_index
			&& let Some(kind) = &index.index_type
			&& kind == "fulltext"
		{
			bail!(NOT_SUPPORTED_ERROR);
		}
		// Extract parameters
		let s = scan.start.map(|s| format!("OFFSET {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = SqliteDialect::filter_clause(scan)?;
		let o = AnsiSqlDialect::order_by_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("SELECT id FROM record {c} {o} {l} {s}");
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
				let stm = format!("SELECT * FROM record {c} {o} {l} {s}");
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

	async fn batch_create<T>(&self, key_vals: Vec<(T, BenchValue)>) -> Result<()>
	where
		T: Into<ToSqlOutput<'static>> + Send + 'static,
	{
		// Clone the connection
		let conn = self.conn.clone();
		// Fetch the columns to update
		let column_defs = self.columns.clone();
		// Execute the batch update on the connection
		conn.call(move |conn| -> rusqlite::Result<()> {
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
				if let BenchValue::Object(obj) = val {
					for (column, column_type) in &column_defs.0 {
						// Add the column placeholder
						row.push(format!("${param_index}"));
						param_index += 1;
						// Add the column value with proper type conversion
						if let Some(value) = obj.iter().find(|(k, _)| k == column).map(|(_, v)| v) {
							let param = bench_to_sqlite_param(column_type, value)
								.expect("Failed to convert BenchValue to SQL parameter");
							all_params.push(param);
						} else {
							panic!("Missing value for column {column}");
						}
					}
				}
				// Add the row to the inserts
				inserts.push(format!("({})", row.join(", ")));
			}
			// Fetch the columns to insert
			let columns = column_defs
				.0
				.iter()
				.map(|(name, _)| AnsiSqlDialect::escape_field(name.clone()))
				.collect::<Vec<String>>()
				.join(", ");
			// Build the INSERT statement
			let stmt = format!("INSERT INTO record (id, {columns}) VALUES {}", inserts.join(", "));
			// Convert boxed values to references for execute
			let params: Vec<&dyn tokio_rusqlite::types::ToSql> =
				all_params.iter().map(|p| p.as_ref()).collect();
			// Execute the statement
			let count = conn.execute(&stmt, params.as_slice())?;
			// Check the number of rows inserted
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
		// Clone the connection
		let conn = self.conn.clone();
		// Fetch the columns to read
		let column_defs = self.columns.clone();
		// Execute the batch read on the connection
		conn.call(move |conn| -> rusqlite::Result<()> {
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
			// Prepare the statement
			let mut prepared_stmt = conn.prepare(&stmt)?;
			let mut rows = prepared_stmt.query(params.as_slice())?;
			// Iterate over the rows
			let mut count = 0;
			while let Some(row) = rows.next()? {
				// Consume the row
				let names = row.as_ref().column_names();
				let mut map = Vec::with_capacity(names.len());
				for (i, name) in names.into_iter().enumerate() {
					map.push((name.to_owned(), row.get(i)?));
				}
				// Build the typed bench value
				let mut val: Vec<(String, BenchValue)> = Vec::with_capacity(map.len());
				for (key, value) in map {
					let bv = sqlite_cell_to_bench(&column_defs, &key, value);
					val.push((key, bv));
				}
				black_box(BenchValue::Object(val));
				count += 1;
			}
			// Check the number of rows read
			assert_eq!(count, all_params.len());
			Ok(())
		})
		.await
		.map_err(Into::into)
	}

	async fn batch_update<T>(&self, key_vals: Vec<(T, BenchValue)>) -> Result<()>
	where
		T: Into<ToSqlOutput<'static>> + Send + 'static,
	{
		// Clone the connection
		let conn = self.conn.clone();
		// Fetch the columns to update
		let column_defs = self.columns.clone();
		// Execute the batch update on the connection
		conn.call(move |conn| -> rusqlite::Result<()> {
			// Start a transaction
			conn.execute_batch("BEGIN")?;
			// Build and execute the statement
			let result = (|| {
				for (key, val) in key_vals {
					if let BenchValue::Object(obj) = val {
						// Build the SET clause
						let mut set_parts = Vec::new();
						let mut params: Vec<Box<dyn tokio_rusqlite::types::ToSql>> = Vec::new();
						let mut param_index = 1;
						// Iterate over the columns to update
						for (column, column_type) in &column_defs.0 {
							let escaped_col = AnsiSqlDialect::escape_field(column.clone());
							set_parts.push(format!("{escaped_col} = ${param_index}"));
							param_index += 1;
							// Add the column value with proper type conversion
							if let Some(value) =
								obj.iter().find(|(k, _)| k == column).map(|(_, v)| v)
							{
								let param = bench_to_sqlite_param(column_type, value)
									.expect("Failed to convert BenchValue to SQL parameter");
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
						// Convert boxed values to references for execute
						let param_refs: Vec<&dyn tokio_rusqlite::types::ToSql> =
							params.iter().map(|p| p.as_ref()).collect();
						// Execute the statement
						let count = conn.execute(&stmt, param_refs.as_slice())?;
						// Check the number of rows updated
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
		// Clone the connection
		let conn = self.conn.clone();
		// Execute the batch delete on the connection
		conn.call(move |conn| -> rusqlite::Result<()> {
			// Build the IN clause with positional parameters
			let ids = (1..=keys.len()).map(|i| format!("${i}")).collect::<Vec<String>>().join(", ");
			// Convert keys to ToSql parameters
			let mut all_params: Vec<Box<dyn tokio_rusqlite::types::ToSql>> = Vec::new();
			for key in keys {
				all_params.push(Box::new(key.into()));
			}
			// Build the DELETE statement
			let stmt = format!("DELETE FROM record WHERE id IN ({ids})");
			// Convert boxed values to references for execute
			let params: Vec<&dyn tokio_rusqlite::types::ToSql> =
				all_params.iter().map(|p| p.as_ref()).collect();
			// Execute the statement
			let count = conn.execute(&stmt, params.as_slice())?;
			// Check the number of rows deleted
			assert_eq!(count, all_params.len());
			Ok(())
		})
		.await
		.map_err(Into::into)
	}
}
