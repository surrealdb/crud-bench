#![cfg(feature = "sqlite")]

use crate::dialect::{AnsiSqlDialect, Dialect};
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::memory::Config;
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, KeyType, Projection, Scan};
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

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
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
}
