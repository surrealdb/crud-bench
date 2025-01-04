#![cfg(feature = "sqlite")]

use crate::dialect::{AnsiSqlDialect, Dialect};
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::{ColumnType, Columns};
use crate::{KeyType, Projection, Scan};
use anyhow::Result;
use serde_json::Value as Json;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tokio_rusqlite::types::ToSqlOutput;
use tokio_rusqlite::types::Value;
use tokio_rusqlite::Connection;

const DATABASE_DIR: &str = "sqlite";

pub(crate) struct SqliteClientProvider {
	conn: Arc<Connection>,
	kt: KeyType,
	columns: Columns,
}

impl BenchmarkEngine<SqliteClient> for SqliteClientProvider {
	/// The number of seconds to wait before connecting
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, columns: Columns, _endpoint: Option<&str>) -> Result<Self> {
		// Remove the database directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Recreate the database directory
		std::fs::create_dir(DATABASE_DIR)?;
		// Switch to the new directory
		std::env::set_current_dir(DATABASE_DIR)?;
		// Create the connection
		let conn = Connection::open("db").await?;
		// Create the store
		Ok(Self {
			conn: Arc::new(conn),
			kt,
			columns,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<SqliteClient> {
		Ok(SqliteClient {
			conn: self.conn.clone(),
			kt: self.kt,
			columns: self.columns.clone(),
		})
	}
}

pub(crate) struct SqliteClient {
	conn: Arc<Connection>,
	kt: KeyType,
	columns: Columns,
}

impl BenchmarkClient for SqliteClient {
	async fn shutdown(&self) -> Result<()> {
		// Remove the database directory
		std::fs::remove_dir_all(DATABASE_DIR).ok();
		// Ok
		Ok(())
	}

	async fn startup(&self) -> Result<()> {
		// Optimise SQLite
		let stmt = "
            PRAGMA synchronous = NORMAL;
            PRAGMA journal_mode = WAL;
            PRAGMA page_size = 4096;
            PRAGMA cache_size = 2500;
            PRAGMA locking_mode = EXCLUSIVE;
		";
		self.execute_batch(Cow::Borrowed(stmt)).await?;
		let id_type = match self.kt {
			KeyType::Integer => "SERIAL",
			KeyType::String26 => "VARCHAR(26)",
			KeyType::String90 => "VARCHAR(90)",
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
	) -> Result<Vec<Value>> {
		self.conn
			.call(move |conn| {
				let mut stmt = conn.prepare(stmt.as_ref())?;
				let mut rows = match params {
					Some(params) => stmt.query([&params])?,
					None => stmt.query(())?,
				};
				let mut vec = Vec::new();
				while let Some(row) = rows.next()? {
					vec.push(row.get(0)?);
				}
				Ok(vec)
			})
			.await
			.map_err(Into::into)
	}

	async fn create(&self, key: ToSqlOutput<'static>, val: Json) -> Result<()> {
		let (fields, values) = self.columns.insert_clauses::<AnsiSqlDialect>(val)?;
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
		let set = self.columns.set_clause::<AnsiSqlDialect>(val)?;
		let stmt = format!("UPDATE record SET {set} WHERE id=$1");
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

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.map(|s| format!("OFFSET {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = scan.condition.as_ref().map(|s| format!("WHERE {s}")).unwrap_or_default();
		// Perform the relevant projection scan type
		match scan.projection()? {
			Projection::Id => {
				let stmt = format!("SELECT id FROM record {c} {l} {s}");
				let res = self.query(Cow::Owned(stmt), None).await?;
				Ok(res.len())
			}
			Projection::Full => {
				let stmt = format!("SELECT * FROM record {c} {l} {s}");
				let res = self.query(Cow::Owned(stmt), None).await?;
				Ok(res.len())
			}
			Projection::Count => {
				let stmt = format!("SELECT COUNT(*) FROM (SELECT id FROM record {c} {l} {s})");
				let res = self.query(Cow::Owned(stmt), None).await?;
				let Value::Integer(count) = res.first().unwrap() else {
					panic!("Unexpected response type `{res:?}`");
				};
				Ok(*count as usize)
			}
		}
	}
}
