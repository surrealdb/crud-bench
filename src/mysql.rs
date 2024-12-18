#![cfg(feature = "mysql")]

use crate::dialect::{AnsiSqlDialect, Dialect};
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::{ColumnType, Columns};
use crate::{KeyType, Projection, Scan};
use anyhow::Result;
use mysql_async::consts;
use mysql_async::prelude::Queryable;
use mysql_async::prelude::ToValue;
use mysql_async::{Conn, Opts, Row};
use serde_json::{Map, Value};
use std::hint::black_box;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) const MYSQL_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "mysql",
	pre_args: "-p 127.0.0.1:3306:3306 -e MYSQL_ROOT_PASSWORD=mysql",
	post_args: "-u mysql -h localhost",
};

pub(crate) struct MysqlClientProvider(KeyType, Columns, String);

impl BenchmarkEngine<MysqlClient> for MysqlClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, columns: Columns, endpoint: Option<&str>) -> Result<Self> {
		let url = endpoint.unwrap_or("mysql://mysql:mysql@localhost:3306/bench").to_owned();
		Ok(Self(kt, columns, url))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<MysqlClient> {
		let conn = Conn::new(Opts::from_url(&self.2)?).await?;
		Ok(MysqlClient {
			conn: Arc::new(Mutex::new(conn)),
			kt: self.0,
			columns: self.1.clone(),
		})
	}
}

pub(crate) struct MysqlClient {
	conn: Arc<Mutex<Conn>>,
	kt: KeyType,
	columns: Columns,
}

impl BenchmarkClient for MysqlClient {
	async fn startup(&self) -> Result<()> {
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
			.join(", ");
		let stm = format!("CREATE TABLE record ( id {id_type} PRIMARY KEY, {fields});");
		self.conn.lock().await.query_drop(&stm).await?;
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: Value) -> Result<()> {
		self.create(key as i32, val).await
	}

	async fn create_string(&self, key: String, val: Value) -> Result<()> {
		self.create(key, val).await
	}

	async fn read_u32(&self, key: u32) -> Result<()> {
		self.read(key as i32).await
	}

	async fn read_string(&self, key: String) -> Result<()> {
		self.read(key).await
	}

	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		self.update(key as i32, val).await
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		self.update(key, val).await
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key as i32).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key).await
	}

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}
}

impl MysqlClient {
	fn consume(&self, mut row: Row) -> Result<Value> {
		let mut val: Map<String, Value> = Map::new();
		//
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
					_ => todo!(),
				},
			);
		}
		Ok(val.into())
	}

	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: ToValue + Sync,
	{
		let (fields, values) = self.columns.insert_clauses::<AnsiSqlDialect>(val)?;
		let stm = format!("INSERT INTO record (id, {fields}) VALUES (?, {values})");
		let res: Vec<Row> = self.conn.lock().await.exec(&stm, (key.to_value(),)).await?;
		assert_eq!(res.len(), 1);
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<()>
	where
		T: ToValue + Sync,
	{
		let stm = "SELECT * FROM record WHERE id=$1";
		let res: Vec<Row> = self.conn.lock().await.exec(&stm, (key.to_value(),)).await?;
		assert_eq!(res.len(), 1);
		Ok(())
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: ToValue + Sync,
	{
		let set = self.columns.set_clause::<AnsiSqlDialect>(val)?;
		let stm = format!("UPDATE record SET {set} WHERE id=$1");
		let res: Vec<Row> = self.conn.lock().await.exec(&stm, (key.to_value(),)).await?;
		assert_eq!(res.len(), 1);
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: ToValue + Sync,
	{
		let stm = "DELETE FROM record WHERE id=$1";
		let res: Vec<Row> = self.conn.lock().await.exec(&stm, (key.to_value(),)).await?;
		assert_eq!(res.len(), 1);
		Ok(())
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.map(|s| format!("OFFSET {}", s)).unwrap_or("".to_string());
		let l = scan.limit.map(|s| format!("LIMIT {}", s)).unwrap_or("".to_string());
		let c = scan.condition.as_ref().map(|s| format!("WHERE {}", s)).unwrap_or("".to_string());
		// Perform the relevant projection scan type
		match scan.projection()? {
			Projection::Id => {
				let stm = format!("SELECT id FROM record {c} {l} {s}");
				let res: Vec<Row> = self.conn.lock().await.query(&stm).await?;
				let res = res
					.into_iter()
					.map(|v| -> Result<_> { Ok(black_box(self.consume(v)?)) })
					.collect::<Result<Vec<_>>>()?;
				Ok(res.len())
			}
			Projection::Full => {
				let stm = format!("SELECT * FROM record {c} {l} {s}");
				let res: Vec<Row> = self.conn.lock().await.query(&stm).await?;
				let res = res
					.into_iter()
					.map(|v| -> Result<_> { Ok(black_box(self.consume(v)?)) })
					.collect::<Result<Vec<_>>>()?;
				Ok(res.len())
			}
			Projection::Count => {
				let stm = format!("SELECT COUNT(*) FROM (SELECT id FROM record {c} {l} {s})");
				let res: Vec<Row> = self.conn.lock().await.query(&stm).await?;
				let count: i64 = res.first().unwrap().get(0).unwrap();
				Ok(count as usize)
			}
		}
	}
}
