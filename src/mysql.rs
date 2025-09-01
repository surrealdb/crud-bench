#![cfg(feature = "mysql")]

use crate::dialect::{Dialect, MySqlDialect};
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::Result;
use mysql_async::consts;
use mysql_async::prelude::Queryable;
use mysql_async::prelude::ToValue;
use mysql_async::{Conn, Opts, Row};
use serde_json::{Map, Value};
use std::hint::black_box;
use std::sync::Arc;
use tokio::sync::Mutex;

pub const DEFAULT: &str = "mysql://root:mysql@127.0.0.1:3306/bench";

pub(crate) const fn docker(options: &Benchmark) -> DockerParams {
	DockerParams {
		image: "mysql",
		pre_args: "--ulimit nofile=65536:65536 -p 127.0.0.1:3306:3306 -e MYSQL_ROOT_HOST=% -e MYSQL_ROOT_PASSWORD=mysql -e MYSQL_DATABASE=bench",
		post_args: match options.sync {
			true => "--max-connections=1024 --innodb-buffer-pool-size=16G --innodb-buffer-pool-instances=32 --sync_binlog=1 --innodb-flush-log-at-trx-commit=1",
			false => "--max-connections=1024 --innodb-buffer-pool-size=16G --innodb-buffer-pool-instances=32 --sync_binlog=0 --innodb-flush-log-at-trx-commit=0",
		}
	}
}

pub(crate) struct MysqlClientProvider(KeyType, Columns, String);

impl BenchmarkEngine<MysqlClient> for MysqlClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, columns: Columns, options: &Benchmark) -> Result<Self> {
		// Get the custom endpoint if specified
		let url = options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned();
		// Create the client provider
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
		let stm =
			format!("DROP TABLE IF EXISTS record; CREATE TABLE record ( id {id_type} PRIMARY KEY, {fields}) ENGINE=InnoDB;");
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

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.map(|s| format!("OFFSET {s}")).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {s}")).unwrap_or_default();
		let c = scan.condition.as_ref().map(|s| format!("WHERE {s}")).unwrap_or_default();
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
}
