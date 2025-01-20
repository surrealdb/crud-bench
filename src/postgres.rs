#![cfg(feature = "postgres")]

use crate::dialect::{AnsiSqlDialect, Dialect};
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::Result;
use serde_json::{Map, Value};
use std::hint::black_box;
use tokio_postgres::types::{Json, ToSql};
use tokio_postgres::{Client, NoTls, Row};

pub const DEFAULT: &str = "host=localhost user=postgres password=postgres";

pub(crate) const POSTGRES_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "postgres",
	pre_args: "--ulimit nofile=65536:65536 -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres",
	post_args: "postgres -N 1024 -c fsync=off",
};

pub(crate) struct PostgresClientProvider(KeyType, Columns, String);

impl BenchmarkEngine<PostgresClient> for PostgresClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, columns: Columns, options: &Benchmark) -> Result<Self> {
		// Get the custom endpoint if specified
		let url = options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned();
		// Create the client provider
		Ok(Self(kt, columns, url))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<PostgresClient> {
		// Connect to the database with TLS disabled
		let (client, connection) = tokio_postgres::connect(&self.2, NoTls).await?;
		// Log any errors when the connection is closed
		tokio::spawn(async move {
			if let Err(e) = connection.await {
				eprintln!("connection error: {}", e);
			}
		});
		// Create the client
		Ok(PostgresClient {
			client,
			kt: self.0,
			columns: self.1.clone(),
		})
	}
}

pub(crate) struct PostgresClient {
	client: Client,
	kt: KeyType,
	columns: Columns,
}

impl BenchmarkClient for PostgresClient {
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
				let n = AnsiSqlDialect::escape_field(n.clone());
				match t {
					ColumnType::String => format!("{n} TEXT NOT NULL"),
					ColumnType::Integer => format!("{n} INTEGER NOT NULL"),
					ColumnType::Object => format!("{n} JSONB NOT NULL"),
					ColumnType::Float => format!("{n} REAL NOT NULL"),
					ColumnType::DateTime => format!("{n} TIMESTAMP NOT NULL"),
					ColumnType::Uuid => format!("{n} UUID NOT NULL"),
					ColumnType::Bool => format!("{n} BOOL NOT NULL"),
				}
			})
			.collect::<Vec<String>>()
			.join(", ");
		let stm = format!("DROP TABLE IF EXISTS record; CREATE TABLE record ( id {id_type} PRIMARY KEY, {fields});");
		self.client.batch_execute(&stm).await?;
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

impl PostgresClient {
	fn consume(&self, row: Row, columns: bool) -> Result<Value> {
		let mut val: Map<String, Value> = Map::new();
		match self.kt {
			KeyType::Integer => {
				let v: i32 = row.try_get("id")?;
				val.insert("id".into(), Value::from(v));
			}
			KeyType::String26 | KeyType::String90 | KeyType::String250 | KeyType::String506 => {
				let v: String = row.try_get("id")?;
				val.insert("id".into(), Value::from(v));
			}
			KeyType::Uuid => {
				let v: uuid::Uuid = row.try_get("id")?;
				val.insert("id".into(), Value::from(v.to_string()));
			}
		}
		if columns {
			for (n, t) in self.columns.0.iter() {
				val.insert(
					n.clone(),
					match t {
						ColumnType::Bool => {
							let v: bool = row.try_get(n.as_str())?;
							Value::from(v)
						}
						ColumnType::Float => {
							let v: f64 = row.try_get(n.as_str())?;
							Value::from(v)
						}
						ColumnType::Integer => {
							let v: i32 = row.try_get(n.as_str())?;
							Value::from(v)
						}
						ColumnType::String => {
							let v: String = row.try_get(n.as_str())?;
							Value::from(v)
						}
						ColumnType::DateTime => {
							let v: String = row.try_get(n.as_str())?;
							Value::from(v)
						}
						ColumnType::Uuid => {
							let v: uuid::Uuid = row.try_get(n.as_str())?;
							Value::from(v.to_string())
						}
						ColumnType::Object => {
							let v: Json<Value> = row.try_get(n.as_str())?;
							v.0
						}
					},
				);
			}
		}
		Ok(val.into())
	}

	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let (fields, values) = AnsiSqlDialect::create_clause(&self.columns, val);
		let stm = format!("INSERT INTO record (id, {fields}) VALUES ($1, {values})");
		let res = self.client.execute(&stm, &[&key]).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let stm = "SELECT * FROM record WHERE id=$1";
		let res = self.client.query(stm, &[&key]).await?;
		assert_eq!(res.len(), 1);
		black_box(self.consume(res.into_iter().next().unwrap(), true)?);
		Ok(())
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let fields = AnsiSqlDialect::update_clause(&self.columns, val);
		let stm = format!("UPDATE record SET {fields} WHERE id=$1");
		let res = self.client.execute(&stm, &[&key]).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let stm = "DELETE FROM record WHERE id=$1";
		let res = self.client.execute(stm, &[&key]).await?;
		assert_eq!(res, 1);
		Ok(())
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.map(|s| format!("OFFSET {}", s)).unwrap_or_default();
		let l = scan.limit.map(|s| format!("LIMIT {}", s)).unwrap_or_default();
		let c = scan.condition.as_ref().map(|s| format!("WHERE {}", s)).unwrap_or_default();
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("SELECT id FROM record {c} {l} {s}");
				let res = self.client.query(&stm, &[]).await?;
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(self.consume(v, false).unwrap());
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let stm = format!("SELECT * FROM record {c} {l} {s}");
				let res = self.client.query(&stm, &[]).await?;
				// We use a for loop to iterate over the results, while
				// calling black_box internally. This is necessary as
				// an iterator with `filter_map` or `map` is optimised
				// out by the compiler when calling `count` at the end.
				let mut count = 0;
				for v in res {
					black_box(self.consume(v, true).unwrap());
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let stm = format!("SELECT COUNT(*) FROM (SELECT id FROM record {c} {l} {s})");
				let res = self.client.query(&stm, &[]).await?;
				let count: i64 = res.first().unwrap().get(0);
				Ok(count as usize)
			}
		}
	}
}
