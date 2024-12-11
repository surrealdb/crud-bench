#![cfg(feature = "postgres")]

use tokio_postgres::{Client, NoTls};

use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::dialect::{AnsiSqlDialect, Dialect};
use crate::docker::DockerParams;
use crate::valueprovider::{ColumnType, Columns};
use crate::{KeyType, Scan};
use anyhow::Result;
use serde_json::Value;
use tokio_postgres::types::ToSql;

pub(crate) const POSTGRES_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "postgres",
	pre_args: "-p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres",
	post_args: "postgres -N 1024",
};

pub(crate) struct PostgresClientProvider(KeyType, Columns);

impl BenchmarkEngine<PostgresClient> for PostgresClientProvider {
	async fn setup(kt: KeyType, columns: Columns) -> Result<Self> {
		Ok(Self(kt, columns))
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<PostgresClient> {
		let url = endpoint.unwrap_or("host=localhost user=postgres password=postgres".to_owned());
		let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
		tokio::spawn(async move {
			if let Err(e) = connection.await {
				eprintln!("connection error: {}", e);
			}
		});
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
			KeyType::String506 => "VARCHAR(506)",
			KeyType::Uuid => {
				todo!()
			}
		};
		let fields: Vec<String> = self
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
			.collect();
		let fields = fields.join(",");
		let stm = format!("CREATE TABLE record ( id {id_type} PRIMARY KEY,{fields});");
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

	async fn scan_u32(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan) -> Result<usize> {
		self.scan(scan).await
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
}

impl PostgresClient {
	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let (fields, values) = self.columns.insert_clauses::<AnsiSqlDialect>(val)?;
		let stm = format!("INSERT INTO record (id,{fields}) VALUES ($1,{values})");
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
		Ok(())
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		let s = scan.start.map(|s| format!("OFFSET {}", s)).unwrap_or("".to_string());
		let l = scan.limit.map(|s| format!("LIMIT {}", s)).unwrap_or("".to_string());
		let c = scan.condition.as_ref().map(|s| format!("WHERE {}", s)).unwrap_or("".to_string());
		let k = scan.keys_only.unwrap_or(false);
		let stm = if k {
			format!("SELECT id FROM record {c} {l} {s}")
		} else {
			format!("SELECT * FROM record {c} {l} {s}")
		};
		let res = self.client.query(&stm, &[]).await?;
		Ok(res.len())
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: ToSql + Sync,
	{
		let set = self.columns.set_clause::<AnsiSqlDialect>(val)?;
		let stm = format!("UPDATE record SET {set} WHERE id=$1");
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
}
