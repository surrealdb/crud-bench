#![cfg(feature = "scylladb")]

use crate::dialect::AnsiSqlDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::{ColumnType, Columns};
use crate::{KeyType, Projection, Scan};
use anyhow::{bail, Result};
use futures::StreamExt;
use scylla::_macro_internal::{SerializeRow, SerializeValue};
use scylla::transport::iterator::TypedRowStream;
use scylla::{Session, SessionBuilder};
use serde_json::Value;
use std::fmt::Display;

pub(crate) const SCYLLADB_DOCKER_PARAMS: DockerParams = DockerParams {
	image: "scylladb/scylla",
	pre_args: "-p 9042:9042",
	post_args: "",
};

pub(crate) struct ScyllaDBClientProvider(KeyType, Columns);

impl BenchmarkEngine<ScylladbClient> for ScyllaDBClientProvider {
	async fn setup(kt: KeyType, columns: Columns) -> Result<Self> {
		Ok(ScyllaDBClientProvider(kt, columns))
	}

	async fn create_client(&self, endpoint: Option<String>) -> Result<ScylladbClient> {
		let node = endpoint.unwrap_or("127.0.0.1:9042".to_owned());
		let session = SessionBuilder::new().known_node(node).build().await?;
		Ok(ScylladbClient {
			session,
			kt: self.0,
			columns: self.1.clone(),
		})
	}
}

pub(crate) struct ScylladbClient {
	session: Session,
	kt: KeyType,
	columns: Columns,
}

impl BenchmarkClient for ScylladbClient {
	async fn startup(&self) -> Result<()> {
		self.session
			.query_unpaged(
				"
					CREATE KEYSPACE bench
					WITH replication = { 'class': 'SimpleStrategy', 'replication_factor' : 1 }
					AND durable_writes = true
				",
				(),
			)
			.await?;
		let id_type = match self.kt {
			KeyType::Integer => "int",
			KeyType::String26 | KeyType::String90 | KeyType::String506 => "TEXT",
			KeyType::Uuid => {
				todo!()
			}
		};
		let fields: Vec<String> = self
			.columns
			.0
			.iter()
			.map(|(n, t)| match t {
				ColumnType::String => format!("{n} TEXT"),
				ColumnType::Integer => format!("{n} INT"),
				ColumnType::Object => format!("{n} TEXT"),
				ColumnType::Float => format!("{n} FLOAT"),
				ColumnType::DateTime => format!("{n} TIMESTAMP"),
				ColumnType::Uuid => format!("{n} UUID"),
				ColumnType::Bool => format!("{n} BOOLEAN"),
			})
			.collect();
		let fields = fields.join(",");
		self.session
			.query_unpaged(
				format!("CREATE TABLE bench.record ( id {id_type} PRIMARY KEY, {fields})"),
				(),
			)
			.await?;
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

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, val: Value) -> Result<()> {
		self.update(key as i32, val).await
	}

	async fn update_string(&self, key: String, val: Value) -> Result<()> {
		self.update(key, val).await
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key as i32).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key).await
	}
}

impl ScylladbClient {
	async fn create<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: Display,
	{
		let (fields, values) = self.columns.insert_clauses::<AnsiSqlDialect>(val)?;
		self.session
			.query_unpaged(
				format!("INSERT INTO bench.record (id, {fields}) VALUES ({key}, {values})"),
				(),
			)
			.await?;
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<()>
	where
		T: SerializeValue,
	{
		let res =
			self.session.query_unpaged("SELECT * FROM bench.record WHERE id=?", (&key,)).await?;
		assert_eq!(res.into_rows_result()?.rows_num(), 1);
		Ok(())
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		let s = scan.start.unwrap_or(0);
		let l = (scan.start.unwrap_or(0) + scan.limit.unwrap_or(0)) as i32;
		let c = scan.condition.as_ref().map(|s| format!("WHERE {}", s)).unwrap_or("".to_string());
		let p = scan.projection()?;
		let stm = match p {
			Projection::Id => {
				format!("SELECT id FROM record {c} LIMIT ?")
			}
			Projection::Full => {
				format!("SELECT * FROM record {c} LIMIT ?")
			}
			Projection::Count => {
				format!("SELECT count(*) FROM record {c} LIMIT ?")
			}
		};
		let mut rows_stream: TypedRowStream<(String,)> =
			self.session.query_iter(stm, (&l,)).await?.rows_stream()?;
		if s > 0 {
			let _ = (&mut rows_stream).skip(s);
		}
		match p {
			Projection::Id | Projection::Full => {
				let mut count = 0;
				while let Some(next_row_res) = rows_stream.next().await {
					let id: (String,) = next_row_res?;
					assert!(!id.is_empty());
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				if let Some(next_row_res) = rows_stream.next().await {
					let count: (String,) = next_row_res?;
					let count: usize = count.0.parse()?;
					Ok(count)
				} else {
					bail!("No row returned");
				}
			}
		}
	}
	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: Display,
	{
		let set = self.columns.set_clause::<AnsiSqlDialect>(val)?;
		self.session
			.query_unpaged(format!("UPDATE bench.record SET {set} WHERE id={key}"), ())
			.await?;
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: SerializeValue,
	{
		self.session.query_unpaged("DELETE FROM bench.record WHERE id=?", (&key,)).await?;
		Ok(())
	}
}
