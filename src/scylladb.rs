#![cfg(feature = "scylladb")]

use crate::benchmark::{BenchmarkClient, BenchmarkEngine};
use crate::dialect::AnsiSqlDialect;
use crate::docker::DockerParams;
use crate::valueprovider::{ColumnType, Columns};
use crate::KeyType;
use anyhow::Result;
use scylla::_macro_internal::SerializeValue;
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
		assert_eq!(res.rows_num()?, 1);
		Ok(())
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
