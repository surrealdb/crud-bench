#![cfg(feature = "scylladb")]

use crate::dialect::AnsiSqlDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::Result;
use futures::StreamExt;
use scylla::_macro_internal::SerializeValue;
use scylla::transport::session::PoolSize;
use scylla::{Session, SessionBuilder};
use serde_json::Value;
use std::hint::black_box;
use std::num::NonZeroUsize;

pub const DEFAULT: &str = "127.0.0.1:9042";

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	DockerParams {
		image: "scylladb/scylla",
		pre_args: match options.sync {
			true => {
				"-p 9042:9042 -e SCYLLA_ARGS='--commitlog-sync=batch --commitlog-sync-batch-window-in-ms=1'".to_string()
			}
			false => {
				"-p 9042:9042 -e SCYLLA_ARGS='--commitlog-sync=periodic --commitlog-sync-period-in-ms=1000'".to_string()
			}
		},
		post_args: "".to_string(),
	}
}

pub(crate) struct ScyllaDBClientProvider(KeyType, Columns, String);

impl BenchmarkEngine<ScylladbClient> for ScyllaDBClientProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(kt: KeyType, columns: Columns, options: &Benchmark) -> Result<Self> {
		let url = options.endpoint.as_deref().unwrap_or(DEFAULT).to_owned();
		Ok(ScyllaDBClientProvider(kt, columns, url))
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<ScylladbClient> {
		let session = SessionBuilder::new()
			.pool_size(PoolSize::PerHost(NonZeroUsize::new(1).unwrap()))
			.known_node(&self.2)
			.tcp_nodelay(true)
			.build()
			.await?;
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
			KeyType::Integer => "INT",
			KeyType::String26 | KeyType::String90 | KeyType::String250 | KeyType::String506 => {
				"TEXT"
			}
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
		T: SerializeValue,
	{
		let (fields, values) = AnsiSqlDialect::create_clause(&self.columns, val);
		let stm = format!("INSERT INTO bench.record (id, {fields}) VALUES (?, {values})");
		self.session.query_unpaged(stm, (&key,)).await?;
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<()>
	where
		T: SerializeValue,
	{
		let stm = "SELECT * FROM bench.record WHERE id=?";
		let res = self.session.query_unpaged(stm, (&key,)).await?;
		assert_eq!(res.into_rows_result()?.rows_num(), 1);
		Ok(())
	}

	async fn update<T>(&self, key: T, val: Value) -> Result<()>
	where
		T: SerializeValue,
	{
		let fields = AnsiSqlDialect::update_clause(&self.columns, val);
		let stm = format!("UPDATE bench.record SET {fields} WHERE id=?");
		self.session.query_unpaged(stm, (&key,)).await?;
		Ok(())
	}

	async fn delete<T>(&self, key: T) -> Result<()>
	where
		T: SerializeValue,
	{
		let stm = "DELETE FROM bench.record WHERE id=?";
		self.session.query_unpaged(stm, (&key,)).await?;
		Ok(())
	}

	async fn scan(&self, scan: &Scan) -> Result<usize> {
		// Extract parameters
		let s = scan.start.unwrap_or_default();
		let l = scan.limit.map(|l| format!("LIMIT {}", l + s)).unwrap_or_default();
		let c = AnsiSqlDialect::filter_clause(scan)?;
		let p = scan.projection()?;
		// Perform the relevant projection scan type
		match p {
			Projection::Id => {
				let stm = format!("SELECT id FROM bench.record {c} {l}");
				let mut res = self.session.query_iter(stm, ()).await?.rows_stream()?.skip(s);
				let mut count = 0;
				while let Some(v) = res.next().await {
					let v: (String,) = v?;
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Full => {
				let stm = format!("SELECT id FROM bench.record {c} {l}");
				let mut res = self.session.query_iter(stm, ()).await?.rows_stream()?.skip(s);
				let mut count = 0;
				while let Some(v) = res.next().await {
					let v: (String,) = v?;
					black_box(v);
					count += 1;
				}
				Ok(count)
			}
			Projection::Count => {
				let stm = format!("SELECT count(*) FROM bench.record {c} {l}");
				let mut res = self.session.query_iter(stm, ()).await?.rows_stream()?.skip(s);
				let count: (String,) = res.next().await.unwrap()?;
				let count: usize = count.0.parse()?;
				Ok(count)
			}
		}
	}
}
