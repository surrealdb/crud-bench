#![cfg(feature = "scylladb")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::dialect::AnsiSqlDialect;
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::value::BenchValue;
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, anyhow, bail};
use futures::StreamExt;
use scylla::_macro_internal::SerializeValue;
use scylla::client::{PoolSize, session::Session, session_builder::SessionBuilder};
use scylla::value::{CqlTimestamp, CqlValue};
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
	// The return type when reading a row
	type ReadRow = BenchValue;

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
				ColumnType::Array => format!("{n} TEXT"),
				ColumnType::Float => format!("{n} FLOAT"),
				ColumnType::DateTime => format!("{n} TIMESTAMP"),
				ColumnType::Uuid => format!("{n} UUID"),
				ColumnType::Decimal => format!("{n} DECIMAL"),
				ColumnType::Bool => format!("{n} BOOLEAN"),
				ColumnType::Bytes => format!("{n} BLOB"),
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

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.create_row(CqlValue::Int(key as i32), val).await
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.create_row(CqlValue::Text(key), val).await
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		self.read(key as i32).await
	}

	async fn read_string(&self, key: String) -> Result<BenchValue> {
		self.read(key).await
	}

	async fn scan_u32(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		self.scan(scan).await
	}

	async fn scan_string(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		self.scan(scan).await
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		self.update_row(CqlValue::Int(key as i32), val).await
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		self.update_row(CqlValue::Text(key), val).await
	}

	#[allow(dependency_on_unit_never_type_fallback)]
	async fn delete_u32(&self, key: u32) -> Result<()> {
		self.delete(key as i32).await
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		self.delete(key).await
	}
}

fn bench_to_cql_value(column_type: &ColumnType, v: &BenchValue) -> Result<CqlValue> {
	match (column_type, v) {
		(ColumnType::Integer, BenchValue::Int(i)) => Ok(CqlValue::Int(
			i32::try_from(*i).map_err(|_| anyhow!("integer out of range for CQL INT"))?,
		)),
		(ColumnType::Integer, BenchValue::UInt(u)) => Ok(CqlValue::Int(
			i32::try_from(*u).map_err(|_| anyhow!("integer out of range for CQL INT"))?,
		)),
		(ColumnType::Float, BenchValue::Float(f)) => Ok(CqlValue::Float(*f as f32)),
		(ColumnType::Float, BenchValue::Int(i)) => Ok(CqlValue::Float(*i as f32)),
		(ColumnType::Bool, BenchValue::Bool(b)) => Ok(CqlValue::Boolean(*b)),
		(ColumnType::String, BenchValue::String(s)) => Ok(CqlValue::Text(s.clone())),
		(ColumnType::Object | ColumnType::Array, _) => {
			Ok(CqlValue::Text(serde_json::to_string(&v.to_json())?))
		}
		(ColumnType::DateTime, BenchValue::DateTime(dt)) => {
			Ok(CqlValue::Timestamp(CqlTimestamp(dt.timestamp_millis())))
		}
		(ColumnType::Uuid, BenchValue::Uuid(u)) => Ok(CqlValue::Uuid(*u)),
		(ColumnType::Decimal, BenchValue::Decimal(d)) => {
			// CQL DECIMAL parses textual numeric representations directly.
			Ok(CqlValue::Text(d.to_string()))
		}
		(ColumnType::Bytes, BenchValue::Bytes(b)) => Ok(CqlValue::Blob(b.clone())),
		(t, _) => bail!("BenchValue does not match column type {t:?}"),
	}
}

impl ScylladbClient {
	async fn create_row(&self, key: CqlValue, val: BenchValue) -> Result<()> {
		let obj = val.into_object()?;
		let field_names: Vec<String> = std::iter::once("id".to_string())
			.chain(self.columns.0.iter().map(|(n, _)| n.clone()))
			.collect();
		let placeholders = (0..field_names.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
		let stm = format!(
			"INSERT INTO bench.record ({}) VALUES ({})",
			field_names.join(", "),
			placeholders
		);
		let mut row: Vec<CqlValue> = vec![key];
		for (c, ct) in &self.columns.0 {
			let v = obj
				.iter()
				.find(|(k, _)| k == c)
				.map(|(_, v)| v)
				.ok_or_else(|| anyhow!("Missing value for column {c}"))?;
			row.push(bench_to_cql_value(ct, v)?);
		}
		self.session.query_unpaged(stm, row).await?;
		Ok(())
	}

	async fn update_row(&self, key: CqlValue, val: BenchValue) -> Result<()> {
		let obj = val.into_object()?;
		let sets: Vec<String> = self.columns.0.iter().map(|(n, _)| format!("{n} = ?")).collect();
		let stm = format!("UPDATE bench.record SET {} WHERE id = ?", sets.join(", "));
		let mut row: Vec<CqlValue> = Vec::new();
		for (c, ct) in &self.columns.0 {
			let v = obj
				.iter()
				.find(|(k, _)| k == c)
				.map(|(_, v)| v)
				.ok_or_else(|| anyhow!("Missing value for column {c}"))?;
			row.push(bench_to_cql_value(ct, v)?);
		}
		row.push(key);
		self.session.query_unpaged(stm, row).await?;
		Ok(())
	}

	async fn read<T>(&self, key: T) -> Result<BenchValue>
	where
		T: SerializeValue,
	{
		let stm = "SELECT JSON * FROM bench.record WHERE id=?";
		let res = self.session.query_unpaged(stm, (&key,)).await?;
		let rows = res.into_rows_result()?;
		assert_eq!(rows.rows_num(), 1);
		let (val,): (String,) = rows.single_row().map_err(|e| anyhow!("{e}"))?;
		let json: serde_json::Value = serde_json::from_str(&val)?;
		Ok(black_box(BenchValue::from(&json)))
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
		// Ordered scans are not supported
		if scan.order_by.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
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
