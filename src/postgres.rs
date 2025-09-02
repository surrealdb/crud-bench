#![cfg(feature = "postgres")]

use crate::dialect::{AnsiSqlDialect, Dialect};
use crate::docker::DockerParams;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::memory::Config;
use crate::valueprovider::{ColumnType, Columns};
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::Result;
use serde_json::{Map, Value};
use std::hint::black_box;
use tokio_postgres::types::{Json, ToSql};
use tokio_postgres::{Client, NoTls, Row};

pub const DEFAULT: &str = "host=127.0.0.1 user=postgres password=postgres";

/// Calculate Postgres specific memory allocation
fn calculate_postgres_memory() -> (u64, u64, u64, u64, u64, u64) {
	// Load the system memory
	let memory = Config::new();
	// Use ~33% of recommended cache allocation
	let shared_buffers_gb = (memory.cache_gb / 3).max(1);
	// Use ~75% of total system memory for caching
	let effective_cache_gb = memory.cache_gb;
	// Scale work_mem with shared_buffers
	let work_mem_mb = (shared_buffers_gb * 64).max(32);
	// Use 25% of shared_buffers, max 8GB
	let maintenance_work_mem_gb = (shared_buffers_gb / 4).clamp(1, 8);
	// Scale WAL with shared_buffers
	let max_wal_gb = (shared_buffers_gb).clamp(2, 16);
	let min_wal_gb = (max_wal_gb / 4).max(1);
	// Return configuration
	(
		shared_buffers_gb,
		effective_cache_gb,
		work_mem_mb,
		maintenance_work_mem_gb,
		max_wal_gb,
		min_wal_gb,
	)
}

pub(crate) fn docker(options: &Benchmark) -> DockerParams {
	// Calculate memory allocation
	let (
		shared_buffers_gb,
		effective_cache_gb,
		work_mem_mb,
		maintenance_work_mem_gb,
		max_wal_gb,
		min_wal_gb,
	) = calculate_postgres_memory();
	// Return Docker parameters
	DockerParams {
		image: "postgres",
		pre_args:
			"--ulimit nofile=65536:65536 -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres"
				.to_string(),
		post_args: match options.optimised {
			// Optimised configuration
			true => format!(
				"postgres -N 1024 \
				-c shared_buffers={shared_buffers_gb}GB \
				-c effective_cache_size={effective_cache_gb}GB \
				-c work_mem={work_mem_mb}MB \
				-c maintenance_work_mem={maintenance_work_mem_gb}GB \
				-c wal_buffers=16MB \
				-c checkpoint_timeout=15min \
				-c checkpoint_completion_target=0.9 \
				-c random_page_cost=1.1 \
				-c effective_io_concurrency=200 \
				-c min_wal_size={min_wal_gb}GB \
				-c max_wal_size={max_wal_gb}GB \
				-c fsync={} \
				-c synchronous_commit=on",
				if options.sync {
					"on"
				} else {
					"off"
				}
			),
			// Default configuration
			false => format!(
				"postgres -N 1024 \
				-c fsync={} \
				-c synchronous_commit=on",
				if options.sync {
					"on"
				} else {
					"off"
				}
			),
		},
	}
}

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
				eprintln!("connection error: {e}");
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
		let stm = format!(
			"DROP TABLE IF EXISTS record; CREATE TABLE record ( id {id_type} PRIMARY KEY, {fields});"
		);
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

	async fn batch_create_u32(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut pairs = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			pairs.push((key as i32, val));
		}
		self.batch_create(pairs).await
	}

	async fn batch_create_string(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut key_vals_vec = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			key_vals_vec.push((key, val));
		}
		self.batch_create(key_vals_vec).await
	}

	async fn batch_read_u32(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = u32> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key as i32);
		}
		self.batch_read(keys_vec).await
	}

	async fn batch_read_string(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = String> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key);
		}
		self.batch_read(keys_vec).await
	}

	async fn batch_update_u32(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (u32, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut pairs = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			pairs.push((key as i32, val));
		}
		self.batch_update(pairs).await
	}

	async fn batch_update_string(
		&self,
		batch_size: usize,
		key_vals: impl Iterator<Item = (String, serde_json::Value)> + Send,
	) -> Result<()> {
		let mut key_vals_vec = Vec::with_capacity(batch_size);
		for (key, val) in key_vals {
			key_vals_vec.push((key, val));
		}
		self.batch_update(key_vals_vec).await
	}

	async fn batch_delete_u32(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = u32> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key as i32);
		}
		self.batch_delete(keys_vec).await
	}

	async fn batch_delete_string(
		&self,
		batch_size: usize,
		keys: impl Iterator<Item = String> + Send,
	) -> Result<()> {
		let mut keys_vec = Vec::with_capacity(batch_size);
		for key in keys {
			keys_vec.push(key);
		}
		self.batch_delete(keys_vec).await
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
		let c = AnsiSqlDialect::filter_clause(scan)?;
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

	async fn batch_create<T>(&self, key_vals: Vec<(T, Value)>) -> Result<()>
	where
		T: ToSql + Sync,
	{
		// Fetch the columns to insert
		let columns = self
			.columns
			.0
			.iter()
			.map(|(name, _)| AnsiSqlDialect::escape_field(name.clone()))
			.collect::<Vec<String>>()
			.join(", ");
		// Store the records to insert
		let mut inserts = Vec::with_capacity(key_vals.len());
		// Store the query parameters
		let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
		// Store the column values
		let mut values: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
		// Store the row index
		let mut index = 1;
		// Iterate over the key-value pairs
		for (_, val) in &key_vals {
			// Add the id placeholder
			let mut row = vec![format!("${index}")];
			index += 1;
			// Process the columns
			if let Value::Object(obj) = val {
				for (column, column_type) in &self.columns.0 {
					// Add the column placeholder
					row.push(format!("${index}"));
					index += 1;
					// Add the column value with proper type conversion
					if let Some(value) = obj.get(column) {
						let value = convert_json_to_sql_param(column, column_type, value)?;
						values.push(value);
					} else {
						return Err(anyhow::anyhow!("Missing value for column {column}"));
					}
				}
			}
			// Add the row to the inserts
			inserts.push(format!("({})", row.join(", ")));
		}
		// Store the param index
		let mut index = 0;
		// Iterate over the key-value pairs
		for (key, val) in &key_vals {
			params.push(key);
			if let Value::Object(_) = val {
				for _ in &self.columns.0 {
					params.push(values[index].as_ref());
					index += 1;
				}
			}
		}
		// Build and execute the INSERT statement
		let stm = format!("INSERT INTO record (id, {columns}) VALUES {}", inserts.join(", "));
		let res = self.client.execute(&stm, &params).await?;
		assert_eq!(res, key_vals.len() as u64);
		Ok(())
	}

	async fn batch_read<T>(&self, keys: Vec<T>) -> Result<()>
	where
		T: ToSql + Sync,
	{
		// Store the record ids
		let params: Vec<&(dyn ToSql + Sync)> =
			keys.iter().map(|k| k as &(dyn ToSql + Sync)).collect();
		// Build the IN clause
		let ids = (1..=keys.len()).map(|i| format!("${i}")).collect::<Vec<String>>().join(", ");
		// Build and execute the DELETE statement
		let stm = format!("SELECT * FROM record WHERE id IN ({ids})");
		let res = self.client.query(&stm, &params).await?;
		assert_eq!(res.len(), keys.len());
		for row in res {
			black_box(self.consume(row, true).unwrap());
		}
		Ok(())
	}

	async fn batch_update<T>(&self, key_vals: Vec<(T, Value)>) -> Result<()>
	where
		T: ToSql + Sync,
	{
		// Store the columns to update
		let columns = self
			.columns
			.0
			.iter()
			.map(|(name, _)| {
				format!("{name} = data.{name}", name = AnsiSqlDialect::escape_field(name.clone()),)
			})
			.collect::<Vec<String>>()
			.join(", ");
		// Store the columns to select
		let fields = format!(
			"id, {}",
			self.columns
				.0
				.iter()
				.map(|(name, _)| AnsiSqlDialect::escape_field(name.clone()))
				.collect::<Vec<String>>()
				.join(", ")
		);
		// Store the records to insert
		let mut inserts = Vec::with_capacity(key_vals.len());
		// Store the query parameters
		let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
		// Store the column values
		let mut values: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
		// Store the row index
		let mut index = 1;
		// Iterate over the key-value pairs
		for (_, val) in &key_vals {
			// Start with the key parameter placeholder
			let mut row = vec![format!("${index}::{}", get_key_type(&self.kt))];
			index += 1;
			// Process each column value in the record
			if let Value::Object(obj) = val {
				for (column, column_type) in &self.columns.0 {
					// Add parameter placeholder for this column
					row.push(format!("${index}::{}", get_column_type(column_type)));
					index += 1;
					// Add the column value with proper type conversion
					if let Some(value) = obj.get(column) {
						let value = convert_json_to_sql_param(column, column_type, value)?;
						values.push(value);
					} else {
						return Err(anyhow::anyhow!("Missing value for column {column}"));
					}
				}
			}
			// Add the complete row to the VALUES construct
			inserts.push(format!("({})", row.join(", ")));
		}
		// Store the param index
		let mut index = 0;
		// Iterate over the key-value pairs
		for (key, val) in &key_vals {
			// Add the key as the first parameter
			params.push(key);
			// Add all column values as subsequent parameters
			if let Value::Object(_) = val {
				for _ in &self.columns.0 {
					params.push(values[index].as_ref());
					index += 1;
				}
			}
		}
		// Build and execute the UPDATE statement
		let stm = format!(
			"UPDATE record SET {columns} FROM (VALUES {}) AS data({fields}) WHERE record.id = data.id",
			inserts.join(", "),
		);
		let res = self.client.execute(&stm, &params).await?;
		assert_eq!(res, key_vals.len() as u64);
		Ok(())
	}

	async fn batch_delete<T>(&self, keys: Vec<T>) -> Result<()>
	where
		T: ToSql + Sync,
	{
		// Store the record ids
		let params: Vec<&(dyn ToSql + Sync)> =
			keys.iter().map(|k| k as &(dyn ToSql + Sync)).collect();
		// Build the IN clause
		let ids = (1..=keys.len()).map(|i| format!("${i}")).collect::<Vec<String>>().join(", ");
		// Build and execute the DELETE statement
		let stm = format!("DELETE FROM record WHERE id IN ({ids})");
		let res = self.client.execute(&stm, &params).await?;
		assert_eq!(res as usize, keys.len());
		Ok(())
	}
}

/// Get PostgreSQL type name for explicit casting
fn get_key_type(key_type: &KeyType) -> &'static str {
	match key_type {
		KeyType::Integer => "INTEGER",
		KeyType::String26 => "TEXT",
		KeyType::String90 => "TEXT",
		KeyType::String250 => "TEXT",
		KeyType::String506 => "TEXT",
		KeyType::Uuid => "UUID",
	}
}

/// Get PostgreSQL type name for explicit casting
fn get_column_type(column_type: &ColumnType) -> &'static str {
	match column_type {
		ColumnType::Integer => "INTEGER",
		ColumnType::Float => "REAL",
		ColumnType::Bool => "BOOLEAN",
		ColumnType::String => "TEXT",
		ColumnType::Object => "JSONB",
		ColumnType::DateTime => "TIMESTAMP",
		ColumnType::Uuid => "UUID",
	}
}

/// Convert a JSON value to a PostgreSQL parameter based on column type
fn convert_json_to_sql_param(
	column_name: &str,
	column_type: &ColumnType,
	json_value: &Value,
) -> Result<Box<dyn ToSql + Sync + Send>> {
	match column_type {
		ColumnType::Integer => {
			if let Some(int_val) = json_value.as_i64() {
				Ok(Box::new(int_val as i32))
			} else {
				Err(anyhow::anyhow!("Expected integer for column {column_name}"))
			}
		}
		ColumnType::Float => {
			if let Some(float_val) = json_value.as_f64() {
				Ok(Box::new(float_val as f32))
			} else {
				Err(anyhow::anyhow!("Expected float for column {column_name}"))
			}
		}
		ColumnType::Bool => {
			if let Some(bool_val) = json_value.as_bool() {
				Ok(Box::new(bool_val))
			} else {
				Err(anyhow::anyhow!("Expected boolean for column {column_name}"))
			}
		}
		ColumnType::String => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow::anyhow!("Expected string for column {column_name}"))
			}
		}
		ColumnType::Object => Ok(Box::new(Json(json_value.clone()))),
		ColumnType::DateTime => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow::anyhow!("Expected datetime string for column {column_name}"))
			}
		}
		ColumnType::Uuid => {
			if let Some(str_val) = json_value.as_str() {
				if let Ok(uuid) = uuid::Uuid::parse_str(str_val) {
					Ok(Box::new(uuid))
				} else {
					Err(anyhow::anyhow!("Invalid UUID for column {column_name}"))
				}
			} else {
				Err(anyhow::anyhow!("Expected UUID string for column {column_name}"))
			}
		}
	}
}
