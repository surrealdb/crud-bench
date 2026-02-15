use crate::benchmark::Benchmark;
use crate::database::Database;
use crate::keyprovider::KeyProvider;
use crate::valueprovider::ValueProvider;
use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};
use docker::Container;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{IsTerminal, Write};
use tokio::runtime;

// Benchmark modules
mod allocator;
mod benchmark;
mod chart;
mod database;
mod dialect;
mod docker;
mod engine;
mod keyprovider;
mod memory;
mod profiling;
mod result;
mod storage;
mod system;
mod valueprovider;

// Datastore modules
mod arangodb;
mod dragonfly;
mod dry;
mod fjall;
mod keydb;
mod lmdb;
mod map;
mod mariadb;
mod mdbx;
mod mongodb;
mod mysql;
mod neo4j;
mod postgres;
mod redb;
mod redis;
mod rocksdb;
mod scylladb;
mod slatedb;
mod sqlite;
mod surrealdb;
mod surrealdb2;
mod surrealds;
mod surrealkv;
mod surrealmx;

#[derive(Parser, Debug)]
#[command(term_width = 0)]
pub(crate) struct Args {
	/// An optional name for the test, used as a suffix for the JSON result file name
	#[arg(short, long)]
	pub(crate) name: Option<String>,

	/// The database to benchmark
	#[arg(short, long)]
	pub(crate) database: Database,

	/// Specify a custom Docker image
	#[arg(short, long)]
	pub(crate) image: Option<String>,

	/// Whether to run Docker in privileged mode
	#[arg(short, long)]
	pub(crate) privileged: bool,

	/// Specify a custom endpoint to connect to
	#[arg(short, long)]
	pub(crate) endpoint: Option<String>,

	/// Maximum number of blocking threads (default is the number of CPU cores)
	#[arg(short, long, default_value=num_cpus::get().to_string(), value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) blocking: u32,

	/// Number of async runtime workers (default is the number of CPU cores)
	#[arg(short, long, default_value=num_cpus::get().to_string(), value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) workers: u32,

	/// Number of concurrent clients
	#[arg(short, long, default_value = "1", value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) clients: u32,

	/// Number of concurrent threads per client
	#[arg(short, long, default_value = "1", value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) threads: u32,

	/// Number of samples to be created, read, updated, and deleted
	#[arg(short, long, value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) samples: u32,

	/// Override scan sample counts (number of query iterations per scan test)
	#[arg(long, value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) scan_samples: Option<u32>,

	/// Generate the keys in a pseudo-randomized order
	#[arg(short, long)]
	pub(crate) random: bool,

	/// Whether to ensure data is synced and durable
	#[arg(long, default_value = "false")]
	pub(crate) sync: bool,

	/// Whether to enable disk persistence for Redis-family databases
	#[arg(long, default_value = "false")]
	pub(crate) persisted: bool,

	/// Use optimised database configurations instead of defaults
	#[arg(long, default_value = "false")]
	pub(crate) optimised: bool,

	/// The type of the key
	#[arg(short, long, default_value_t = KeyType::Integer, value_enum)]
	pub(crate) key: KeyType,

	/// Print-out an example of a generated value
	#[arg(long)]
	pub(crate) show_sample: bool,

	/// Collect system information for a given pid
	#[arg(long, value_parser=clap::value_parser!(u32).range(0..))]
	pub(crate) pid: Option<u32>,

	/// Store benchmark results in SurrealDB
	#[arg(long)]
	pub(crate) store_results: bool,

	/// SurrealDB endpoint for storing results
	#[arg(long, env = "CRUD_BENCH_STORAGE_ENDPOINT", default_value = "ws://localhost:8000")]
	pub(crate) storage_endpoint: String,

	/// The value specification as inline JSON or @path to a JSON file
	#[arg(
		short,
		long,
		env = "CRUD_BENCH_VALUE",
		default_value = "@config/value.json",
		hide_default_value = true
	)]
	pub(crate) value: String,

	/// An array of scan specifications as inline JSON or @path to a JSON file
	#[arg(
		long,
		env = "CRUD_BENCH_SCANS",
		default_value = "@config/scans.json",
		hide_default_value = true
	)]
	pub(crate) scans: String,

	/// An array of batch operation specifications as inline JSON or @path to a JSON file
	#[arg(
		long,
		env = "CRUD_BENCH_BATCHES",
		default_value = "@config/batches.json",
		hide_default_value = true
	)]
	pub(crate) batches: String,

	/// Skip all scan benchmarks
	#[arg(long, default_value = "false")]
	pub(crate) skip_scans: bool,

	/// Skip all batch benchmarks
	#[arg(long, default_value = "false")]
	pub(crate) skip_batches: bool,

	/// Skip index operations, but still table scan queries
	#[arg(long, default_value = "false")]
	pub(crate) skip_indexes: bool,

	/// Setup queries to run after creates and before scans (e.g. graph edges, secondary tables).
	/// Inline JSON or @path to a JSON file with per-dialect query arrays.
	#[arg(
		long,
		env = "CRUD_BENCH_SETUP",
		hide_default_value = true
	)]
	pub(crate) setup: Option<String>,
}

#[derive(Debug, ValueEnum, Clone, Copy)]
pub(crate) enum KeyType {
	/// 4 bytes integer
	Integer,
	/// 26 ascii bytes
	String26,
	/// 90 ascii bytes
	String90,
	/// 250 ascii bytes
	String250,
	/// 506 ascii bytes
	String506,
	/// UUID type 7
	Uuid,
}

pub(crate) type Scans = Vec<Scan>;

pub(crate) type Batches = Vec<BatchOperation>;

/// Setup queries to run after creates and before scans.
/// Each field contains an array of queries for that dialect.
/// When `surrealdb2` is absent, falls back to `surrealdb`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub(crate) struct SetupConfig {
	pub(crate) sql: Option<Vec<String>>,
	pub(crate) mysql: Option<Vec<String>>,
	pub(crate) neo4j: Option<Vec<String>>,
	pub(crate) mongodb: Option<Vec<String>>,
	pub(crate) arangodb: Option<Vec<String>>,
	pub(crate) surrealdb: Option<Vec<String>>,
	pub(crate) surrealdb2: Option<Vec<String>>,
}

impl SetupConfig {
	/// Returns setup queries for the given dialect, with fallback.
	pub fn queries_for(&self, dialect: &str) -> Option<&[String]> {
		match dialect {
			"surrealdb2" => self
				.surrealdb2
				.as_deref()
				.or(self.surrealdb.as_deref()),
			"surrealdb" => self.surrealdb.as_deref(),
			"sql" => self.sql.as_deref(),
			"mysql" => self.mysql.as_deref(),
			"neo4j" => self.neo4j.as_deref(),
			"mongodb" => self.mongodb.as_deref(),
			"arangodb" => self.arangodb.as_deref(),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Index {
	#[serde(default)]
	pub(crate) skip: bool,
	pub(crate) fields: Vec<String>,
	pub(crate) unique: Option<bool>,
	pub(crate) index_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Scan {
	name: String,
	samples: Option<usize>,
	condition: Option<Condition>,
	/// Full raw query per dialect. When set, bypasses auto-generated SELECT.
	query: Option<Condition>,
	start: Option<usize>,
	limit: Option<usize>,
	expect: Option<usize>,
	projection: Option<String>,
	index: Option<Index>,
	/// Dialects for which this scan should be skipped entirely.
	#[serde(default)]
	skip_for: Vec<String>,
}

impl Scan {
	/// Returns the scan projection type
	fn projection(&self) -> Result<Projection> {
		match self.projection.as_deref() {
			Some("ID") => Ok(Projection::Id),
			Some("FULL") => Ok(Projection::Full),
			Some("COUNT") => Ok(Projection::Count),
			Some(o) => bail!(format!("Unsupported projection: {}", o)),
			_ => Ok(Projection::Full),
		}
	}

	/// Returns the raw query for the given dialect, with fallback.
	/// For "surrealdb2", falls back to "surrealdb" if no v2-specific query is set.
	pub fn raw_query(&self, dialect: &str) -> Option<&str> {
		self.query.as_ref().and_then(|q| match dialect {
			"surrealdb2" => q.surrealdb2.as_deref().or(q.surrealdb.as_deref()),
			"surrealdb" => q.surrealdb.as_deref(),
			"sql" => q.sql.as_deref(),
			"mysql" => q.mysql.as_deref(),
			"neo4j" => q.neo4j.as_deref(),
			"arangodb" => q.arangodb.as_deref(),
			_ => None,
		})
	}

	/// Returns the query or condition text for the given dialect.
	/// Tries raw query first, then falls back to the condition string.
	pub fn query_text(&self, dialect: &str) -> Option<String> {
		self.raw_query(dialect)
			.map(|s| s.to_string())
			.or_else(|| {
				self.condition.as_ref().and_then(|c| match dialect {
					"surrealdb2" => c.surrealdb2.clone().or(c.surrealdb.clone()),
					"surrealdb" => c.surrealdb.clone(),
					"sql" => c.sql.clone(),
					"mysql" => c.mysql.clone(),
					"neo4j" => c.neo4j.clone(),
					"arangodb" => c.arangodb.clone(),
					_ => None,
				})
			})
	}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum Projection {
	Id,
	Full,
	Count,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Condition {
	sql: Option<String>,
	mysql: Option<String>,
	neo4j: Option<String>,
	mongodb: Option<Value>,
	arangodb: Option<String>,
	surrealdb: Option<String>,
	/// SurrealDB v2-specific override. Falls back to `surrealdb` if unset.
	surrealdb2: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct BatchOperation {
	pub(crate) name: String,
	pub(crate) operation: BatchOperationType,
	pub(crate) batch_size: usize,
	pub(crate) samples: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum BatchOperationType {
	Create,
	Read,
	Update,
	Delete,
}

/// Resolves a JSON argument value. If the value starts with `@`, it is
/// treated as a file path and the contents are read from disk. Otherwise
/// the value is returned as-is (inline JSON).
fn resolve_json_arg(value: &str) -> Result<String> {
	if let Some(path) = value.strip_prefix('@') {
		std::fs::read_to_string(path)
			.map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", path, e))
	} else {
		Ok(value.to_string())
	}
}

fn main() -> Result<()> {
	// Initialise the logger
	env_logger::init();
	// Parse the command line arguments
	let args = Args::parse();
	// Run the benchmark
	run(args)
}

fn run(args: Args) -> Result<()> {
	// Check if we should profile
	if std::env::var("PROFILE").is_ok() {
		profiling::initialise();
	}
	// Prepare the benchmark
	let mut benchmark = Benchmark::new(&args);
	// If a Docker image is specified but the endpoint, spawn the container.
	let container = if args.endpoint.is_some() {
		// The endpoint is specified usually when you want the benchmark to run against a remote server.
		// Not handling this results in crud-bench starting a container never used by the client and the benchmark.
		None
	} else {
		args.database.start_docker(&benchmark)
	};
	// Setup the asynchronous runtime
	let runtime = runtime::Builder::new_multi_thread()
		.thread_stack_size(2 * 1024 * 1024) // Set stack size to 5MiB
		.max_blocking_threads(args.blocking as usize) // Set the number of blocking threads
		.worker_threads(args.workers as usize) // Set the number of worker threads
		.thread_name("crud-bench-runtime") // Set the name of the runtime threads
		.enable_all() // Enables all runtime features, including I/O and time
		.build()
		.expect("Failed to create a runtime");
	// Setup the blocking thread pool
	let _ = affinitypool::Builder::new()
		.thread_stack_size(5 * 1024 * 1024) // Set stack size to 5MiB
		.worker_threads(args.blocking as usize) // Set the number of worker threads
		.thread_name("crud-bench-threadpool") // Set the name of the threadpool threads
		.thread_per_core(true) // Try to set a thread per core
		.build()
		.build_global();
	// Display formatting
	if std::io::stdout().is_terminal() {
		println!("--------------------------------------------------");
	}
	// Collect system information
	let system = system::collect();
	// Create benchmark metadata
	let metadata = result::BenchmarkMetadata {
		samples: args.samples,
		clients: args.clients,
		threads: args.threads,
		key_type: format!("{:?}", args.key),
		random: args.random,
		sync: args.sync,
		persisted: args.persisted,
		optimised: args.optimised,
	};
	// Get database display name
	let name = args.database.name().to_string();
	// Build the key provider
	let kp = KeyProvider::new(args.key, args.random);
	// Resolve the value, scans, and batches arguments (inline JSON or @file)
	let value = resolve_json_arg(&args.value)?;
	let scans_str = resolve_json_arg(&args.scans)?;
	let batches_str = resolve_json_arg(&args.batches)?;
	// Build the value provider
	let vp = ValueProvider::new(&value)?;
	// Parse the batches configuration
	let mut batches: Batches = serde_json::from_str(&batches_str)?;
	// Check if we should skip batches
	if args.skip_batches {
		batches.clear();
	}
	// Parse the scans configuration
	let mut scans: Scans = serde_json::from_str(&scans_str)?;
	// Check if we should skip scans
	if args.skip_scans {
		scans.clear();
	} else if args.skip_indexes {
		for scan in &mut scans {
			if let Some(index) = scan.index.as_mut() {
				index.skip = true;
			}
		}
	}
	// Parse the optional setup configuration
	let setup: SetupConfig = match &args.setup {
		Some(setup_arg) => {
			let setup_str = resolve_json_arg(setup_arg)?;
			serde_json::from_str(&setup_str)?
		}
		None => SetupConfig::default(),
	};
	// Run the benchmark
	let res = runtime.block_on(async {
		args.database
			.run(
				&mut benchmark,
				args.key,
				kp,
				vp,
				scans,
				batches,
				setup,
				Some(name.clone()),
				Some(system),
				Some(metadata),
			)
			.await
	});
	// Check if we should profile
	if std::env::var("PROFILE").is_ok() {
		profiling::process();
	}
	// Output the results
	match res {
		// Output the results
		Ok(res) => {
			println!("--------------------------------------------------");
			match container.as_ref().map(Container::image) {
				Some(v) => {
					print!("Benchmark result for {:?} on docker {v}", args.database)
				}
				None => match args.endpoint {
					Some(endpoint) => {
						print!("Benchmark result for {:?}; endpoint => {endpoint}", args.database)
					}
					None => print!("Benchmark result for {:?}", args.database),
				},
			}
			println!("{}", args.name.as_ref().map(|s| format!(" - {s}")).unwrap_or_default());
			println!(
				"CPUs: {} - Blocking threads: {} - Workers: {} - Clients: {} - Threads: {} - Samples: {} - Key: {:?} - Random: {}",
				num_cpus::get(),
				args.blocking,
				args.workers,
				args.clients,
				args.threads,
				args.samples,
				args.key,
				args.random,
			);
			println!("--------------------------------------------------");
			println!("{res}");
			println!("--------------------------------------------------");
			if args.show_sample {
				println!("Value sample: {:#}", res.sample);
				println!("--------------------------------------------------");
			}

			// Serialize the struct to a JSON string
			let json_string = serde_json::to_string_pretty(&res)?;

			// Write the JSON string to a file
			let result_name = args
				.name
				.as_ref()
				.map(|s| format!("result-{s}.json"))
				.unwrap_or_else(|| "result.json".to_string());
			let mut file = File::create(result_name)?;
			file.write_all(json_string.as_bytes())?;

			// Write the CSV file
			let result_csv_name = args
				.name
				.as_ref()
				.map(|s| format!("result-{s}.csv"))
				.unwrap_or_else(|| "result.csv".to_string());
			res.to_csv(&result_csv_name)?;

			// Write the HTML chart file
			let result_html_name = args
				.name
				.as_ref()
				.map(|s| format!("result-{s}.html"))
				.unwrap_or_else(|| "result.html".to_string());
			res.to_html_charts(&result_html_name, &name)?;
			println!("📊 Interactive charts saved to: {}", result_html_name);

			// Store results in SurrealDB if requested
			if args.store_results {
				match runtime.block_on(async {
					let client = storage::StorageClient::connect(&args.storage_endpoint).await?;
					client.store_result(&res).await
				}) {
					Ok(_) => {
						println!("💾 Results stored in SurrealDB at: {}", args.storage_endpoint)
					}
					Err(e) => eprintln!("⚠️ Failed to store results in SurrealDB: {e}"),
				}
			}

			Ok(())
		}
		// Output the errors
		Err(e) => {
			// Print the error output of the benchmark
			eprintln!("--------------------------------------------------");
			eprintln!("Failure: {e}");
			eprintln!("--------------------------------------------------");
			// Print the error output of the container
			if container.is_some() {
				match Container::logs() {
					Ok(stdout) => eprintln!("{stdout}"),
					Err(stderr) => eprintln!("{stderr}"),
				}
			}
			Err(e)
		}
	}
}

#[cfg(test)]
mod test {
	use crate::{Args, Database, KeyType, run};
	use anyhow::Result;
	use serial_test::serial;

	fn test(database: Database, key: KeyType, random: bool) -> Result<()> {
		run(Args {
			image: None,
			name: None,
			database,
			privileged: false,
			endpoint: None,
			blocking: 5,
			workers: 5,
			clients: 2,
			threads: 2,
			samples: 10000,
			scan_samples: None,
			sync: false,
			persisted: false,
			optimised: false,
			random,
			key,
			value: r#"{"text":"String:50", "integer":"int"}"#.to_string(),
			scans: r#"[{"name": "limit", "start": 50, "limit": 100, "expect": 100}]"#.to_string(),
			batches:
				r#"[{"name": "batch_test", "operation": "CREATE", "batch_size": 5, "samples": 10}]"#
					.to_string(),
			show_sample: false,
			pid: None,
			store_results: false,
			storage_endpoint: "ws://localhost:8000".to_string(),
			skip_scans: false,
			skip_batches: false,
			skip_indexes: false,
			setup: None,
		})
	}

	#[test]
	#[serial]
	fn test_integer_ordered() -> Result<()> {
		test(Database::Map, KeyType::Integer, false)
	}

	#[test]
	#[serial]
	fn test_integer_unordered_dry() -> Result<()> {
		test(Database::Dry, KeyType::Integer, true)
	}

	#[test]
	#[serial]
	fn test_integer_unordered_map() -> Result<()> {
		test(Database::Map, KeyType::Integer, true)
	}

	#[test]
	#[serial]
	fn test_string26_ordered() -> Result<()> {
		test(Database::Map, KeyType::String26, false)
	}

	#[test]
	#[serial]
	fn test_string26_unordered() -> Result<()> {
		test(Database::Map, KeyType::String26, true)
	}

	#[test]
	#[serial]
	fn test_string90_ordered() -> Result<()> {
		test(Database::Map, KeyType::String90, false)
	}

	#[test]
	#[serial]
	fn test_string90_unordered() -> Result<()> {
		test(Database::Map, KeyType::String90, true)
	}

	#[test]
	#[serial]
	fn test_string250_ordered() -> Result<()> {
		test(Database::Map, KeyType::String250, false)
	}

	#[test]
	#[serial]
	fn test_string250_unordered() -> Result<()> {
		test(Database::Map, KeyType::String250, true)
	}

	#[test]
	#[serial]
	fn test_string506_ordered() -> Result<()> {
		test(Database::Map, KeyType::String506, false)
	}

	#[test]
	#[serial]
	fn test_string506_unordered_map() -> Result<()> {
		test(Database::Map, KeyType::String506, true)
	}

	#[test]
	#[serial]
	fn test_string506_unordered_dry() -> Result<()> {
		test(Database::Dry, KeyType::String506, true)
	}
}
