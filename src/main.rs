use crate::benchmark::Benchmark;
use crate::database::Database;
use crate::keyprovider::KeyProvider;
use crate::valueprovider::ValueProvider;
use anyhow::{bail, Result};
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
mod terminal;
mod valueprovider;

// Datastore modules
mod arangodb;
mod dragonfly;
mod dry;
mod fjall;
mod keydb;
mod lmdb;
mod map;
mod mdbx;
mod mongodb;
mod mysql;
mod neo4j;
mod postgres;
mod redb;
mod redis;
mod rocksdb;
mod scylladb;
mod sqlite;
mod surrealdb;
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

	/// Size of the text value
	#[arg(
		short,
		long,
		env = "CRUD_BENCH_VALUE",
		hide_default_value = true,
		default_value = r#"{
			"text": "string:50",
			"number": "int:1..5000",
			"integer": "int",
			"words": "words:100;hello,world,foo,bar,test,search,data,query,index,document,database,performance"
		}"#
	)]
	pub(crate) value: String,

	/// An array of scan specifications
	#[arg(
		long,
		env = "CRUD_BENCH_SCANS",
		hide_default_value = true,
		default_value = r#"[
			{ "name": "count_all", "samples": 100, "projection": "COUNT" },
			{ "name": "limit_id", "samples": 10000, "projection": "ID", "limit": 100, "expect": 100 },
			{ "name": "limit_all", "samples": 10000, "projection": "FULL", "limit": 100, "expect": 100 },
			{ "name": "limit_start_id", "samples": 10000, "projection": "ID", "start": 5000, "limit": 100, "expect": 100 },
			{ "name": "limit_start_all", "samples": 10000, "projection": "FULL", "start": 5000, "limit": 100, "expect": 100 },
			{ "name": "where_field_integer_eq", "samples": 100, "projection": "FULL",
				"condition": {
					"sql": "number = 21",
					"mysql": "number = 21",
					"neo4j": "r.number = 21",
					"mongodb": { "number": { "$eq": 21 } },
					"arangodb": "r.number == 21",
					"surrealdb": "number = 21"
				},
				"index": {
					"fields": ["number"]
				}
			},
			{ "name": "where_field_integer_gte_lte", "samples": 100, "projection": "FULL", "limit": 100,
				"condition": {
					"sql": "number >= 18 AND number <= 21",
					"mysql": "number >= 18 AND number <= 21",
					"neo4j": "r.number >= 18 AND r.number <= 21",
					"mongodb": { "number": { "$gte": 18, "$lte": 21 } },
					"arangodb": "r.number >= 18 AND r.number <= 21",
					"surrealdb": "number >= 18 AND number <= 21"
				},
				"index": {
					"fields": ["number"]
				}
			},
			{ "name": "where_field_fulltext_single", "samples": 100, "projection": "FULL", "limit": 100,
				"condition": {
					"sql": "to_tsvector('english', words) @@ to_tsquery('english', 'hello')",
					"mysql": "MATCH(words) AGAINST('hello' IN NATURAL LANGUAGE MODE)",
					"neo4j": "hello",
					"mongodb": { "$text": { "$search": "hello" } },
					"surrealdb": "words @@ 'hello'"
				},
				"index": {
					"fields": ["words"],
					"index_type": "fulltext"
				}
			},
			{ "name": "where_field_fulltext_multi_and", "samples": 100, "projection": "FULL", "limit": 100,
				"condition": {
					"sql": "to_tsvector('english', words) @@ to_tsquery('english', 'hello & world')",
					"mysql": "MATCH(words) AGAINST('+hello +world' IN NATURAL LANGUAGE MODE)",
					"neo4j": "hello AND world",
					"mongodb": { "$text": { "$search": "hello world" } },
					"surrealdb": "words @@ 'hello' AND words @@ 'world'"
				},
				"index": {
					"fields": ["words"],
					"index_type": "fulltext"
				}
			},
			{ "name": "where_field_fulltext_multi_or", "samples": 100, "projection": "FULL", "limit": 100,
				"condition": {
					"sql": "to_tsvector('english', words) @@ to_tsquery('english', 'foo | bar')",
					"mysql": "MATCH(words) AGAINST('foo bar' IN NATURAL LANGUAGE MODE)",
					"neo4j": "foo OR bar",
					"mongodb": { "$text": { "$search": "foo bar" } },
					"surrealdb": "words @@ 'foo' OR words @@ 'bar'"
				},
				"index": {
					"fields": ["words"],
					"index_type": "fulltext"
				}
			}
		]"#
	)]
	pub(crate) scans: String,

	/// An array of batch operation specifications
	#[arg(
		long,
		env = "CRUD_BENCH_BATCHES",
		hide_default_value = true,
		default_value = r#"[
			{ "name": "batch_create_100", "operation": "CREATE", "batch_size": 100, "samples": 250 },
			{ "name": "batch_read_100", "operation": "READ", "batch_size": 100, "samples": 250 },
			{ "name": "batch_update_100", "operation": "UPDATE", "batch_size": 100, "samples": 250 },
			{ "name": "batch_delete_100", "operation": "DELETE", "batch_size": 100, "samples": 250 },
			{ "name": "batch_create_1000", "operation": "CREATE", "batch_size": 1000, "samples": 250 },
			{ "name": "batch_read_1000", "operation": "READ", "batch_size": 1000, "samples": 250 },
			{ "name": "batch_update_1000", "operation": "UPDATE", "batch_size": 1000, "samples": 250 },
			{ "name": "batch_delete_1000", "operation": "DELETE", "batch_size": 1000, "samples": 250 }
		]"#
	)]
	pub(crate) batches: String,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Index {
	pub(crate) fields: Vec<String>,
	pub(crate) unique: Option<bool>,
	pub(crate) index_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Scan {
	name: String,
	samples: Option<usize>,
	condition: Option<Condition>,
	start: Option<usize>,
	limit: Option<usize>,
	expect: Option<usize>,
	projection: Option<String>,
	index: Option<Index>,
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
		.thread_stack_size(5 * 1024 * 1024) // Set stack size to 5MiB
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
	// Build the value provider
	let vp = ValueProvider::new(&args.value)?;
	// Run the benchmark
	let res = runtime.block_on(async {
		args.database
			.run(
				&mut benchmark,
				args.key,
				kp,
				vp,
				&args.scans,
				&args.batches,
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
	use crate::{run, Args, Database, KeyType};
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
