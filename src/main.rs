use crate::benchmark::Benchmark;
use crate::database::Database;
use crate::keyprovider::KeyProvider;
use crate::valueprovider::ValueProvider;
use anyhow::{bail, Result};
use clap::{Parser, ValueEnum};
use docker::DockerContainer;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{IsTerminal, Write};
use tokio::runtime;

// Benchmark modules
mod benchmark;
mod database;
mod dialect;
mod docker;
mod engine;
mod keyprovider;
mod result;
mod terminal;
mod valueprovider;

// Datastore modules
mod arangodb;
mod dragonfly;
mod dry;
mod keydb;
mod lmdb;
mod map;
mod mongodb;
mod mysql;
mod postgres;
mod redb;
mod redis;
mod rocksdb;
mod scylladb;
mod speedb;
mod sqlite;
mod surrealdb;
mod surrealkv;

#[derive(Parser, Debug)]
#[command(term_width = 0)]
pub(crate) struct Args {
	/// Docker image
	#[arg(short, long)]
	pub(crate) image: Option<String>,

	/// An optional name for the test, used as a suffix for the JSON result file name
	#[arg(short, long)]
	pub(crate) name: Option<String>,

	/// Database
	#[arg(short, long)]
	pub(crate) database: Database,

	/// Endpoint
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

	/// The type of the key
	#[arg(short, long, default_value_t = KeyType::Integer, value_enum)]
	pub(crate) key: KeyType,

	/// Size of the text value
	#[arg(
		short,
		long,
		env = "CRUD_BENCH_VALUE",
		default_value = r#"{
			"text": "string:50",
			"integer": "int"
		}"#
	)]
	pub(crate) value: String,

	/// Print-out an example of a generated value
	#[arg(long)]
	pub(crate) show_sample: bool,

	/// Collect system information for a given pid
	#[arg(short, long, value_parser=clap::value_parser!(u32).range(0..))]
	pub(crate) pid: Option<u32>,

	/// An array of scan specifications
	#[arg(
		short = 'a',
		long,
		env = "CRUD_BENCH_SCANS",
		default_value = r#"[
			{ "name": "count_all", "samples": 100, "projection": "COUNT" },
			{ "name": "limit_keys", "samples": 100, "projection": "ID", "limit": 100, "expect": 100 },
			{ "name": "limit_full", "samples": 100, "projection": "FULL", "limit": 100, "expect": 100 },
			{ "name": "limit_count", "samples": 100, "projection": "COUNT", "limit": 100, "expect": 100 }
		]"#
	)]
	pub(crate) scans: String,
}

#[derive(Debug, ValueEnum, Clone, Copy)]
pub(crate) enum KeyType {
	/// 4 bytes integer
	Integer,
	/// 26 ascii bytes
	String26,
	/// 90 ascii bytes
	String90,
	/// 506 ascii bytes
	String506,
	/// UUID type 7
	Uuid,
}

pub(crate) type Scans = Vec<Scan>;
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Scan {
	name: String,
	samples: Option<usize>,
	condition: Option<String>,
	start: Option<usize>,
	limit: Option<usize>,
	expect: Option<usize>,
	projection: Option<String>,
}

#[derive(Debug)]
pub(crate) enum Projection {
	Id,
	Full,
	Count,
}
impl Scan {
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

fn main() -> Result<()> {
	// Initialise the logger
	env_logger::init();
	// Parse the command line arguments
	let args = Args::parse();
	// Run the benchmark
	run(args)
}

fn run(args: Args) -> Result<()> {
	// Prepare the benchmark
	let benchmark = Benchmark::new(&args);
	// If a Docker image is specified but the endpoint, spawn the container.
	let container = if args.endpoint.is_some() {
		// The endpoint is specified usually when you want the benchmark to run against a remote server.
		// Not handling this results in crud-bench starting a container never used by the client and the benchmark.
		None
	} else {
		args.database.start_docker(args.image)
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
	// Build the key provider
	let kp = KeyProvider::new(args.key, args.random);
	// Build the value provider
	let vp = ValueProvider::new(&args.value)?;
	// Run the benchmark
	let res = runtime
		.block_on(async { args.database.run(&benchmark, args.key, kp, vp, &args.scans).await });
	// Output the results
	match res {
		// Output the results
		Ok(res) => {
			println!("--------------------------------------------------");
			match container.as_ref().map(DockerContainer::image) {
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
			println!("{}", args.name.as_ref().map(|s| format!(" - {}", s)).unwrap_or_default());
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
				.map(|s| format!("result-{}.json", s))
				.unwrap_or_else(|| "result.json".to_string());
			let mut file = File::create(result_name)?;
			file.write_all(json_string.as_bytes())?;

			Ok(())
		}
		// Output the errors
		Err(e) => {
			if let Some(container) = &container {
				eprintln!("--------------------------------------------------");
				container.logs();
			}
			eprintln!("--------------------------------------------------");
			eprintln!("Failure: {e}");
			eprintln!("--------------------------------------------------");
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
			endpoint: None,
			blocking: 5,
			workers: 5,
			clients: 2,
			threads: 2,
			samples: 10000,
			random,
			key,
			value: r#"{"text":"String:50", "integer":"int"}"#.to_string(),
			scans: r#"[{"name": "limit", "start": 50, "limit": 100, "expect": 100}]"#.to_string(),
			show_sample: false,
			pid: None,
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
