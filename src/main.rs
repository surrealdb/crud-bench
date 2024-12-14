use crate::benchmark::Benchmark;
use crate::database::Database;
use crate::keyprovider::KeyProvider;
use crate::valueprovider::ValueProvider;
use anyhow::{bail, Result};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::io::IsTerminal;
use tokio::runtime::Builder;

// Benchmark modules
mod benchmark;
mod database;
mod dialect;
mod docker;
mod keyprovider;
mod result;
mod valueprovider;

// Datastore modules
mod dragonfly;
mod dry;
mod keydb;
mod map;
mod mongodb;
mod postgres;
mod redb;
mod redis;
mod rocksdb;
mod scylladb;
mod speedb;
mod surrealdb;
mod surrealkv;

#[derive(Parser, Debug)]
#[command(term_width = 0)]
pub(crate) struct Args {
	/// Docker image
	#[arg(short, long)]
	pub(crate) image: Option<String>,

	/// Database
	#[arg(short, long)]
	pub(crate) database: Database,

	/// Endpoint
	#[arg(short, long)]
	pub(crate) endpoint: Option<String>,

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
			{ "name": "count_all", "samples": 10, "projection": "COUNT" },
			{ "name": "limit_keys", "samples": 10, "projection": "ID", "limit": 100, "expect": 100 },
			{ "name": "limit_full", "samples": 10, "projection": "FULL", "limit": 100, "expect": 100 },
			{ "name": "limit_count", "samples": 10, "projection": "COUNT", "limit": 100, "expect": 100 }
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
	// If a Docker image is specified, spawn the container
	let container = args.database.start_docker(args.image);
	let image = container.as_ref().map(|c| c.image().to_string());
	// Set up the asynchronous runtime
	let runtime = Builder::new_multi_thread()
		.thread_stack_size(10 * 1024 * 1024) // Set stack size to 10MiB
		.worker_threads(args.workers as usize) // Set the number of worker threads
		.enable_all() // Enables all runtime features, including I/O and time
		.build()
		.expect("Failed to create a runtime");
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
			match image {
				Some(v) => println!("Benchmark result for {:?} on docker {v}", args.database),
				None => match args.endpoint {
					Some(endpoint) => {
						println!("Benchmark result for {:?}; endpoint => {endpoint}", args.database)
					}
					None => println!("Benchmark result for {:?}", args.database),
				},
			}
			println!(
				"CPUs: {} - Workers: {} - Clients: {} - Threads: {} - Samples: {} - Key: {:?} - Random: {}",
				num_cpus::get(),
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
			database,
			endpoint: None,
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
