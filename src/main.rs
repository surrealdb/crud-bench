use crate::benchmark::Benchmark;
use std::io::IsTerminal;

use crate::database::Database;
use crate::keyprovider::KeyProvider;
use anyhow::Result;
use clap::{Parser, ValueEnum};
use tokio::runtime::Builder;

mod benchmark;
mod database;
mod docker;
mod dry;
mod keydb;
mod keyprovider;
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

	/// Number of async runtime workers, defaulting to the number of CPUs
	#[clap(short, long, default_value=num_cpus::get().to_string(), value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) workers: u32,

	/// Number of concurrent clients
	#[clap(short, long, default_value = "1", value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) clients: u32,

	/// Number of concurrent threads per client
	#[clap(short, long, default_value = "1", value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) threads: u32,

	/// Number of samples to be created, read, updated, and deleted
	#[clap(short, long, value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) samples: u32,

	/// Generate the keys in a pseudo-randomized order
	#[clap(short, long)]
	pub(crate) random: bool,

	/// The type of the key
	#[clap(short, long, default_value_t = KeyType::Integer, value_enum)]
	pub(crate) key: KeyType,
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
	// Setup the asynchronous runtime
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
	// Run the benchmark
	let res = runtime.block_on(async { args.database.run(&benchmark, args.key, kp).await });
	// Output the results
	match res {
		// Output the results
		Ok(res) => {
			println!("--------------------------------------------------");
			match image {
				Some(v) => println!("Benchmark result for {:?} on docker {v}", args.database),
				None => println!("Benchmark result for {:?}", args.database),
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
			Ok(())
		}
		// Output the errors
		Err(e) => {
			if let Some(container) = &container {
				container.logs();
			}
			Err(e)
		}
	}
}

#[cfg(test)]
mod test {
	use crate::{run, Args, Database, KeyType};
	use anyhow::Result;
	fn test(key: KeyType, random: bool) -> Result<()> {
		run(Args {
			image: None,
			database: Database::Dry,
			endpoint: None,
			workers: 5,
			clients: 2,
			threads: 2,
			samples: 10000,
			random,
			key,
		})
	}

	#[test]
	fn test_integer_ordered() -> Result<()> {
		test(KeyType::Integer, false)
	}

	#[test]
	fn test_integer_unordered() -> Result<()> {
		test(KeyType::Integer, true)
	}

	#[test]
	fn test_string26_ordered() -> Result<()> {
		test(KeyType::String26, false)
	}

	#[test]
	fn test_string26_unordered() -> Result<()> {
		test(KeyType::String26, true)
	}

	#[test]
	fn test_string90_ordered() -> Result<()> {
		test(KeyType::String90, false)
	}

	#[test]
	fn test_string90_unordered() -> Result<()> {
		test(KeyType::String90, true)
	}

	#[test]
	fn test_string506_ordered() -> Result<()> {
		test(KeyType::String506, false)
	}

	#[test]
	fn test_string506_unordered() -> Result<()> {
		test(KeyType::String506, true)
	}
}
