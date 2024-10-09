use crate::benchmark::Benchmark;
use std::io::IsTerminal;

use crate::database::Database;
use crate::keyprovider::{KeyProvider, OrderedInteger, UnorderedInteger};
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
	/// 16 ascii bytes
	String16,
	/// 68 ascii bytes
	String68,
	/// UUID type 7
	Uuid,
}

fn main() {
	// Initialise the logger
	env_logger::init();
	// Parse the command line arguments
	let args = Args::parse();
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
	// Run the benchmark
	let res = runtime.block_on(async {
		if args.random {
			match args.key {
				KeyType::Integer => {
					UnorderedInteger::default().run(&benchmark, &args.database).await
				}
				KeyType::String16 => {
					todo!()
				}
				KeyType::String68 => {
					todo!()
				}
				KeyType::Uuid => {
					todo!()
				}
			}
		} else {
			match args.key {
				KeyType::Integer => OrderedInteger::default().run(&benchmark, &args.database).await,
				KeyType::String16 => {
					todo!()
				}
				KeyType::String68 => {
					todo!()
				}
				KeyType::Uuid => {
					todo!()
				}
			}
		}
	});
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
				"CPUs: {} - Workers: {} - Clients: {} - Threads: {} - Samples: {} - Random: {}",
				num_cpus::get(),
				args.workers,
				args.clients,
				args.threads,
				args.samples,
				args.random,
			);
			println!("--------------------------------------------------");
			println!("{res}");
			println!("--------------------------------------------------");
		}
		// Output the errors
		Err(e) => {
			if let Some(container) = &container {
				container.logs();
			}
			eprintln!("Failure: {e}");
		}
	}
}
