use crate::benchmark::{Benchmark, BenchmarkResult};
use crate::docker::DockerContainer;
use crate::docker::DockerParams;
use crate::dry::DryClientProvider;
use std::io::IsTerminal;
use std::process::ExitCode;

#[cfg(feature = "keydb")]
use crate::keydb::KeydbClientProvider;
#[cfg(feature = "mongodb")]
use crate::mongodb::MongoDBClientProvider;
#[cfg(feature = "postgres")]
use crate::postgres::PostgresClientProvider;
#[cfg(feature = "rocksdb")]
use crate::redb::ReDBClientProvider;
#[cfg(feature = "redis")]
use crate::redis::RedisClientProvider;
#[cfg(feature = "rocksdb")]
use crate::rocksdb::RocksDBClientProvider;
#[cfg(feature = "scylladb")]
use crate::scylladb::ScyllaDBClientProvider;
#[cfg(feature = "speedb")]
use crate::speedb::SpeeDBClientProvider;
#[cfg(feature = "surrealdb")]
use crate::surrealdb::SurrealDBClientProvider;
#[cfg(feature = "surrealkv")]
use crate::surrealkv::SurrealKVClientProvider;
use anyhow::Result;
use clap::{Parser, ValueEnum};
use tokio::runtime::Builder;

mod benchmark;
mod docker;
mod dry;
mod keydb;
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
	#[clap(short, long, value_parser=clap::value_parser!(i32).range(1..))]
	pub(crate) samples: i32,
}

#[derive(ValueEnum, Debug, Clone)]
pub(crate) enum Database {
	Dry,
	#[cfg(feature = "redb")]
	Redb,
	#[cfg(feature = "speedb")]
	Speedb,
	#[cfg(feature = "rocksdb")]
	Rocksdb,
	#[cfg(feature = "surrealkv")]
	Surrealkv,
	#[cfg(feature = "surrealdb")]
	Surrealdb,
	#[cfg(feature = "surrealdb")]
	SurrealdbMemory,
	#[cfg(feature = "surrealdb")]
	SurrealdbRocksdb,
	#[cfg(feature = "surrealdb")]
	SurrealdbSurrealkv,
	#[cfg(feature = "scylladb")]
	Scylladb,
	#[cfg(feature = "mongodb")]
	Mongodb,
	#[cfg(feature = "postgres")]
	Postgres,
	#[cfg(feature = "redis")]
	Redis,
	#[cfg(feature = "keydb")]
	Keydb,
}

impl Database {
	/// Start the Docker container if necessary
	fn start_docker(&self, image: Option<String>) -> Option<DockerContainer> {
		let params: DockerParams = match self {
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbMemory => surrealdb::SURREALDB_MEMORY_DOCKER_PARAMS,
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbRocksdb => surrealdb::SURREALDB_ROCKSDB_DOCKER_PARAMS,
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbSurrealkv => surrealdb::SURREALDB_SURREALKV_DOCKER_PARAMS,
			#[cfg(feature = "scylladb")]
			Database::Scylladb => scylladb::SCYLLADB_DOCKER_PARAMS,
			#[cfg(feature = "mongodb")]
			Database::Mongodb => mongodb::MONGODB_DOCKER_PARAMS,
			#[cfg(feature = "postgres")]
			Database::Postgres => postgres::POSTGRES_DOCKER_PARAMS,
			#[cfg(feature = "redis")]
			Database::Redis => redis::REDIS_DOCKER_PARAMS,
			#[cfg(feature = "keydb")]
			Database::Keydb => keydb::KEYDB_DOCKER_PARAMS,
			#[allow(unreachable_patterns)]
			_ => return None,
		};
		let image = image.unwrap_or(params.image.to_string());
		let container = DockerContainer::start(image, params.pre_args, params.post_args);
		Some(container)
	}
	/// Run the benchmarks for the chosen database
	async fn run(&self, benchmark: &Benchmark) -> Result<BenchmarkResult> {
		match self {
			Database::Dry => benchmark.run(DryClientProvider::default()).await,
			#[cfg(feature = "redb")]
			Database::Redb => benchmark.run(ReDBClientProvider::setup().await?).await,
			#[cfg(feature = "speedb")]
			Database::Speedb => benchmark.run(SpeeDBClientProvider::setup().await?).await,
			#[cfg(feature = "rocksdb")]
			Database::Rocksdb => benchmark.run(RocksDBClientProvider::setup().await?).await,
			#[cfg(feature = "surrealkv")]
			Database::Surrealkv => benchmark.run(SurrealKVClientProvider::setup().await?).await,
			#[cfg(feature = "surrealdb")]
			Database::Surrealdb => benchmark.run(SurrealDBClientProvider::default()).await,
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbMemory => benchmark.run(SurrealDBClientProvider::default()).await,
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbRocksdb => benchmark.run(SurrealDBClientProvider::default()).await,
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbSurrealkv => benchmark.run(SurrealDBClientProvider::default()).await,
			#[cfg(feature = "scylladb")]
			Database::Scylladb => benchmark.run(ScyllaDBClientProvider::default()).await,
			#[cfg(feature = "mongodb")]
			Database::Mongodb => benchmark.run(MongoDBClientProvider::default()).await,
			#[cfg(feature = "postgres")]
			Database::Postgres => benchmark.run(PostgresClientProvider::default()).await,
			#[cfg(feature = "redis")]
			Database::Redis => benchmark.run(RedisClientProvider::default()).await,
			#[cfg(feature = "keydb")]
			Database::Keydb => benchmark.run(KeydbClientProvider::default()).await,
		}
	}
}

fn main() -> ExitCode {
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
	let res = runtime.block_on(async { args.database.run(&benchmark).await });
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
				"CPUs: {} - Workers: {} - Clients: {} - Threads: {} - Samples: {}",
				num_cpus::get(),
				args.workers,
				args.clients,
				args.threads,
				args.samples,
			);
			println!("--------------------------------------------------");
			println!("{res}");
			println!("--------------------------------------------------");
			ExitCode::from(0)
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
			ExitCode::from(1)
		}
	}
}
