use crate::benchmark::{Benchmark, BenchmarkResult};
use crate::docker::DockerContainer;
use crate::docker::DockerParams;
use crate::dry::DryClientProvider;

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
use log::info;
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

	/// Database
	#[arg(short, long)]
	pub(crate) endpoint: Option<String>,

	/// Number of samples
	#[clap(short, long)]
	pub(crate) samples: i32,

	/// Number of concurrent threads
	#[clap(short, long)]
	pub(crate) threads: usize,

	/// Number of workers for the client async runtime (tokio)
	#[clap(short, long)]
	pub(crate) workers: Option<usize>,
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
	fn start_docker(&self, image: Option<String>) -> Option<DockerContainer> {
		let params: DockerParams = match self {
			Database::Dry => return None,
			#[cfg(feature = "redb")]
			Database::Redb => return None,
			#[cfg(feature = "speedb")]
			Database::Speedb => return None,
			#[cfg(feature = "rocksdb")]
			Database::Rocksdb => return None,
			#[cfg(feature = "surrealkv")]
			Database::Surrealkv => return None,
			#[cfg(feature = "surrealdb")]
			Database::Surrealdb => return None,
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
		};
		let image = image.unwrap_or(params.image.to_string());
		let container = DockerContainer::start(image, params.pre_args, params.post_args);
		Some(container)
	}

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

fn main() {
	// Initialise the logger
	env_logger::init();
	info!("Benchmark started!");

	// Parse the command line arguments
	let args = Args::parse();

	// Prepare the benchmark
	let benchmark = Benchmark::new(&args);

	// Spawn the docker image if any
	let container = args.database.start_docker(args.image);
	let image = container.as_ref().map(|c| c.image().to_string());

	let workers = args.workers.unwrap_or_else(num_cpus::get);
	let runtime = Builder::new_multi_thread()
		.thread_stack_size(10 * 1024 * 1024) // Set stack size to 10MiB
		.worker_threads(workers) // Set the number of worker threads
		.enable_all() // Enables all runtime features, including I/O and time
		.build()
		.expect("Failed to create a runtime");

	// Run the benchmark
	let res = runtime.block_on(async { args.database.run(&benchmark).await });

	match res {
		// print the results
		Ok(res) => {
			println!(
				"Benchmark result for {:?} on docker {image:?} - Samples: {} - Threads: {} - Workers: {} - Cpus: {}",
				args.database, args.samples, args.threads, workers, num_cpus::get()
			);
			println!("{res}");
		}
		// print the docker logs if any error occurred
		Err(e) => {
			if let Some(container) = &container {
				container.logs();
			}
			eprintln!("Failure: {e}");
		}
	}
}
