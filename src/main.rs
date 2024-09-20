use anyhow::Result;
use clap::{Parser, ValueEnum};
use log::info;

use crate::benchmark::{Benchmark, BenchmarkResult};
use crate::docker::DockerContainer;
use crate::docker::DockerParams;
use crate::dry::DryClientProvider;
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
#[cfg(feature = "surrealdb")]
use crate::surrealdb::SurrealDBClientProvider;
#[cfg(feature = "surrealkv")]
use crate::surrealkv::SurrealKVClientProvider;

mod benchmark;
mod docker;
mod dry;
mod mongodb;
mod postgres;
mod redb;
mod redis;
mod rocksdb;
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
}

#[derive(ValueEnum, Debug, Clone)]
pub(crate) enum Database {
	Dry,
	#[cfg(feature = "redb")]
	Redb,
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
	#[cfg(feature = "mongodb")]
	Mongodb,
	#[cfg(feature = "postgres")]
	Postgres,
	#[cfg(feature = "redis")]
	Redis,
}

impl Database {
	fn start_docker(&self, image: Option<String>) -> Option<DockerContainer> {
		let params: DockerParams = match self {
			Database::Dry => return None,
			#[cfg(feature = "redb")]
			Database::Redb => return None,
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
			#[cfg(feature = "mongodb")]
			Database::Mongodb => mongodb::MONGODB_DOCKER_PARAMS,
			#[cfg(feature = "postgres")]
			Database::Postgres => postgres::POSTGRES_DOCKER_PARAMS,
			#[cfg(feature = "redis")]
			Database::Redis => redis::REDIS_DOCKER_PARAMS,
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
			#[cfg(feature = "mongodb")]
			Database::Mongodb => benchmark.run(MongoDBClientProvider::default()).await,
			#[cfg(feature = "postgres")]
			Database::Postgres => benchmark.run(PostgresClientProvider::default()).await,
			#[cfg(feature = "redis")]
			Database::Redis => benchmark.run(RedisClientProvider::default()).await,
		}
	}
}

#[tokio::main]
async fn main() -> Result<()> {
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

	// Run the benchmark
	let res = args.database.run(&benchmark).await;

	match res {
		// print the results
		Ok(res) => {
			println!(
				"Benchmark result for {:?} on docker {image:?} - Samples: {} - Threads: {}",
				args.database, args.samples, args.threads
			);
			println!("{res}");
			Ok(())
		}
		// print the docker logs if any error occurred
		Err(e) => {
			if let Some(container) = &container {
				container.logs();
			}
			Err(e)
		}
	}
}
