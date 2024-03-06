use anyhow::Result;
use clap::{Parser, ValueEnum};
use log::info;

use crate::benchmark::{Benchmark, BenchmarkResult, DryClientProvider};
use crate::docker::DockerContainer;
use crate::mongodb::{MongoDBClientProvider, MONGODB_DOCKER_PARAMS};
use crate::postgres::{PostgresClientProvider, POSTGRES_DOCKER_PARAMS};
use crate::surrealdb::{
	SurrealDBClientProvider, SURREAL_MEMORY_DOCKER_PARAMS, SURREAL_ROCKSDB_DOCKER_PARAMS,
	SURREAL_SPEEDB_DOCKER_PARAMS,
};

mod benchmark;
mod docker;
mod mongodb;
mod postgres;
mod surrealdb;

#[derive(Parser, Debug)]
#[command(term_width = 0)]
pub(crate) struct Args {
	/// Docker image
	#[arg(short, long)]
	pub(crate) image: Option<String>,

	/// Database
	#[arg(short, long)]
	pub(crate) database: Database,

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
	Surrealdb,
	SurrealdbMemory,
	SurrealdbRocksdb,
	SurrealdbSpeedb,
	Mongodb,
	Postgresql,
}

impl Database {
	fn start_docker(&self, image: Option<String>) -> Option<DockerContainer> {
		let params = match self {
			Database::Dry => return None,
			Database::Surrealdb => return None,
			Database::SurrealdbMemory => SURREAL_MEMORY_DOCKER_PARAMS,
			Database::SurrealdbRocksdb => SURREAL_ROCKSDB_DOCKER_PARAMS,
			Database::SurrealdbSpeedb => SURREAL_SPEEDB_DOCKER_PARAMS,
			Database::Mongodb => MONGODB_DOCKER_PARAMS,
			Database::Postgresql => POSTGRES_DOCKER_PARAMS,
		};
		let image = image.unwrap_or(params.image.to_string());
		let container = DockerContainer::start(image, params.pre_args, params.post_args);
		Some(container)
	}

	fn run(&self, benchmark: &Benchmark) -> Result<BenchmarkResult> {
		match self {
			Database::Dry => benchmark.run(DryClientProvider::default()),
			Database::Surrealdb
			| Database::SurrealdbMemory
			| Database::SurrealdbRocksdb
			| Database::SurrealdbSpeedb => benchmark.run(SurrealDBClientProvider::default()),
			Database::Mongodb => benchmark.run(MongoDBClientProvider::default()),
			Database::Postgresql => benchmark.run(PostgresClientProvider::default()),
		}
	}
}

fn main() -> Result<()> {
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
	let res = args.database.run(&benchmark);

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
