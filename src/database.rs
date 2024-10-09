use crate::benchmark::{Benchmark, BenchmarkResult};
use crate::docker::{DockerContainer, DockerParams};
use crate::dry::DryClientProvider;
use crate::keydb::{KeydbClientProvider, KEYDB_DOCKER_PARAMS};
use crate::keyprovider::KeyProvider;
use crate::mongodb::{MongoDBClientProvider, MONGODB_DOCKER_PARAMS};
use crate::postgres::{PostgresClientProvider, POSTGRES_DOCKER_PARAMS};
use crate::redb::ReDBClientProvider;
use crate::redis::{RedisClientProvider, REDIS_DOCKER_PARAMS};
use crate::rocksdb::RocksDBClientProvider;
use crate::scylladb::{ScyllaDBClientProvider, SCYLLADB_DOCKER_PARAMS};
use crate::surrealdb::{
	SurrealDBClientProvider, SURREALDB_MEMORY_DOCKER_PARAMS, SURREALDB_ROCKSDB_DOCKER_PARAMS,
	SURREALDB_SURREALKV_DOCKER_PARAMS,
};
use crate::surrealkv::SurrealKVClientProvider;
use anyhow::Result;
use clap::ValueEnum;

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
	pub(crate) fn start_docker(&self, image: Option<String>) -> Option<DockerContainer> {
		let params: DockerParams = match self {
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbMemory => SURREALDB_MEMORY_DOCKER_PARAMS,
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbRocksdb => SURREALDB_ROCKSDB_DOCKER_PARAMS,
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbSurrealkv => SURREALDB_SURREALKV_DOCKER_PARAMS,
			#[cfg(feature = "scylladb")]
			Self::Scylladb => SCYLLADB_DOCKER_PARAMS,
			#[cfg(feature = "mongodb")]
			Self::Mongodb => MONGODB_DOCKER_PARAMS,
			#[cfg(feature = "postgres")]
			Self::Postgres => POSTGRES_DOCKER_PARAMS,
			#[cfg(feature = "redis")]
			Self::Redis => REDIS_DOCKER_PARAMS,
			#[cfg(feature = "keydb")]
			Self::Keydb => KEYDB_DOCKER_PARAMS,
			#[allow(unreachable_patterns)]
			_ => return None,
		};
		let image = image.unwrap_or(params.image.to_string());
		let container = DockerContainer::start(image, params.pre_args, params.post_args);
		Some(container)
	}

	/// Run the benchmarks for the chosen database
	pub(crate) async fn run<K>(&self, benchmark: &Benchmark, kp: K) -> Result<BenchmarkResult>
	where
		K: KeyProvider,
	{
		match self {
			Database::Dry => benchmark.run(DryClientProvider::default(), kp).await,
			#[cfg(feature = "redb")]
			Database::Redb => benchmark.run(ReDBClientProvider::setup().await?, kp).await,
			#[cfg(feature = "speedb")]
			Database::Speedb => benchmark.run(SpeeDBClientProvider::setup().await?, kp).await,
			#[cfg(feature = "rocksdb")]
			Database::Rocksdb => benchmark.run(RocksDBClientProvider::setup().await?, kp).await,
			#[cfg(feature = "surrealkv")]
			Database::Surrealkv => benchmark.run(SurrealKVClientProvider::setup().await?, kp).await,
			#[cfg(feature = "surrealdb")]
			Database::Surrealdb => benchmark.run(SurrealDBClientProvider::default(), kp).await,
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbMemory => benchmark.run(SurrealDBClientProvider::default(), kp).await,
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbRocksdb => {
				benchmark.run(crate::mongodb::MongoDBClientProvider::default(), kp).await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbSurrealkv => benchmark.run(SurrealDBClientProvider::default(), kp).await,
			#[cfg(feature = "scylladb")]
			Database::Scylladb => benchmark.run(ScyllaDBClientProvider::default(), kp).await,
			#[cfg(feature = "mongodb")]
			Database::Mongodb => benchmark.run(MongoDBClientProvider::default(), kp).await,
			#[cfg(feature = "postgres")]
			Database::Postgres => benchmark.run(PostgresClientProvider::default(), kp).await,
			#[cfg(feature = "redis")]
			Database::Redis => benchmark.run(RedisClientProvider::default(), kp).await,
			#[cfg(feature = "keydb")]
			Database::Keydb => benchmark.run(KeydbClientProvider::default(), kp).await,
		}
	}
}
