use crate::benchmark::{Benchmark, BenchmarkEngine, BenchmarkResult};
use crate::docker::{DockerContainer, DockerParams};
use crate::keyprovider::KeyProvider;
use crate::map::MapClientProvider;

use crate::dry::DryClientProvider;
use crate::valueprovider::ValueProvider;
use crate::KeyType;
use anyhow::Result;
use clap::ValueEnum;

#[derive(ValueEnum, Debug, Clone)]
pub(crate) enum Database {
	Dry,
	Map,
	#[cfg(feature = "dragonfly")]
	Dragonfly,
	#[cfg(feature = "keydb")]
	Keydb,
	#[cfg(feature = "mongodb")]
	Mongodb,
	#[cfg(feature = "postgres")]
	Postgres,
	#[cfg(feature = "redb")]
	Redb,
	#[cfg(feature = "redis")]
	Redis,
	#[cfg(feature = "rocksdb")]
	Rocksdb,
	#[cfg(feature = "scylladb")]
	Scylladb,
	#[cfg(feature = "speedb")]
	Speedb,
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
}

impl Database {
	/// Start the Docker container if necessary
	pub(crate) fn start_docker(&self, image: Option<String>) -> Option<DockerContainer> {
		let params: DockerParams = match self {
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbMemory => crate::surrealdb::SURREALDB_MEMORY_DOCKER_PARAMS,
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbRocksdb => crate::surrealdb::SURREALDB_ROCKSDB_DOCKER_PARAMS,
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbSurrealkv => crate::surrealdb::SURREALDB_SURREALKV_DOCKER_PARAMS,
			#[cfg(feature = "scylladb")]
			Self::Scylladb => crate::scylladb::SCYLLADB_DOCKER_PARAMS,
			#[cfg(feature = "mongodb")]
			Self::Mongodb => crate::mongodb::MONGODB_DOCKER_PARAMS,
			#[cfg(feature = "postgres")]
			Self::Postgres => crate::postgres::POSTGRES_DOCKER_PARAMS,
			#[cfg(feature = "dragonfly")]
			Self::Dragonfly => crate::dragonfly::DRAGONFLY_DOCKER_PARAMS,
			#[cfg(feature = "redis")]
			Self::Redis => crate::redis::REDIS_DOCKER_PARAMS,
			#[cfg(feature = "keydb")]
			Self::Keydb => crate::keydb::KEYDB_DOCKER_PARAMS,
			#[allow(unreachable_patterns)]
			_ => return None,
		};
		let image = image.unwrap_or(params.image.to_string());
		let container = DockerContainer::start(image, params.pre_args, params.post_args);
		Some(container)
	}

	/// Run the benchmarks for the chosen database
	pub(crate) async fn run(
		&self,
		benchmark: &Benchmark,
		kt: KeyType,
		kp: KeyProvider,
		vp: ValueProvider,
	) -> Result<BenchmarkResult> {
		match self {
			Database::Dry => {
				benchmark.run(DryClientProvider::setup(kt, vp.columns()).await?, kp, vp).await
			}
			Database::Map => {
				benchmark.run(MapClientProvider::setup(kt, vp.columns()).await?, kp, vp).await
			}
			#[cfg(feature = "dragonfly")]
			Database::Dragonfly => {
				benchmark
					.run(
						crate::dragonfly::DragonflyClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "redb")]
			Database::Redb => {
				benchmark
					.run(crate::redb::ReDBClientProvider::setup(kt, vp.columns()).await?, kp, vp)
					.await
			}
			#[cfg(feature = "speedb")]
			Database::Speedb => {
				benchmark
					.run(
						crate::speedb::SpeeDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "rocksdb")]
			Database::Rocksdb => {
				benchmark
					.run(
						crate::rocksdb::RocksDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "surrealkv")]
			Database::Surrealkv => {
				benchmark
					.run(
						crate::surrealkv::SurrealKVClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::Surrealdb => {
				benchmark
					.run(
						crate::surrealdb::SurrealDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbMemory => {
				benchmark
					.run(
						crate::surrealdb::SurrealDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbRocksdb => {
				benchmark
					.run(
						crate::surrealdb::SurrealDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbSurrealkv => {
				benchmark
					.run(
						crate::surrealdb::SurrealDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "scylladb")]
			Database::Scylladb => {
				benchmark
					.run(
						crate::scylladb::ScyllaDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "mongodb")]
			Database::Mongodb => {
				benchmark
					.run(
						crate::mongodb::MongoDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "postgres")]
			Database::Postgres => {
				benchmark
					.run(
						crate::postgres::PostgresClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
					)
					.await
			}
			#[cfg(feature = "redis")]
			Database::Redis => {
				benchmark
					.run(crate::redis::RedisClientProvider::setup(kt, vp.columns()).await?, kp, vp)
					.await
			}
			#[cfg(feature = "keydb")]
			Database::Keydb => {
				benchmark
					.run(crate::keydb::KeydbClientProvider::setup(kt, vp.columns()).await?, kp, vp)
					.await
			}
		}
	}
}
