use crate::benchmark::{Benchmark, BenchmarkEngine, BenchmarkResult};
use crate::docker::{DockerContainer, DockerParams};
use crate::keyprovider::KeyProvider;
use crate::map::MapClientProvider;

use crate::dialect::{AnsiSqlDialect, DefaultDialect};
use crate::dry::DryClientProvider;
use crate::valueprovider::ValueProvider;
use crate::{KeyType, Scans};
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
		scans: Scans,
	) -> Result<BenchmarkResult> {
		match self {
			Database::Dry => {
				benchmark
					.run::<_, DefaultDialect, _>(
						DryClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			Database::Map => {
				benchmark
					.run::<_, DefaultDialect, _>(
						MapClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "dragonfly")]
			Database::Dragonfly => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::dragonfly::DragonflyClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "redb")]
			Database::Redb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::redb::ReDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "speedb")]
			Database::Speedb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::speedb::SpeeDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "rocksdb")]
			Database::Rocksdb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::rocksdb::RocksDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealkv")]
			Database::Surrealkv => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::surrealkv::SurrealKVClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::Surrealdb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::surrealdb::SurrealDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbMemory => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::surrealdb::SurrealDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbRocksdb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::surrealdb::SurrealDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbSurrealkv => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::surrealdb::SurrealDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "scylladb")]
			Database::Scylladb => {
				benchmark
					.run::<_, AnsiSqlDialect, _>(
						crate::scylladb::ScyllaDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "mongodb")]
			Database::Mongodb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::mongodb::MongoDBClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "postgres")]
			Database::Postgres => {
				benchmark
					.run::<_, AnsiSqlDialect, _>(
						crate::postgres::PostgresClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "redis")]
			Database::Redis => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::redis::RedisClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "keydb")]
			Database::Keydb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::keydb::KeydbClientProvider::setup(kt, vp.columns()).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
		}
	}
}
