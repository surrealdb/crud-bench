use crate::benchmark::Benchmark;
use crate::dialect::{
	AnsiSqlDialect, ArangoDBDialect, DefaultDialect, MongoDBDialect, MySqlDialect, Neo4jDialect,
	SurrealDBDialect,
};
use crate::docker::{Container, DockerParams};
use crate::dry::DryClientProvider;
use crate::engine::BenchmarkEngine;
use crate::keyprovider::KeyProvider;
use crate::map::MapClientProvider;
use crate::result::BenchmarkResult;
use crate::valueprovider::ValueProvider;
use crate::KeyType;
use anyhow::Result;
use clap::ValueEnum;

#[derive(ValueEnum, Debug, Clone, Copy)]
pub(crate) enum Database {
	Dry,
	Map,
	#[cfg(feature = "arangodb")]
	Arangodb,
	#[cfg(feature = "dragonfly")]
	Dragonfly,
	#[cfg(feature = "echodb")]
	Echodb,
	#[cfg(feature = "fjall")]
	Fjall,
	#[cfg(feature = "keydb")]
	Keydb,
	#[cfg(feature = "lmdb")]
	Lmdb,
	#[cfg(feature = "memodb")]
	Memodb,
	#[cfg(feature = "mongodb")]
	Mongodb,
	#[cfg(feature = "mysql")]
	Mysql,
	#[cfg(feature = "neo4j")]
	Neo4j,
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
	#[cfg(feature = "sqlite")]
	Sqlite,
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
	pub(crate) fn start_docker(&self, options: &Benchmark) -> Option<Container> {
		// Get any pre-defined Docker configuration
		let params: DockerParams = match self {
			#[cfg(feature = "arangodb")]
			Self::Arangodb => crate::arangodb::docker(options),
			#[cfg(feature = "dragonfly")]
			Self::Dragonfly => crate::dragonfly::docker(options),
			#[cfg(feature = "keydb")]
			Self::Keydb => crate::keydb::docker(options),
			#[cfg(feature = "mongodb")]
			Self::Mongodb => crate::mongodb::docker(options),
			#[cfg(feature = "mysql")]
			Self::Mysql => crate::mysql::docker(options),
			#[cfg(feature = "mysql")]
			Self::Neo4j => crate::neo4j::docker(options),
			#[cfg(feature = "postgres")]
			Self::Postgres => crate::postgres::docker(options),
			#[cfg(feature = "redis")]
			Self::Redis => crate::redis::docker(options),
			#[cfg(feature = "scylladb")]
			Self::Scylladb => crate::scylladb::docker(options),
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbMemory => crate::surrealdb::docker(options),
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbRocksdb => crate::surrealdb::docker(options),
			#[cfg(feature = "surrealdb")]
			Self::SurrealdbSurrealkv => crate::surrealdb::docker(options),
			#[allow(unreachable_patterns)]
			_ => return None,
		};
		// Check if a custom image has been specified
		let image = options.image.clone().unwrap_or(params.image.to_string());
		// Start the specified container with arguments
		let container = Container::start(image, params.pre_args, params.post_args, options);
		// Return the container reference
		Some(container)
	}

	/// Run the benchmarks for the chosen database
	pub(crate) async fn run(
		&self,
		benchmark: &Benchmark,
		kt: KeyType,
		kp: KeyProvider,
		vp: ValueProvider,
		scans: &str,
	) -> Result<BenchmarkResult> {
		let scans = serde_json::from_str(scans)?;
		match self {
			Database::Dry => {
				benchmark
					.run::<_, DefaultDialect, _>(
						DryClientProvider::setup(kt, vp.columns(), benchmark).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "arangodb")]
			Database::Arangodb => {
				benchmark
					.run::<_, ArangoDBDialect, _>(
						crate::arangodb::ArangoDBClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
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
						crate::dragonfly::DragonflyClientProvider::setup(
							kt,
							vp.columns(),
							benchmark,
						)
						.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "echodb")]
			Database::Echodb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::echodb::EchoDBClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "fjall")]
			Database::Fjall => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::fjall::FjallClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
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
						crate::keydb::KeydbClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "lmdb")]
			Database::Lmdb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::lmdb::LmDBClientProvider::setup(kt, vp.columns(), benchmark).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			Database::Map => {
				benchmark
					.run::<_, DefaultDialect, _>(
						MapClientProvider::setup(kt, vp.columns(), benchmark).await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "memodb")]
			Database::Memodb => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::memodb::MemoDBClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "mongodb")]
			Database::Mongodb => {
				benchmark
					.run::<_, MongoDBDialect, _>(
						crate::mongodb::MongoDBClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "mysql")]
			Database::Mysql => {
				benchmark
					.run::<_, MySqlDialect, _>(
						crate::mysql::MysqlClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "neo4j")]
			Database::Neo4j => {
				benchmark
					.run::<_, Neo4jDialect, _>(
						crate::neo4j::Neo4jClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
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
						crate::postgres::PostgresClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
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
						crate::redb::ReDBClientProvider::setup(kt, vp.columns(), benchmark).await?,
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
						crate::redis::RedisClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
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
						crate::rocksdb::RocksDBClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
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
						crate::scylladb::ScyllaDBClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "sqlite")]
			Database::Sqlite => {
				benchmark
					.run::<_, AnsiSqlDialect, _>(
						crate::sqlite::SqliteClientProvider::setup(kt, vp.columns(), benchmark)
							.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::Surrealdb => {
				benchmark
					.run::<_, SurrealDBDialect, _>(
						crate::surrealdb::SurrealDBClientProvider::setup(
							kt,
							vp.columns(),
							benchmark,
						)
						.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbMemory => {
				benchmark
					.run::<_, SurrealDBDialect, _>(
						crate::surrealdb::SurrealDBClientProvider::setup(
							kt,
							vp.columns(),
							benchmark,
						)
						.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbRocksdb => {
				benchmark
					.run::<_, SurrealDBDialect, _>(
						crate::surrealdb::SurrealDBClientProvider::setup(
							kt,
							vp.columns(),
							benchmark,
						)
						.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbSurrealkv => {
				benchmark
					.run::<_, SurrealDBDialect, _>(
						crate::surrealdb::SurrealDBClientProvider::setup(
							kt,
							vp.columns(),
							benchmark,
						)
						.await?,
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
						crate::surrealkv::SurrealKVClientProvider::setup(
							kt,
							vp.columns(),
							benchmark,
						)
						.await?,
						kp,
						vp,
						scans,
					)
					.await
			}
		}
	}
}
