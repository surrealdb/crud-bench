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
	#[cfg(feature = "fjall")]
	Fjall,
	#[cfg(feature = "keydb")]
	Keydb,
	#[cfg(feature = "mdbx")]
	Mdbx,
	#[cfg(feature = "lmdb")]
	Lmdb,
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
	#[cfg(feature = "surrealdb")]
	Surrealdb,
	#[cfg(feature = "surrealdb")]
	SurrealdbMemory,
	#[cfg(feature = "surrealdb")]
	SurrealdbRocksdb,
	#[cfg(feature = "surrealdb")]
	SurrealdbSurrealkv,
	#[cfg(feature = "surrealkv")]
	Surrealkv,
	#[cfg(feature = "surrealmx")]
	Surrealmx,
	#[cfg(feature = "surrealdb")]
	Surrealds,
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
			#[cfg(feature = "neo4j")]
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
		let container = Container::start(image, &params.pre_args, &params.post_args, options);
		// Return the container reference
		Some(container)
	}

	#[allow(clippy::too_many_arguments)]
	/// Run the benchmarks for the chosen database
	pub(crate) async fn run(
		&self,
		benchmark: &mut Benchmark,
		kt: KeyType,
		kp: KeyProvider,
		vp: ValueProvider,
		scans: &str,
		batches: &str,
		database: Option<String>,
		system: Option<crate::system::SystemInfo>,
		metadata: Option<crate::result::BenchmarkMetadata>,
	) -> Result<BenchmarkResult> {
		let scans = serde_json::from_str(scans)?;
		let batches = serde_json::from_str(batches)?;
		match self {
			Database::Dry => {
				benchmark
					.run::<_, DefaultDialect, _>(
						DryClientProvider::setup(kt, vp.columns(), benchmark).await?,
						kp,
						vp,
						scans,
						batches,
						database,
						system,
						metadata,
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
					)
					.await
			}
			#[cfg(feature = "mdbx")]
			Database::Mdbx => {
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::mdbx::MDBXClientProvider::setup(kt, vp.columns(), benchmark).await?,
						kp,
						vp,
						scans,
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
					)
					.await
			}
			#[cfg(feature = "surrealdb")]
			Database::Surrealds => {
				benchmark
					.run::<_, SurrealDBDialect, _>(
						crate::surrealds::SurrealDBClientsProvider::setup(
							kt,
							vp.columns(),
							benchmark,
						)
						.await?,
						kp,
						vp,
						scans,
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
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
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
					)
					.await
			}
			#[cfg(feature = "surrealmx")]
			Database::Surrealmx => {
				benchmark.persisted = false;
				benchmark
					.run::<_, DefaultDialect, _>(
						crate::surrealmx::SurrealMXClientProvider::setup(
							kt,
							vp.columns(),
							benchmark,
						)
						.await?,
						kp,
						vp,
						scans,
						batches,
						database.clone(),
						system.clone(),
						metadata.clone(),
					)
					.await
			}
		}
	}

	pub fn name(&self) -> &'static str {
		match self {
			Database::Dry => "Dry run",
			Database::Map => "DashMap",
			#[cfg(feature = "redis")]
			Database::Redis => "Redis",
			#[cfg(feature = "keydb")]
			Database::Keydb => "KeyDB",
			#[cfg(feature = "dragonfly")]
			Database::Dragonfly => "Dragonfly",
			#[cfg(feature = "rocksdb")]
			Database::Rocksdb => "RocksDB",
			#[cfg(feature = "lmdb")]
			Database::Lmdb => "LMDB",
			#[cfg(feature = "mdbx")]
			Database::Mdbx => "MDBX",
			#[cfg(feature = "mongodb")]
			Database::Mongodb => "MongoDB",
			#[cfg(feature = "mysql")]
			Database::Mysql => "MySQL",
			#[cfg(feature = "postgres")]
			Database::Postgres => "PostgreSQL",
			#[cfg(feature = "sqlite")]
			Database::Sqlite => "SQLite",
			#[cfg(feature = "neo4j")]
			Database::Neo4j => "Neo4j",
			#[cfg(feature = "arangodb")]
			Database::Arangodb => "ArangoDB",
			#[cfg(feature = "scylladb")]
			Database::Scylladb => "ScyllaDB",
			#[cfg(feature = "fjall")]
			Database::Fjall => "Fjall",
			#[cfg(feature = "redb")]
			Database::Redb => "Redb",
			#[cfg(feature = "surrealkv")]
			Database::Surrealkv => "SurrealKV",
			#[cfg(feature = "surrealmx")]
			Database::Surrealmx => "SurrealMX",
			#[cfg(feature = "surrealdb")]
			Database::Surrealdb => "SurrealDB",
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbMemory => "SurrealDB (Memory)",
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbRocksdb => "SurrealDB (RocksDB)",
			#[cfg(feature = "surrealdb")]
			Database::SurrealdbSurrealkv => "SurrealDB (SurrealKV)",
			#[allow(unreachable_patterns)]
			_ => "Unknown",
		}
	}
}
