//! `crud-bench` entrypoint: CLI arguments, scan/batch configuration types, and the main `run` loop.
//!
//! Datastore implementations live in sibling modules; workload loading uses [`crate::config`].

use crate::benchmark::Benchmark;
use crate::config::load_bench_toml;
use crate::database::Database;
use crate::keyprovider::KeyProvider;
use crate::terminal::ColorChoice;
use crate::valueprovider::ValueProvider;
use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};
use docker::Container;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{IsTerminal, Write};
use tokio::runtime;

// Benchmark modules
mod allocator;
mod benchmark;
mod chart;
mod config;
mod database;
mod dialect;
mod docker;
mod engine;
mod keyprovider;
mod memory;
mod profiling;
mod result;
#[cfg(feature = "surrealdb")]
mod storage;
mod system;
mod terminal;
mod util;
mod value;
mod valueprovider;
mod workloads;

// Datastore modules
mod arangodb;
mod clouddb;
mod dragonfly;
mod dry;
mod fjall;
mod keydb;
mod lmdb;
mod map;
mod mariadb;
mod mdbx;
mod mongodb;
mod mysql;
mod neo4j;
mod postgres;
mod redb;
mod redis;
mod rocksdb;
mod rocksdb_plain;
mod scylladb;
mod slatedb;
mod sqlite;
mod surrealdb;
mod surrealds;
mod surrealkv;
mod surrealmx;

/// Command-line interface for a single benchmark run.
#[derive(Parser, Debug)]
#[command(term_width = 0)]
pub(crate) struct Args {
	/// An optional name for the test, used as a suffix for the JSON result file name
	#[arg(short, long)]
	pub(crate) name: Option<String>,

	/// The database to benchmark
	#[arg(short, long)]
	pub(crate) database: Database,

	/// Specify a custom Docker image
	#[arg(short, long)]
	pub(crate) image: Option<String>,

	/// Whether to run Docker in privileged mode
	#[arg(short, long)]
	pub(crate) privileged: bool,

	/// Specify a custom endpoint to connect to
	#[arg(short, long)]
	pub(crate) endpoint: Option<String>,

	/// Maximum number of blocking threads (default is the number of CPU cores)
	#[arg(short, long, default_value=num_cpus::get().to_string(), value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) blocking: u32,

	/// Number of async runtime workers (default is the number of CPU cores)
	#[arg(short, long, default_value=num_cpus::get().to_string(), value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) workers: u32,

	/// Number of concurrent clients
	#[arg(short, long, default_value = "1", value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) clients: u32,

	/// Number of concurrent threads per client
	#[arg(short, long, default_value = "1", value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) threads: u32,

	/// Number of samples to be created, read, updated, and deleted
	#[arg(short, long, value_parser=clap::value_parser!(u32).range(1..))]
	pub(crate) samples: u32,

	/// Generate the keys in a pseudo-randomized order
	#[arg(short, long)]
	pub(crate) random: bool,

	/// Whether to ensure data is synced and durable
	#[arg(long, default_value = "false")]
	pub(crate) sync: bool,

	/// Per-operation timeout in seconds
	#[arg(long, env = "CRUD_BENCH_OPERATION_TIMEOUT", default_value = "1800", value_parser=clap::value_parser!(u64).range(1..))]
	pub(crate) operation_timeout: u64,

	/// Whether to enable disk persistence for Redis-family databases
	#[arg(long, default_value = "false")]
	pub(crate) persisted: bool,

	/// Use optimised database configurations instead of defaults
	#[arg(long, default_value = "false")]
	pub(crate) optimised: bool,

	/// When to use colour in terminal output (`NO_COLOR` disables colour for `auto` and `always`).
	#[arg(long, value_enum, default_value_t = ColorChoice::Auto)]
	pub(crate) color: ColorChoice,

	/// The type of the key
	#[arg(short, long, default_value_t = KeyType::Integer, value_enum)]
	pub(crate) key: KeyType,

	/// Print-out an example of a generated value
	#[arg(long)]
	pub(crate) show_sample: bool,

	/// Collect system information for a given pid
	#[arg(long, value_parser=clap::value_parser!(u32).range(0..))]
	pub(crate) pid: Option<u32>,

	/// Store benchmark results in SurrealDB
	#[arg(long)]
	pub(crate) store_results: bool,

	/// SurrealDB endpoint for storing results
	#[arg(long, env = "CRUD_BENCH_STORAGE_ENDPOINT", default_value = "ws://localhost:8000")]
	pub(crate) storage_endpoint: String,

	/// Path to the benchmark configuration file
	#[arg(long, env = "CRUD_BENCH_CONFIG", default_value = "config/bench.toml")]
	pub(crate) config: String,

	/// Skip all scan benchmarks
	#[arg(long, default_value = "false")]
	pub(crate) skip_scans: bool,

	/// Skip all batch benchmarks
	#[arg(long, default_value = "false")]
	pub(crate) skip_batches: bool,

	/// Skip index operations, but still table scan queries
	#[arg(long, default_value = "false")]
	pub(crate) skip_indexes: bool,

	/// Emit debug phase markers for log-based tooling
	#[arg(long, default_value_t = false)]
	pub(crate) emit_phase_markers: bool,
}

/// Primary key shape and size for generated record ids.
#[derive(Debug, ValueEnum, Clone, Copy)]
pub(crate) enum KeyType {
	/// 4 bytes integer
	Integer,
	/// 26 ascii bytes
	String26,
	/// 90 ascii bytes
	String90,
	/// 250 ascii bytes
	String250,
	/// 506 ascii bytes
	String506,
	/// UUID type 7
	Uuid,
}

/// Expanded scan benchmarks ready to execute (one [`Scan`] per row after multi-run expansion).
pub(crate) type Scans = Vec<Scan>;

/// Batch throughput cases from the benchmark TOML.
pub(crate) type Batches = Vec<BatchOperation>;

/// One row inside a multi-run scan entry (`runs` on [`ScanSpec`]).
#[derive(Debug, Deserialize)]
pub(crate) struct ScanRun {
	/// Label for this run in results and CLI output.
	name: String,
	/// `ID`, `FULL`, or `COUNT`; overrides the parent [`ScanSpec`] `projection` when set.
	projection: Option<String>,
}

/// Deserialized scan file entry: either a single [`Scan`] (`name`) or several (`runs`), never both.
#[derive(Debug, Deserialize)]
pub(crate) struct ScanSpec {
	/// Stable identifier for grouping, results, and index job names when `with_index` is set.
	id: String,
	/// Display name for a single-run scan; omit when using `runs` instead (mutually exclusive).
	name: Option<String>,
	/// Multiple named projections sharing the same parameters; omit when using `name` instead.
	runs: Option<Vec<ScanRun>>,
	/// Overrides the global sample count for this scan when set.
	samples: Option<usize>,
	/// Per-dialect filter fragments (`WHERE`); omit for unrestricted/table scans.
	condition: Option<Condition>,
	/// Per-dialect `ORDER BY` fragments; omit for unordered scans.
	order_by: Option<OrderByClause>,
	/// Row offset before returning results (`OFFSET` / `SKIP`).
	start: Option<usize>,
	/// Maximum rows to return (`LIMIT`).
	limit: Option<usize>,
	/// Expected row count for validation when set.
	expect: Option<usize>,
	/// `ID`, `FULL`, or `COUNT`; applies to single-run entries or as default for [`ScanRun`] rows.
	projection: Option<String>,
	/// Index definition for indexed-scan legs; omit for heap/table scans only.
	with_index: Option<Index>,
	/// Mixed read/write legs after each scan sample; omitted in config deserializes as empty (read-only).
	#[serde(default)]
	with_writes: Vec<ScanWithWrites>,
}

impl ScanSpec {
	/// Turns one spec entry into one or more [`Scan`] values, validating `name` vs `runs`.
	fn into_scans(self, spec_group: u32) -> Result<Vec<Scan>> {
		let ScanSpec {
			id,
			name,
			runs,
			samples,
			condition,
			order_by,
			start,
			limit,
			expect,
			projection,
			with_index,
			with_writes,
		} = self;

		if id.trim().is_empty() {
			bail!("each scan entry must have a non-empty `id`");
		}

		match (name, runs) {
			(Some(_), Some(_)) => {
				bail!("scan entry must specify either `name` or `runs`, not both");
			}
			(None, None) => {
				bail!("scan entry must include a non-empty `name` or a non-empty `runs` array");
			}
			(None, Some(r)) if r.is_empty() => {
				bail!("scan entry must include a non-empty `name` or a non-empty `runs` array");
			}
			(Some(n), None) => {
				if n.is_empty() {
					bail!("scan `name` must not be empty");
				}
				Ok(vec![Scan {
					id: id.clone(),
					spec_group,
					multi_run_spec: false,
					name: n,
					samples,
					condition,
					order_by,
					start,
					limit,
					expect,
					projection,
					with_index,
					with_writes,
				}])
			}
			(None, Some(runs)) => {
				let multi_run_spec = runs.len() > 1;
				let default_projection = projection.clone();
				let mut out = Vec::with_capacity(runs.len());
				for run in runs {
					if run.name.is_empty() {
						bail!("each entry in `runs` must have a non-empty `name`");
					}
					let run_projection = run.projection.or_else(|| default_projection.clone());
					out.push(Scan {
						id: id.clone(),
						spec_group,
						multi_run_spec,
						name: run.name,
						samples,
						condition: condition.clone(),
						order_by: order_by.clone(),
						start,
						limit,
						expect,
						projection: run_projection,
						with_index: with_index.clone(),
						with_writes: with_writes.clone(),
					});
				}
				Ok(out)
			}
		}
	}
}

/// Expands each [`ScanSpec`] into one or more [`Scan`] rows (multi-run specs produce several rows).
fn expand_scan_specs(specs: Vec<ScanSpec>) -> Result<Scans> {
	let mut scans = Scans::new();
	for (group, spec) in specs.into_iter().enumerate() {
		scans.extend(spec.into_scans(group as u32)?);
	}
	Ok(scans)
}

/// Every scan with a non-skipped `with_index` must supply a non-empty `id` for datastore index names.
fn validate_scan_index_ids(scans: &[Scan]) -> Result<()> {
	for scan in scans {
		if let Some(ref idx) = scan.with_index
			&& !idx.skip
		{
			scan.required_index_id()?;
		}
	}
	Ok(())
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// Physical or logical index attached to a scan (fulltext, field btree, etc.).
pub(crate) struct Index {
	/// When true, skip index create/drop but still run the query leg (table scan).
	#[serde(default)]
	pub(crate) skip: bool,
	/// Columns or paths included in the index.
	pub(crate) fields: Vec<String>,
	/// Whether the index enforces uniqueness when supported by the backend.
	pub(crate) unique: Option<bool>,
	/// Backend-specific hint, e.g. `"fulltext"`.
	pub(crate) index_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// One executable scan benchmark row after expanding [`ScanSpec`] (includes multi-run variants).
pub(crate) struct Scan {
	/// Stable id from the scan spec (grouping, results, index names when indexed).
	pub(crate) id: String,
	/// Which top-level scan JSON object this row came from (CLI grouping only).
	#[serde(skip)]
	pub(crate) spec_group: u32,
	/// True when this row came from a `runs` array with more than one entry (CLI run subheaders).
	#[serde(skip, default)]
	pub(crate) multi_run_spec: bool,
	/// Human-readable title for this scan row (from `name` or a `runs[]` entry).
	name: String,
	/// Sample count for this scan; falls back to CLI `--samples` when unset.
	samples: Option<usize>,
	/// Filter predicates per dialect; omit for full scans.
	condition: Option<Condition>,
	/// Optional `ORDER BY` per datastore (omit for unordered scans).
	#[serde(default)]
	order_by: Option<OrderByClause>,
	/// Row offset (`OFFSET` / `SKIP`) before returning rows.
	start: Option<usize>,
	/// Maximum rows to return (`LIMIT`).
	limit: Option<usize>,
	/// Asserted cardinality when set.
	expect: Option<usize>,
	/// Result shape: `ID`, `FULL`, or `COUNT`.
	projection: Option<String>,
	/// Optional index specification for indexed scan legs (`skip`, `fields`, etc.).
	with_index: Option<Index>,
	/// Read+write workloads (ratio / mode / operation); omit or use `[]` for read-only scans.
	#[serde(default)]
	pub(crate) with_writes: Vec<ScanWithWrites>,
}

/// Mixed read/write scan leg: scan samples plus paired updates that touch indexed columns while
/// keeping approximate match cardinality stable (see `workloads` module).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ScanWithWrites {
	/// Fraction of samples that include compensating writes after the scan (0.0–1.0).
	#[serde(default = "default_writes_ratio")]
	pub(crate) ratio: f64,
	/// How writes are interleaved with scan samples for this leg.
	#[serde(default)]
	pub(crate) mode: ScanWritesMode,
	/// Which datastore operation the write leg performs (currently update-only).
	#[serde(default)]
	pub(crate) operation: ScanWritesOperation,
}

/// Default write ratio when `ratio` is omitted in config (`0.1`).
fn default_writes_ratio() -> f64 {
	0.1
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
/// Scheduling of writes relative to scan iterations for mixed workloads.
pub(crate) enum ScanWritesMode {
	/// Perform compensating writes interleaved with scan samples.
	#[default]
	Interleaved,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// Write operation mixed into an indexed scan workload.
pub(crate) enum ScanWritesOperation {
	/// In-place record updates (same keys as read path).
	#[default]
	Update,
}

impl Scan {
	/// Index / analyzer name for backends that create a physical index (`with_index` without `skip`).
	pub(crate) fn required_index_id(&self) -> Result<&str> {
		let id = self.id.trim();
		if id.is_empty() {
			bail!(
				"scan `{}` has a non-skipped `with_index` but needs a non-empty `id` for the index name",
				self.name
			);
		}
		Ok(id)
	}

	/// Returns the scan projection type for adapter code.
	fn projection(&self) -> Result<Projection> {
		match self.projection.as_deref() {
			Some("ID") => Ok(Projection::Id),
			Some("FULL") => Ok(Projection::Full),
			Some("COUNT") => Ok(Projection::Count),
			Some(o) => bail!(format!("Unsupported projection: {}", o)),
			_ => Ok(Projection::Full),
		}
	}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
/// What columns or aggregates the scan returns.
pub(crate) enum Projection {
	/// Primary key / id only.
	Id,
	/// Full records.
	Full,
	/// `COUNT` aggregate only.
	Count,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// Per-dialect `WHERE` fragments for a filtered scan.
pub(crate) struct Condition {
	/// Generic SQL predicate text (ANSI-ish; used where no dialect override exists).
	sql: Option<String>,
	/// PostgreSQL predicate (`WHERE` fragment only).
	postgres: Option<String>,
	/// SQLite dialect predicate.
	sqlite: Option<String>,
	/// MySQL dialect predicate.
	mysql: Option<String>,
	/// Cypher predicate fragment for Neo4j.
	neo4j: Option<String>,
	/// MongoDB filter document (`serde_json::Value`).
	mongodb: Option<Value>,
	/// ArangoDB AQL predicate fragment.
	arangodb: Option<String>,
	/// SurrealQL predicate fragment.
	surrealdb: Option<String>,
}

/// Per-dialect `ORDER BY` fragments for scans (same shape as [`Condition`]).
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub(crate) struct OrderByClause {
	/// Generic SQL `ORDER BY` expression (minus the keyword).
	pub(crate) sql: Option<String>,
	/// MySQL ordering clause.
	pub(crate) mysql: Option<String>,
	/// Neo4j `ORDER BY` fragment.
	pub(crate) neo4j: Option<String>,
	/// MongoDB sort document.
	pub(crate) mongodb: Option<Value>,
	/// ArangoDB sort expression.
	pub(crate) arangodb: Option<String>,
	/// SurrealQL ordering clause.
	pub(crate) surrealdb: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// One batch throughput case (create/read/update/delete many rows per iteration).
pub(crate) struct BatchOperation {
	/// Display name in results.
	pub(crate) name: String,
	/// Which CRUD operation to batch.
	pub(crate) operation: BatchOperationType,
	/// Records per batch transaction or pipeline.
	pub(crate) batch_size: usize,
	/// Timed iterations for this batch case; backend may default when unset.
	pub(crate) samples: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
/// Batch workload operation kind.
pub(crate) enum BatchOperationType {
	/// Insert many.
	Create,
	/// Read many by key.
	Read,
	/// Update many.
	Update,
	/// Delete many.
	Delete,
}

/// CLI entry: init logging, parse [`Args`], dispatch to [`run`].
fn main() -> Result<()> {
	// Initialise the logger
	env_logger::init();
	// Parse the command line arguments
	let args = Args::parse();
	// Run the benchmark
	run(args)
}

/// Runs the full benchmark lifecycle: Docker (if needed), runtime setup, workload load, datastore run, artifacts.
fn run(args: Args) -> Result<()> {
	// Check if we should profile
	if std::env::var("PROFILE").is_ok() {
		profiling::initialise();
	}
	// Prepare the benchmark
	let mut benchmark = Benchmark::new(&args);
	// Check if we should spawn a Docker container
	let container = if args.database.wants_docker(&args.endpoint) {
		// Start the Docker container
		args.database.start_docker(&benchmark)
	} else {
		// No Docker container needed
		None
	};
	// Setup the asynchronous runtime
	let runtime = runtime::Builder::new_multi_thread()
		.thread_stack_size(2 * 1024 * 1024) // Set stack size to 5MiB
		.max_blocking_threads(args.blocking as usize) // Set the number of blocking threads
		.worker_threads(args.workers as usize) // Set the number of worker threads
		.thread_name("crud-bench-runtime") // Set the name of the runtime threads
		.enable_all() // Enables all runtime features, including I/O and time
		.build()
		.expect("Failed to create a runtime");
	// Setup the blocking thread pool
	let _ = affinitypool::Builder::new()
		.thread_stack_size(5 * 1024 * 1024) // Set stack size to 5MiB
		.worker_threads(args.blocking as usize) // Set the number of worker threads
		.thread_name("crud-bench-threadpool") // Set the name of the threadpool threads
		.thread_per_core(true) // Try to set a thread per core
		.build()
		.build_global();
	// Display formatting
	if std::io::stdout().is_terminal() {
		println!("--------------------------------------------------");
	}
	// Collect system information
	let system = system::collect();
	// Create benchmark metadata
	let metadata = result::BenchmarkMetadata {
		samples: args.samples,
		clients: args.clients,
		threads: args.threads,
		key_type: format!("{:?}", args.key),
		random: args.random,
		sync: args.sync,
		persisted: args.persisted,
		optimised: args.optimised,
	};
	// Get database display name
	let name = args.database.name().to_string();
	// Build the key provider
	let kp = KeyProvider::new(args.key, args.random);
	let bench_toml = load_bench_toml(&args.config)?;
	let value_json = serde_json::to_string(&bench_toml.value)?;
	let vp = ValueProvider::new(&value_json)?;
	let mut batches = bench_toml.batches;
	if args.skip_batches {
		batches.clear();
	}
	let mut scans: Scans = expand_scan_specs(bench_toml.scans)?;
	if args.skip_scans {
		scans.clear();
	} else {
		if args.skip_indexes {
			for scan in &mut scans {
				if let Some(index) = scan.with_index.as_mut() {
					index.skip = true;
				}
			}
		}
		validate_scan_index_ids(&scans)?;
	}
	// Run the benchmark
	let res = runtime.block_on(async {
		args.database
			.run(
				&mut benchmark,
				args.key,
				kp,
				vp,
				scans,
				batches,
				Some(name.clone()),
				Some(system),
				Some(metadata),
			)
			.await
	});
	// Check if we should profile
	if std::env::var("PROFILE").is_ok() {
		profiling::process();
	}
	// Output the results
	match res {
		// Output the results
		Ok(res) => {
			println!("--------------------------------------------------");
			match container.as_ref().map(Container::image) {
				Some(v) => {
					print!("Benchmark result for {:?} on docker {v}", args.database)
				}
				None => match args.endpoint {
					Some(endpoint) => {
						print!("Benchmark result for {:?}; endpoint => {endpoint}", args.database)
					}
					None => print!("Benchmark result for {:?}", args.database),
				},
			}
			println!("{}", args.name.as_ref().map(|s| format!(" - {s}")).unwrap_or_default());
			println!(
				"CPUs: {} - Blocking threads: {} - Workers: {} - Clients: {} - Threads: {} - Samples: {} - Key: {:?} - Random: {}",
				num_cpus::get(),
				args.blocking,
				args.workers,
				args.clients,
				args.threads,
				args.samples,
				args.key,
				args.random,
			);
			println!("--------------------------------------------------");
			println!("{res}");
			println!("--------------------------------------------------");
			if args.show_sample {
				println!("Value sample: {:#}", res.sample.to_json());
				println!("--------------------------------------------------");
			}

			// Serialize the struct to a JSON string
			let json_string = serde_json::to_string_pretty(&res)?;

			// Write the JSON string to a file
			let result_name = args
				.name
				.as_ref()
				.map(|s| format!("result-{s}.json"))
				.unwrap_or_else(|| "result.json".to_string());
			let mut file = File::create(result_name)?;
			file.write_all(json_string.as_bytes())?;

			// Write the CSV file
			let result_csv_name = args
				.name
				.as_ref()
				.map(|s| format!("result-{s}.csv"))
				.unwrap_or_else(|| "result.csv".to_string());
			res.to_csv(&result_csv_name)?;

			// Write the HTML chart file
			let result_html_name = args
				.name
				.as_ref()
				.map(|s| format!("result-{s}.html"))
				.unwrap_or_else(|| "result.html".to_string());
			res.to_html_charts(&result_html_name, &name)?;
			println!("📊 Interactive charts saved to: {}", result_html_name);

			// Store results in SurrealDB if requested
			#[cfg(feature = "surrealdb")]
			if args.store_results {
				match runtime.block_on(async {
					let client = storage::StorageClient::connect(&args.storage_endpoint).await?;
					client.store_result(&res).await
				}) {
					Ok(_) => {
						println!("💾 Results stored in SurrealDB at: {}", args.storage_endpoint)
					}
					Err(e) => eprintln!("⚠️ Failed to store results in SurrealDB: {e}"),
				}
			}
			#[cfg(not(feature = "surrealdb"))]
			if args.store_results {
				eprintln!(
					"⚠️ --store-results requires the `surrealdb` feature to be enabled at build time"
				);
			}

			Ok(())
		}
		// Output the errors
		Err(e) => {
			// Print the error output of the benchmark.
			// `{e:#}` renders the full anyhow chain (joined with ": "), so the
			// `query: <sql>` context attached by backend code (see
			// `log_sql_err` in `surrealdb.rs`) appears alongside the underlying
			// driver / KV-store error text.
			eprintln!("--------------------------------------------------");
			eprintln!("Failure: {e:#}");
			eprintln!("--------------------------------------------------");
			// Print the error output of the container
			if container.is_some() {
				match Container::logs() {
					Ok(stdout) => eprintln!("{stdout}"),
					Err(stderr) => eprintln!("{stderr}"),
				}
			}
			Err(e)
		}
	}
}

#[cfg(test)]
/// Unit and integration-style tests for scan expansion and CLI wiring.
mod test {
	use crate::terminal::ColorChoice;
	use crate::{Args, Database, KeyType, run};
	use anyhow::Result;
	use serial_test::serial;

	fn test(database: Database, key: KeyType, random: bool) -> Result<()> {
		run(Args {
			image: None,
			name: None,
			database,
			privileged: false,
			endpoint: None,
			blocking: 5,
			workers: 5,
			clients: 2,
			threads: 2,
			samples: 10000,
			sync: false,
			operation_timeout: 300,
			persisted: false,
			optimised: false,
			color: ColorChoice::Never,
			random,
			key,
			config: format!("{}/config/test.toml", env!("CARGO_MANIFEST_DIR")),
			show_sample: false,
			pid: None,
			store_results: false,
			storage_endpoint: "ws://localhost:8000".to_string(),
			skip_scans: false,
			skip_batches: false,
			skip_indexes: false,
			emit_phase_markers: false,
		})
	}

	#[test]
	#[serial]
	fn test_integer_ordered() -> Result<()> {
		test(Database::Map, KeyType::Integer, false)
	}

	#[test]
	#[serial]
	fn test_integer_unordered_dry() -> Result<()> {
		test(Database::Dry, KeyType::Integer, true)
	}

	#[test]
	#[serial]
	fn test_integer_unordered_map() -> Result<()> {
		test(Database::Map, KeyType::Integer, true)
	}

	#[test]
	#[serial]
	fn test_string26_ordered() -> Result<()> {
		test(Database::Map, KeyType::String26, false)
	}

	#[test]
	#[serial]
	fn test_string26_unordered() -> Result<()> {
		test(Database::Map, KeyType::String26, true)
	}

	#[test]
	#[serial]
	fn test_string90_ordered() -> Result<()> {
		test(Database::Map, KeyType::String90, false)
	}

	#[test]
	#[serial]
	fn test_string90_unordered() -> Result<()> {
		test(Database::Map, KeyType::String90, true)
	}

	#[test]
	#[serial]
	fn test_string250_ordered() -> Result<()> {
		test(Database::Map, KeyType::String250, false)
	}

	#[test]
	#[serial]
	fn test_string250_unordered() -> Result<()> {
		test(Database::Map, KeyType::String250, true)
	}

	#[test]
	#[serial]
	fn test_string506_ordered() -> Result<()> {
		test(Database::Map, KeyType::String506, false)
	}

	#[test]
	#[serial]
	fn test_string506_unordered_map() -> Result<()> {
		test(Database::Map, KeyType::String506, true)
	}

	#[test]
	#[serial]
	fn test_string506_unordered_dry() -> Result<()> {
		test(Database::Dry, KeyType::String506, true)
	}

	#[test]
	fn scan_spec_name_only() -> Result<()> {
		let specs: Vec<super::ScanSpec> =
			serde_json::from_str(r#"[{"id":"spec_a","name":"a","samples":1,"projection":"ID"}]"#)?;
		let scans = super::expand_scan_specs(specs)?;
		assert_eq!(scans.len(), 1);
		assert_eq!(scans[0].name, "a");
		assert_eq!(scans[0].id, "spec_a");
		assert_eq!(scans[0].spec_group, 0);
		assert!(!scans[0].multi_run_spec);
		Ok(())
	}

	#[test]
	fn scan_spec_runs_expand() -> Result<()> {
		let specs: Vec<super::ScanSpec> = serde_json::from_str(
			r#"[{"id":"spec_runs","runs":[{"name":"x","projection":"FULL"},{"name":"y","projection":"COUNT"}],"samples":2}]"#,
		)?;
		let scans = super::expand_scan_specs(specs)?;
		assert_eq!(scans.len(), 2);
		assert_eq!(scans[0].name, "x");
		assert_eq!(scans[1].name, "y");
		assert_eq!(scans[0].id, "spec_runs");
		assert_eq!(scans[1].id, "spec_runs");
		assert_eq!(scans[0].spec_group, 0);
		assert_eq!(scans[1].spec_group, 0);
		assert!(scans[0].multi_run_spec);
		assert!(scans[1].multi_run_spec);
		Ok(())
	}

	#[test]
	fn scan_spec_groups_increment() -> Result<()> {
		let specs: Vec<super::ScanSpec> = serde_json::from_str(
			r#"[{"id":"ga","name":"a","samples":1,"projection":"ID"},{"id":"gb","name":"b","samples":1,"projection":"ID"}]"#,
		)?;
		let scans = super::expand_scan_specs(specs)?;
		assert_eq!(scans.len(), 2);
		assert_eq!(scans[0].spec_group, 0);
		assert_eq!(scans[1].spec_group, 1);
		assert!(!scans[0].multi_run_spec);
		assert!(!scans[1].multi_run_spec);
		Ok(())
	}

	#[test]
	fn scan_spec_single_run_array_not_multi() -> Result<()> {
		let specs: Vec<super::ScanSpec> = serde_json::from_str(
			r#"[{"id":"one","runs":[{"name":"only","projection":"FULL"}],"samples":1}]"#,
		)?;
		let scans = super::expand_scan_specs(specs)?;
		assert_eq!(scans.len(), 1);
		assert!(!scans[0].multi_run_spec);
		Ok(())
	}

	#[test]
	fn scan_spec_name_and_runs_rejected() {
		let specs: Vec<super::ScanSpec> = serde_json::from_str(
			r#"[{"id":"z","name":"a","runs":[{"name":"b","projection":"FULL"}]}]"#,
		)
		.unwrap();
		assert!(super::expand_scan_specs(specs).is_err());
	}

	#[test]
	fn scan_spec_json_requires_id_field() {
		let err = serde_json::from_str::<Vec<super::ScanSpec>>(
			r#"[{"name":"x","samples":1,"projection":"ID"}]"#,
		);
		assert!(err.is_err());
	}

	#[test]
	fn scan_spec_rejects_whitespace_id() {
		let specs: Vec<super::ScanSpec> =
			serde_json::from_str(r#"[{"id":"   ","name":"x","samples":1,"projection":"ID"}]"#)
				.unwrap();
		assert!(super::expand_scan_specs(specs).is_err());
	}

	#[test]
	fn scan_with_index_ok_when_id_present() {
		let specs: Vec<super::ScanSpec> = serde_json::from_str(
			r#"[{"id":"x","name":"y","samples":1,"with_index":{"fields":["n"]}}]"#,
		)
		.unwrap();
		let scans = super::expand_scan_specs(specs).unwrap();
		assert!(super::validate_scan_index_ids(&scans).is_ok());
	}

	#[test]
	fn scan_spec_with_writes_rejects_single_object() {
		let err = serde_json::from_str::<Vec<super::ScanSpec>>(
			r#"[{"id":"w","name":"n","samples":1,"with_writes":{"ratio":0.2}}]"#,
		);
		assert!(err.is_err());
	}

	#[test]
	fn scan_spec_with_writes_vec() {
		let specs: Vec<super::ScanSpec> = serde_json::from_str(
			r#"[{"id":"w","name":"n","samples":1,"with_writes":[{"ratio":0.1},{"ratio":0.5}]}]"#,
		)
		.unwrap();
		let scans = super::expand_scan_specs(specs).unwrap();
		assert_eq!(scans[0].with_writes.len(), 2);
		assert!((scans[0].with_writes[0].ratio - 0.1).abs() < 1e-9);
		assert!((scans[0].with_writes[1].ratio - 0.5).abs() < 1e-9);
	}
}
