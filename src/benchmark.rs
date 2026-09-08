//! Orchestrates benchmark phases (CRUD, scans, batches) against a [`crate::engine::BenchmarkEngine`].
//!
//! Spawns concurrent clients/threads, records latency histograms, and aggregates
//! [`crate::result::OperationResult`] values for reporting.

use crate::dialect::Dialect;
use crate::engine::{BenchmarkClient, BenchmarkEngine, KnnKey, ScanContext};
use crate::keyprovider::KeyProvider;
use crate::result::{
	BenchmarkMetadata, BenchmarkResult, OperationMetric, OperationResult, ScanResult, ScanRun,
	ScanWorkload, writes_ratio_percent,
};
use crate::system::SystemInfo;
use crate::terminal::BenchUi;
use crate::util::format_duration;
use crate::valueprovider::ColumnType;
use crate::valueprovider::{ValueProvider, ValueStream};
use crate::vectorgt::{self, GroundTruth, RecallTally, VectorAnswer};
use crate::workloads;
use crate::{
	Args, BatchOperation, Batches, Index, Scan, ScanWithWrites, Scans, VectorHoldout,
	VectorIndexStrategy, VectorQuerySpec,
};

use anyhow::{Context, Result, bail};
use futures::future::try_join_all;
use hdrhistogram::Histogram;
use indicatif::ProgressBar;
use log::{debug, info};
use tokio::task::JoinSet;
use tokio::time::Instant;

use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

/// Maximum wait when polling until the first datastore client connects.
const TIMEOUT: Duration = Duration::from_secs(60);

/// Fixed sleep between phases to let any server-side phase tail settle
/// (open snapshots, draining tasks) before the next phase opens its
/// profiling window. Conservative — short enough to be invisible to a
/// human, long enough to mop up the kind of MVCC drain visible in
/// SurrealDB/RocksDB after heavy concurrent scans.
const QUIESCE_DELAY: Duration = Duration::from_secs(1);

/// Error string returned by adapters to mark an operation as unsupported (skipped, not fatal).
pub(crate) const NOT_SUPPORTED_ERROR: &str = "NotSupported";

/// Query set for a vector-search scan, indexed by sample number with simple
/// modulo wrap-around. Memory cost is constant in `count`, independent of the
/// dataset size.
///
/// Queries are generated from their own seed and never inserted, so they are
/// disjoint from the corpus by construction: no query is its own nearest
/// neighbour, which would otherwise hand every index one free hit in each
/// top-k. Generating them also means no engine read is needed to materialise
/// the set, so an engine that cannot round-trip a vector column no longer
/// changes how the queries are built.
#[derive(Debug, Clone)]
pub(crate) struct VectorQuerySet {
	pub(crate) queries: Arc<Vec<Vec<f32>>>,
	/// Exact answer key for these queries, when one was computed. Shared by
	/// every engine rather than taken from the engine's own exact leg, so the
	/// recall it scores is comparable across engines.
	pub(crate) ground_truth: Option<Arc<GroundTruth>>,
	/// The answer key resolved into this run's key shape, ready to score
	/// against. Built once per scan so an iteration costs a set lookup.
	pub(crate) accept: Option<Arc<Vec<VectorAnswer>>>,
}

impl VectorQuerySet {
	pub(crate) fn pick(&self, sample: u32) -> &[f32] {
		let q = &self.queries[self.query_index(sample)];
		q.as_slice()
	}

	/// Index of the query `pick` returns for `sample`; scoring needs it to line
	/// a result up with its answer.
	pub(crate) fn query_index(&self, sample: u32) -> usize {
		(sample as usize) % self.queries.len()
	}
}

/// Shared benchmark settings and UI, built from CLI [`crate::Args`].
pub(crate) struct Benchmark {
	/// Whether to run containers in privileged mode
	pub(crate) privileged: bool,
	/// The container image to use
	pub(crate) image: Option<String>,
	/// The server endpoint to connect to
	pub(crate) endpoint: Option<String>,
	/// The number of clients to spawn
	pub(crate) clients: u32,
	/// The number of threads to spawn
	pub(crate) threads: u32,
	/// The number of samples to run
	pub(crate) samples: u32,
	/// Pid to monitor
	pub(crate) pid: Option<u32>,
	/// Whether to ensure data is synced
	pub(crate) sync: bool,
	/// Whether to enable disk persistence
	pub(crate) persisted: bool,
	/// Whether to enable optimised configurations
	pub(crate) optimised: bool,
	/// Per-operation timeout
	pub(crate) operation_timeout: Duration,
	/// Directory holding cached vector-search ground truth
	pub(crate) ground_truth_cache: PathBuf,
	/// JSON form of the configured value template. Ground truth keys its cache
	/// on it: a schema change alters the corpus even at an unchanged seed.
	pub(crate) value_template: String,
	/// Terminal UI (tables, progress bars, phase markers).
	pub(crate) bench_ui: BenchUi,
	/// Grep-friendly `… starting` / `Benchmark starting` lines for profiling scripts
	pub(crate) emit_phase_markers: bool,
}

impl Benchmark {
	/// Builds runtime settings from parsed CLI arguments (including env-driven phase markers).
	pub(crate) fn new(args: &Args) -> Self {
		let emit_phase_markers = args.emit_phase_markers
			|| matches!(
				std::env::var("CRUD_BENCH_EMIT_PHASE_MARKERS").as_deref(),
				Ok("1" | "true" | "yes" | "on")
			);
		Self {
			privileged: args.privileged,
			image: args.image.to_owned(),
			endpoint: args.endpoint.to_owned(),
			clients: args.clients,
			threads: args.threads,
			samples: args.samples,
			sync: args.sync,
			pid: args.pid,
			persisted: args.persisted,
			optimised: args.optimised,
			operation_timeout: Duration::from_secs(args.operation_timeout),
			bench_ui: BenchUi::new(args.color),
			emit_phase_markers,
			ground_truth_cache: PathBuf::from(&args.ground_truth_cache),
			value_template: String::new(),
		}
	}

	/// Record the value template this run was configured with, which is part of
	/// the vector ground-truth cache key.
	pub(crate) fn set_value_template(&mut self, template: String) {
		self.value_template = template;
	}

	/// When `COMPACTION` is set in the environment, run the engine-specific
	/// compaction hook and print elapsed time (same style as phase lines).
	async fn maybe_compact_datastore<C, E>(&self, engine: &E) -> Result<()>
	where
		C: BenchmarkClient + Send + Sync,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		if std::env::var("COMPACTION").is_ok() {
			if self.emit_phase_markers {
				self.bench_ui.println_plain("Compaction starting");
			}
			let t = Instant::now();
			self.wait_for_client(engine).await?.compact().await?;
			self.bench_ui.println_took_head("Compaction", &format_duration(t.elapsed()));
			self.quiesce_and_mark().await;
		}
		Ok(())
	}

	/// Block until `index` is queryable as an index, not merely built.
	///
	/// SurrealDB reports `building.status = "ready"` once the initial build
	/// finishes, but rows indexed afterwards sit in a pending queue that
	/// queries answer by scanning it linearly, drained by a background task on
	/// its own interval. A leg timed against an undrained queue measures that
	/// scan wearing the index's name.
	///
	/// Called between timed operations so the wait lands in no measurement, and
	/// before every indexed leg rather than only after the build: a leg that
	/// writes leaves a queue behind, and the next leg would otherwise inherit
	/// it and report a number that depends on what ran before it.
	///
	/// Unrelated to `COMPACTION` / `ALTER SYSTEM COMPACT`, which compacts the
	/// storage keyspace and does nothing for this queue. Engines without such a
	/// queue no-op.
	async fn await_index_queryable<C>(&self, client: &Arc<C>, index: &str) -> Result<()>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let started = Instant::now();
		client.await_index_queryable(index).await?;
		let waited = started.elapsed();
		if waited > Duration::from_millis(200) {
			self.bench_ui.println_muted(&format!(
				"Waited {} for index `{index}` to become queryable",
				format_duration(waited)
			));
		}
		Ok(())
	}

	/// Sleep a fixed beat to let any server-side phase tail settle (open
	/// snapshots, draining tasks, deferred cleanup that outlives the
	/// client's `try_join_all`), then emit the grep-friendly `Server idle`
	/// marker. dev.sh uses that line to disable + rotate the active perf
	/// window so each phase's flamegraph excludes the next phase's startup
	/// work *and* includes its own server-side tail.
	///
	/// Plain sleep — no client probe — so the marker can't silently wedge
	/// the benchmark if a probe query gets stuck.
	async fn quiesce_and_mark(&self) {
		tokio::time::sleep(QUIESCE_DELAY).await;
		if self.emit_phase_markers {
			self.bench_ui.println_plain("Server idle");
		}
	}

	#[allow(clippy::too_many_arguments)]
	/// Run the benchmark for the desired benchmark engine
	pub(crate) async fn run<C, D, E>(
		&self,
		engine: E,
		kp: KeyProvider,
		mut vp: ValueProvider,
		scans: Scans,
		batches: Batches,
		database: Option<String>,
		system: Option<SystemInfo>,
		metadata: Option<BenchmarkMetadata>,
	) -> Result<BenchmarkResult>
	where
		C: BenchmarkClient + Send + Sync,
		D: Dialect,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		// Generate a value sample for the report
		let sample = vp.generate_value();
		// Setup the datastore
		self.bench_ui
			.println_muted(&format!("Setting up the datastore with {} clients", self.clients));
		// Setup the datastore
		self.wait_for_client(&engine).await?.startup().await?;
		// Setup the clients
		let clients = self.setup_clients(&engine).await?;
		// Start the benchmark (optional line for log-based profiling)
		if self.emit_phase_markers {
			self.bench_ui.println_plain("Benchmark starting");
		}
		// Run the "creates" benchmark
		let creates = self
			.run_operation::<C, D>(
				&clients,
				BenchmarkOperation::Create,
				kp,
				vp.clone(),
				self.samples,
			)
			.await?;
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		// Run the "reads" benchmark
		let reads = self
			.run_operation::<C, D>(&clients, BenchmarkOperation::Read, kp, vp.clone(), self.samples)
			.await?;
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		// Run the "reads" benchmark
		let updates = self
			.run_operation::<C, D>(
				&clients,
				BenchmarkOperation::Update,
				kp,
				vp.clone(),
				self.samples,
			)
			.await?;
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		// Run the "scan" benchmarks
		let mut scan_results = Vec::with_capacity(scans.len());
		let mut prev_spec_group: Option<u32> = None;
		let mut prev_run_key: Option<(u32, String)> = None;
		for scan in scans {
			// New section in the TOML/config → new heading in the CLI output
			if prev_spec_group != Some(scan.spec_group) {
				self.bench_ui.section_header(&format!("Scan · {}", scan.id));
				prev_spec_group = Some(scan.spec_group);
			}
			// Multi-run entries (`runs` array): print a sub-line when the run name changes
			let run_key = (scan.spec_group, scan.name.clone());
			if scan.multi_run_spec && prev_run_key.as_ref() != Some(&run_key) {
				self.bench_ui.println_scan_run(&scan.name);
				prev_run_key = Some(run_key);
			} else if !scan.multi_run_spec {
				prev_run_key = Some(run_key);
			}
			let id = scan.id.clone();
			let name = scan.name.clone();
			let iterations = scan.iterations.map(|s| s as u32).unwrap_or(self.samples);
			let write_specs = scan.with_writes.as_slice();
			let w = write_specs.len();
			let index_spec = scan.with_index.as_ref().filter(|i| !i.skip);

			// Vector-search scans take a dedicated path. Order matters:
			//   1. Generate the query set from its own seed. These vectors are
			//      never inserted, so they are disjoint from the corpus and
			//      need no engine read to materialise.
			//   2. Always invoke BuildVectorIndex. Engines decide whether the
			//      chosen strategy needs an actual index (Redis Bruteforce
			//      builds a FLAT FT index; Surreal/Postgres Bruteforce return
			//      NotSupported and the scan still runs without one).
			//   3. Compute or load the exact answer key, but only once the scan
			//      is known to run — an unsupported engine skips before paying
			//      for a corpus sweep.
			//   4. Run the timed VectorScan.
			//   5. RemoveIndex iff Build succeeded — strictly after the scan.
			let result = if let Some(vq) = scan.vector_query.clone() {
				let dim = vp
					.columns()
					.0
					.iter()
					.find_map(|(n, t)| match t {
						ColumnType::FloatVector(d) if n == &vq.field => Some(*d),
						_ => None,
					})
					.ok_or_else(|| {
						anyhow::anyhow!(
							"scan `{}`: vector_query.field `{}` must be a `vector:<dim>` column in the schema",
							name,
							vq.field
						)
					})?;
				let strategy_needs_index = matches!(
					vq.index_strategy,
					VectorIndexStrategy::Hnsw { .. } | VectorIndexStrategy::DiskAnn { .. }
				);
				let mut query_set = self.build_vector_query_set(&scan, &vq, &vp)?;
				let mut runs = Vec::with_capacity(1);
				// Derive the index spec from `vector_query.field` so the user
				// only declares the field once. Engines that don't need an
				// index for the chosen strategy ignore `idx_spec` and return
				// NotSupported from build.
				let idx_spec = Index {
					skip: false,
					fields: vec![vq.field.clone()],
					unique: None,
					index_type: None,
				};
				let vec_index_build = self
					.run_operation::<C, D>(
						&clients[..1],
						BenchmarkOperation::BuildVectorIndex(idx_spec, vq.clone(), dim, id.clone()),
						kp,
						vp.clone(),
						1,
					)
					.await?;
				if vec_index_build.is_some() {
					self.await_index_queryable(&clients[0], &id).await?;
					self.maybe_compact_datastore::<C, E>(&engine).await?;
				}
				// Run the scan if either the strategy doesn't require an index
				// (so a missing build is fine) or build actually produced one.
				// HNSW/DiskANN with no index = skip.
				let ctx = if strategy_needs_index {
					ScanContext::WithIndex
				} else {
					ScanContext::WithoutIndex
				};
				// A search-time knob given as a list becomes one timed leg per
				// value over this *single* index build. A lone `ef_search` is
				// one arbitrary point on a curve; the comparison worth making
				// is the curve, and tracing it should not cost a rebuild per
				// point.
				let sweep = vq.index_strategy.search_values();
				let mut sweep_results: Vec<(Option<u32>, Option<OperationResult>)> = Vec::new();
				if !strategy_needs_index || vec_index_build.is_some() {
					self.attach_ground_truth(&scan, &vq, &vp, &kp, &mut query_set)?;
					// Bruteforce has no search budget, so it runs once with no
					// value to report.
					let legs: Vec<Option<u32>> = if sweep.len() > 1 {
						sweep.iter().map(|v| Some(*v)).collect()
					} else {
						vec![None]
					};
					for leg in legs {
						// Pin the spec to this leg's value so adapters only ever
						// see a resolved strategy.
						let mut leg_scan = scan.clone();
						if let (Some(value), Some(lvq)) = (leg, leg_scan.vector_query.as_mut()) {
							lvq.index_strategy = vq.index_strategy.with_search_value(value);
						}
						let leg_vq = leg_scan
							.vector_query
							.clone()
							.expect("vector scan always carries a vector_query");
						// Engines holding the budget in session state need it on
						// every client, not just the one that built the index.
						for client in clients.iter() {
							client.prepare_vector_search(&leg_vq).await?;
						}
						if let Some(value) = leg {
							self.bench_ui.println_scan_run(&format!(
								"{name} · {} = {value}",
								search_param_label(&vq.index_strategy)
							));
						}
						// Warm the index before timing. The first leg after a
						// build otherwise absorbs the cost of faulting the
						// structure in, which made it read ~1000x slower than
						// the identical leg that followed it — a sweep would
						// have reported its first point as its worst whatever
						// the parameter said.
						for client in clients.iter() {
							for w in 0..VECTOR_WARMUP_QUERIES {
								match client
									.scan_vector(&leg_scan, query_set.pick(w), &kp, ctx)
									.await
								{
									Ok(_) => {}
									// An engine that cannot serve this scan
									// fails the same way in the timed run,
									// which is where it is reported.
									Err(_) => break,
								}
							}
						}
						let result = self
							.run_operation::<C, D>(
								&clients,
								BenchmarkOperation::VectorScan(leg_scan, ctx, query_set.clone()),
								kp,
								vp.clone(),
								iterations,
							)
							.await?;
						sweep_results.push((leg, result));
					}
				} else {
					sweep_results.push((None, None));
				}
				// Drop the index *after* the scan finishes — strictly in this
				// order so the timed scan sees the index.
				let vec_index_remove = if vec_index_build.is_some() {
					self.run_operation::<C, D>(
						&clients[..1],
						BenchmarkOperation::RemoveIndex(id.clone(), name.clone()),
						kp,
						vp.clone(),
						1,
					)
					.await?
				} else {
					None
				};
				let swept = sweep_results.len() > 1;
				for (value, result) in sweep_results {
					runs.push(ScanRun {
						workload: ScanWorkload::Read,
						indexed: strategy_needs_index,
						result,
						label: value
							.map(|v| format!("{} = {v}", search_param_label(&vq.index_strategy))),
					});
				}
				ScanResult {
					id: id.clone(),
					// A swept scan reports several legs under one build, so the
					// value each leg used has to reach the row label.
					name: if swept {
						format!("{name} (sweep)")
					} else {
						name
					},
					iterations,
					index_build: vec_index_build,
					index_remove: vec_index_remove,
					runs,
				}
			} else if let Some(index_spec) = index_spec {
				// Indexed scan: heap legs → build index → indexed legs → drop index
				let mut runs = Vec::with_capacity(2 + 2 * w);
				// Table-scan / heap query (no physical index)
				let without_index = self
					.run_operation::<C, D>(
						&clients,
						BenchmarkOperation::Scan(scan.clone(), ScanContext::WithoutIndex),
						kp,
						vp.clone(),
						iterations,
					)
					.await?;
				runs.push(ScanRun {
					workload: ScanWorkload::Read,
					indexed: false,
					result: without_index,
					label: None,
				});
				// Optional mixed read+write legs on the heap path (one per `with_writes` entry)
				for spec in write_specs {
					let mixed_without_index = self
						.run_operation::<C, D>(
							&clients,
							BenchmarkOperation::ScanWithWrites(
								scan.clone(),
								ScanContext::WithoutIndex,
								spec.clone(),
							),
							kp,
							vp.clone(),
							iterations,
						)
						.await?;
					runs.push(ScanRun {
						workload: ScanWorkload::ReadWrite {
							write_ratio_percent: writes_ratio_percent(spec),
						},
						indexed: false,
						result: mixed_without_index,
						label: None,
					});
				}
				// BuildIndex uses a single client to avoid races on DDL
				let index_build = self
					.run_operation::<C, D>(
						&clients[..1],
						BenchmarkOperation::BuildIndex(
							index_spec.clone(),
							id.clone(),
							name.clone(),
						),
						kp,
						vp.clone(),
						1,
					)
					.await?;
				let (with_index, index_remove, indexed_write_results) = if index_build.is_some() {
					// Compact the datastore so the indexed-scan phases benchmark a compacted index.
					self.maybe_compact_datastore::<C, E>(&engine).await?;
					self.await_index_queryable(&clients[0], &id).await?;
					// Same query shape using the new index
					let with_index = self
						.run_operation::<C, D>(
							&clients,
							BenchmarkOperation::Scan(scan.clone(), ScanContext::WithIndex),
							kp,
							vp.clone(),
							iterations,
						)
						.await?;
					let mut iw = Vec::with_capacity(w);
					for spec in write_specs {
						// The previous leg's writes leave a queue behind; drain
						// it so each leg is timed from the same index state
						// instead of inheriting whatever ran before it.
						self.await_index_queryable(&clients[0], &id).await?;
						iw.push(
							self.run_operation::<C, D>(
								&clients,
								BenchmarkOperation::ScanWithWrites(
									scan.clone(),
									ScanContext::WithIndex,
									spec.clone(),
								),
								kp,
								vp.clone(),
								iterations,
							)
							.await?,
						);
					}
					let index_remove = self
						.run_operation::<C, D>(
							&clients[..1],
							BenchmarkOperation::RemoveIndex(id.clone(), name.clone()),
							kp,
							vp.clone(),
							1,
						)
						.await?;
					(with_index, index_remove, iw)
				} else {
					// BuildIndex unsupported or skipped → no indexed timings to merge
					(None, None, Vec::new())
				};
				if index_build.is_some() {
					runs.push(ScanRun {
						workload: ScanWorkload::Read,
						indexed: true,
						result: with_index,
						label: None,
					});
					for (spec, r) in write_specs.iter().zip(indexed_write_results) {
						runs.push(ScanRun {
							workload: ScanWorkload::ReadWrite {
								write_ratio_percent: writes_ratio_percent(spec),
							},
							indexed: true,
							result: r,
							label: None,
						});
					}
				} else {
					// Still emit indexed rows so CSV/HTML rows align; cells show "-" when result is None
					runs.push(ScanRun {
						workload: ScanWorkload::Read,
						indexed: true,
						result: None,
						label: None,
					});
					for spec in write_specs {
						runs.push(ScanRun {
							workload: ScanWorkload::ReadWrite {
								write_ratio_percent: writes_ratio_percent(spec),
							},
							indexed: true,
							result: None,
							label: None,
						});
					}
				}
				ScanResult {
					id: id.clone(),
					name,
					iterations,
					index_build,
					index_remove,
					runs,
				}
			} else {
				// No index spec (or index skipped): only heap scan + optional write-mix legs
				let mut runs = Vec::with_capacity(1 + w);
				let without_index = self
					.run_operation::<C, D>(
						&clients,
						BenchmarkOperation::Scan(scan.clone(), ScanContext::WithoutIndex),
						kp,
						vp.clone(),
						iterations,
					)
					.await?;
				runs.push(ScanRun {
					workload: ScanWorkload::Read,
					indexed: false,
					result: without_index,
					label: None,
				});
				for spec in write_specs {
					let mixed_without_index = self
						.run_operation::<C, D>(
							&clients,
							BenchmarkOperation::ScanWithWrites(
								scan.clone(),
								ScanContext::WithoutIndex,
								spec.clone(),
							),
							kp,
							vp.clone(),
							iterations,
						)
						.await?;
					runs.push(ScanRun {
						workload: ScanWorkload::ReadWrite {
							write_ratio_percent: writes_ratio_percent(spec),
						},
						indexed: false,
						result: mixed_without_index,
						label: None,
					});
				}
				ScanResult {
					id: id.clone(),
					name,
					iterations,
					index_build: None,
					index_remove: None,
					runs,
				}
			};
			scan_results.push(result);
		}
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		self.bench_ui.section_header("Delete");
		// Run the "deletes" benchmark
		let deletes = self
			.run_operation::<C, D>(
				&clients,
				BenchmarkOperation::Delete,
				kp,
				vp.clone(),
				self.samples,
			)
			.await?;
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		if !batches.is_empty() {
			self.bench_ui.section_header("Batches");
		}
		// Run the "batch" benchmarks
		let mut batch_results = Vec::with_capacity(batches.len());
		for batch in batches {
			// Get the name of the batch operation
			let name = batch.name.clone();
			let groups = batch.batch_size;
			let iterations = batch.iterations.map(|s| s as u32).unwrap_or(self.samples);
			// Determine the batch operation type
			let operation = match batch.operation {
				crate::BatchOperationType::Create => BenchmarkOperation::BatchCreate(batch.clone()),
				crate::BatchOperationType::Read => BenchmarkOperation::BatchRead(batch.clone()),
				crate::BatchOperationType::Update => BenchmarkOperation::BatchUpdate(batch.clone()),
				crate::BatchOperationType::Delete => BenchmarkOperation::BatchDelete(batch.clone()),
			};
			// Execute the batch benchmark
			let duration =
				self.run_operation::<C, D>(&clients, operation, kp, vp.clone(), iterations).await?;
			// Store the batch benchmark result
			batch_results.push((name, iterations, groups, duration));
		}
		// Mark the benchmark as complete
		if self.emit_phase_markers {
			self.bench_ui.println_plain("Benchmark complete");
		}
		// Shut down the datastore
		self.wait_for_client(&engine).await?.shutdown().await?;
		// Return the benchmark results
		Ok(BenchmarkResult {
			database,
			system,
			metadata,
			creates,
			reads,
			updates,
			scans: scan_results,
			batches: batch_results,
			deletes,
			sample,
		})
	}

	/// Build the held-out [`VectorQuerySet`] for a vector-search scan.
	/// Reads N rows (id picked deterministically from `seed`) and extracts the
	/// `field` column. The read cost is paid once here, off the timed window;
	/// the resulting `Vec<f32>` queries are reused across all scan iterations.
	///
	/// Returns `Ok(None)` when the engine cannot surface vector reads (the
	/// holdout extraction hits [`NOT_SUPPORTED_ERROR`]) so the caller can skip
	/// the entire vector scan instead of aborting the benchmark.
	///
	/// Reuses one of the already-connected clients from the benchmark pool
	/// rather than spawning a fresh one — `wait_for_client` carries a
	/// per-engine pre-connect sleep (5s on SurrealDB) that compounds across
	/// the three vector legs.
	fn build_vector_query_set(
		&self,
		scan: &Scan,
		vq: &VectorQuerySpec,
		vp: &ValueProvider,
	) -> Result<VectorQuerySet> {
		let VectorHoldout {
			count,
			seed,
		} = vq.holdout.clone();
		let queries = vp
			.generate_vectors(&vq.field, count, seed)
			.with_context(|| format!("scan `{}`: building the vector query set", scan.name))?;
		// `VectorQuerySet::pick` indexes modulo the query count, so an empty set
		// would panic inside the timed window. The validator already rejects
		// `holdout.count == 0`; this covers anything else that reaches zero.
		if queries.is_empty() {
			bail!("scan `{}`: vector query set is empty", scan.name);
		}
		Ok(VectorQuerySet {
			queries: Arc::new(queries),
			ground_truth: None,
			accept: None,
		})
	}

	/// Attach the exact answer key for a scan's query set, computing it or
	/// loading it from cache.
	///
	/// Deferred until the scan is known to run: an engine without vector
	/// support skips before this point, and there is no reason to spend a full
	/// corpus sweep producing a key nothing will score against.
	fn attach_ground_truth(
		&self,
		scan: &Scan,
		vq: &VectorQuerySpec,
		vp: &ValueProvider,
		kp: &KeyProvider,
		query_set: &mut VectorQuerySet,
	) -> Result<()> {
		let Some(corpus_seed) = vp.seed() else {
			// Recall needs a reconstructible corpus. Without a seed the scan is
			// still perfectly valid as a latency measurement, so say what is
			// missing and carry on rather than failing the run.
			eprintln!(
				"vector ground truth: scan `{}` will report latency only — set `seed` in the benchmark TOML or pass --corpus-seed to enable recall",
				scan.name
			);
			return Ok(());
		};
		let request = vectorgt::Request {
			field: vq.field.clone(),
			samples: self.samples,
			top_k: vq.top_k,
			metric: vq.distance,
			corpus_seed,
			query_seed: vq.holdout.seed,
			query_count: query_set.queries.len(),
			template: self.value_template.clone(),
		};
		let started = Instant::now();
		let (gt, cached) =
			vectorgt::load_or_compute(&request, vp, &query_set.queries, &self.ground_truth_cache)
				.with_context(|| format!("scan `{}`: computing vector ground truth", scan.name))?;
		if !cached {
			self.bench_ui.println_muted(&format!(
				"Computed exact ground truth for {} queries over {} rows in {}",
				query_set.queries.len(),
				self.samples,
				format_duration(started.elapsed())
			));
		}
		query_set.accept =
			Some(Arc::new(vectorgt::build_answers(&gt, kp, vq.top_k, vq.tie_epsilon)));
		query_set.ground_truth = Some(Arc::new(gt));
		Ok(())
	}

	/// Polls until [`BenchmarkEngine::create_client`] succeeds or [`TIMEOUT`] elapses.
	async fn wait_for_client<C, E>(&self, engine: &E) -> Result<C>
	where
		C: BenchmarkClient + Send + Sync,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		// Get the current system time
		let time = SystemTime::now();
		// Get the timeout for the engine
		let wait = engine.wait_timeout();
		// Check the elapsed time
		while time.elapsed()? < TIMEOUT {
			// Wait for a small amount of time
			if let Some(wait) = wait {
				tokio::time::sleep(wait).await
			};
			// Attempt to create a client connection
			match engine.create_client().await {
				Err(e) => debug!("Received error: {e}"),
				Ok(c) => return Ok(c),
			}
		}
		bail!("Can't create the client")
	}

	/// Creates one async connection per logical client; returns shared handles for workers.
	async fn setup_clients<C, E>(&self, engine: &E) -> Result<Vec<Arc<C>>>
	where
		C: BenchmarkClient + Send + Sync,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		// Create a set of client connections
		let mut clients = Vec::with_capacity(self.clients as usize);
		// Create the desired number of connections
		for i in 0..self.clients {
			// Log some information
			info!("Creating client {}", i + 1);
			// Create a new client connection
			clients.push(engine.create_client());
		}
		// Wait for all the clients to connect
		Ok(try_join_all(clients).await?.into_iter().map(Arc::new).collect())
	}

	/// Runs one logical phase across `clients × threads` workers with shared progress and metrics.
	async fn run_operation<C, D>(
		&self,
		clients: &[Arc<C>],
		operation: BenchmarkOperation,
		kp: KeyProvider,
		vp: ValueProvider,
		samples: u32,
	) -> Result<Option<OperationResult>>
	where
		C: BenchmarkClient + Send + Sync,
		D: Dialect,
	{
		// Optional line for log-based profiling (`dev.sh`, grep over captured logs).
		// `phase_marker_label` includes the scan id / run name / ctx so per-scan and
		// per-index DDL windows are uniquely greppable.
		if self.emit_phase_markers {
			self.bench_ui.println_plain(&format!("{} starting", phase_marker_label(&operation)));
		}
		let progress =
			self.bench_ui.progress_bar(samples as u64, &progress_short_label(&operation));
		// Whether we have experienced an error
		let error = Arc::new(AtomicBool::new(false));
		// Wether the test should be skipped
		let skip = Arc::new(AtomicBool::new(false));
		// The total records processed so far
		let current = Arc::new(AtomicU32::new(0));
		// The total records processed so far
		let complete = Arc::new(AtomicU32::new(0));
		// Store the worker tasks in a join set so failures can stop the operation promptly.
		let mut tasks = JoinSet::new();
		// Measure the starting time
		let metric = OperationMetric::new(self.pid, samples);
		// Loop over the clients
		for (client, _) in clients.iter().cloned().zip(1..) {
			// Loop over the threads
			for _ in 0..self.threads {
				let error = error.clone();
				let skip = skip.clone();
				let current = current.clone();
				let complete = complete.clone();
				let client = client.clone();
				let progress = progress.clone();
				let vp = vp.clone();
				let operation = operation.clone();
				let operation_timeout = self.operation_timeout;
				tasks.spawn(async move {
					match Self::operation_loop::<C, D>(
						client,
						samples,
						&error,
						&current,
						&complete,
						operation,
						operation_timeout,
						(kp, vp, progress),
					)
					.await
					{
						Err(e) if e.to_string().eq(NOT_SUPPORTED_ERROR) => {
							skip.store(true, Ordering::Relaxed);
							Ok(None)
						}
						Err(e) => {
							eprintln!("{e}");
							error.store(true, Ordering::Relaxed);
							Err(e)
						}
						Ok(h) => Ok(Some(h)),
					}
				});
			}
		}
		// Wait for the threads to complete, aborting the remaining tasks on the first failure.
		let mut global_histogram = Histogram::new(3)?;
		let mut global_recall = RecallTally::default();
		while let Some(result) = tasks.join_next().await {
			match result {
				Ok(Ok(Some((histogram, recall)))) => {
					global_histogram.add(histogram)?;
					global_recall.merge(recall);
				}
				Ok(Ok(None)) => {}
				Ok(Err(e)) => {
					error.store(true, Ordering::Relaxed);
					tasks.abort_all();
					while tasks.join_next().await.is_some() {}
					if let Some(ref pb) = progress {
						pb.finish_and_clear();
					}
					return Err(e).with_context(|| format!("{operation} worker failed"));
				}
				Err(e) => {
					error.store(true, Ordering::Relaxed);
					tasks.abort_all();
					while tasks.join_next().await.is_some() {}
					if let Some(ref pb) = progress {
						pb.finish_and_clear();
					}
					return Err(e).with_context(|| format!("{operation} task failed"));
				}
			}
		}
		// Finish the progress bar at 100% before tearing it down
		if let Some(ref pb) = progress {
			pb.set_position(samples as u64);
			pb.finish_and_clear();
		}
		if error.load(Ordering::Relaxed) {
			bail!("Task failure");
		}
		// Histogram + sysinfo snapshots → OperationResult; then print phase timing line
		let result =
			OperationResult::new(metric, global_histogram).with_recall(global_recall.summarise());
		let took = result.total_time();
		match &operation {
			BenchmarkOperation::Scan(_, ctx) => {
				self.bench_ui.println_took_scan(scan_context_slug(*ctx), None, &took);
			}
			BenchmarkOperation::VectorScan(_, ctx, _) => {
				self.bench_ui.println_took_scan(scan_context_slug(*ctx), None, &took);
			}
			BenchmarkOperation::ScanWithWrites(_, ctx, spec) => {
				self.bench_ui.println_took_scan(
					scan_context_slug(*ctx),
					Some(writes_ratio_percent(spec)),
					&took,
				);
			}
			_ => {
				// Create/Read/Update/Delete, index DDL, and batch ops share the default line format
				self.bench_ui.println_took_head(&operation.to_string(), &took);
			}
		}
		// Grep-friendly took marker for ops whose UI line collapses multiple
		// runs onto the same label (scans always reuse `Scan :: no-index`/
		// `Scan :: indexed`; BuildIndex/RemoveIndex reuse their bare name).
		// The rich marker disambiguates by scan id so dev.sh can attach one
		// perf window per run.
		if self.emit_phase_markers
			&& matches!(
				&operation,
				BenchmarkOperation::Scan(..)
					| BenchmarkOperation::ScanWithWrites(..)
					| BenchmarkOperation::BuildIndex(..)
					| BenchmarkOperation::RemoveIndex(..)
			) {
			self.bench_ui.println_plain(&format!(
				"{} took {}",
				phase_marker_label(&operation),
				took
			));
		}
		// Shall we skip the operation? (operation not supported)
		if skip.load(Ordering::Relaxed) {
			return Ok(None);
		}
		// Wait for server-side phase tail to drain and emit the
		// `Server idle` marker. Must happen *after* the took line so
		// dev.sh sees took → Server idle → (next phase) starting.
		self.quiesce_and_mark().await;
		// Everything ok
		Ok(Some(result))
	}

	#[allow(clippy::too_many_arguments)]
	/// Per-worker loop: claim sample indices until done; record microsecond latencies in a histogram.
	async fn operation_loop<C, D>(
		client: Arc<C>,
		samples: u32,
		error: &AtomicBool,
		current: &AtomicU32,
		complete: &AtomicU32,
		operation: BenchmarkOperation,
		operation_timeout: Duration,
		(mut kp, mut vp, progress): (KeyProvider, ValueProvider, Option<Arc<ProgressBar>>),
	) -> Result<(Histogram<u64>, RecallTally)>
	where
		C: BenchmarkClient,
		D: Dialect,
	{
		let mut histogram = Histogram::new(3)?;
		let mut tally = RecallTally::default();
		// Check if we have encountered an error
		while !error.load(Ordering::Relaxed) {
			// Get the current sample number
			let sample = current.fetch_add(1, Ordering::Relaxed);
			// Have we produced enough samples
			if sample >= samples {
				// We are done
				break;
			}
			// Perform the benchmark operation under a per-iteration
			// timeout. A stuck `await` inside the underlying SDK
			// (e.g. a WebSocket reply that never lands because the
			// connection was torn down without completing the
			// matching oneshot) returns an error here instead of
			// parking the worker task forever; the operation `JoinSet` then
			// short-circuits with the operation name in the error
			// chain rather than hanging in `block_on`.
			// KNN hits, kept so recall can be scored after the latency is
			// recorded rather than inside the measured window.
			let mut scored: Option<(usize, Vec<KnnKey>)> = None;
			let time = Instant::now();
			tokio::time::timeout(operation_timeout, async {
				match &operation {
					BenchmarkOperation::Create => {
						let value = vp.generate_value_for(ValueStream::Create, sample);
						client.create(sample, value, &mut kp).await
					}
					BenchmarkOperation::Read => client.read(sample, &mut kp).await.map(|_| ()),
					BenchmarkOperation::Update => {
						let value = vp.generate_value_for(ValueStream::Update, sample);
						client.update(sample, value, &mut kp).await
					}
					BenchmarkOperation::Scan(s, ctx) => client.scan(s, &kp, *ctx).await,
					BenchmarkOperation::VectorScan(s, ctx, qs) => {
						let q = qs.pick(sample);
						let hits = client.scan_vector(s, q, &kp, *ctx).await?;
						scored = Some((qs.query_index(sample), hits));
						Ok(())
					}
					BenchmarkOperation::ScanWithWrites(scan, ctx, spec) => {
						workloads::run_scan_with_writes(
							&*client, scan, *ctx, spec, sample, samples, &mut kp,
						)
						.await
					}
					BenchmarkOperation::BuildIndex(spec, id, _) => {
						client.build_index(spec, id.as_str()).await
					}
					BenchmarkOperation::BuildVectorIndex(spec, vq, dim, name) => {
						client.build_vector_index(spec, vq, *dim, name.as_str()).await
					}
					BenchmarkOperation::RemoveIndex(id, _) => client.drop_index(id.as_str()).await,
					BenchmarkOperation::Delete => client.delete(sample, &mut kp).await,
					BenchmarkOperation::BatchCreate(batch_op) => {
						client.batch_create(sample, batch_op, &mut kp, &mut vp).await
					}
					BenchmarkOperation::BatchRead(batch_op) => {
						client.batch_read(sample, batch_op, &mut kp).await
					}
					BenchmarkOperation::BatchUpdate(batch_op) => {
						client.batch_update(sample, batch_op, &mut kp, &mut vp).await
					}
					BenchmarkOperation::BatchDelete(batch_op) => {
						client.batch_delete(sample, batch_op, &mut kp).await
					}
				}
			})
			.await
			.with_context(|| {
				format!("{operation} did not complete within {operation_timeout:?}")
			})??;
			// Get the completed sample number
			let sample = complete.fetch_add(1, Ordering::Relaxed);
			if let Some(pb) = &progress {
				let done = ((sample + 1).min(samples)) as u64;
				pb.set_position(done);
			}
			histogram.record(time.elapsed().as_micros() as u64)?;
			// Scoring happens strictly after the latency is banked, so recall
			// never inflates the number it is reported beside.
			if let Some((query, hits)) = scored
				&& let BenchmarkOperation::VectorScan(_, _, qs) = &operation
				&& let Some(answers) = qs.accept.as_ref()
				&& let Some(answer) = answers.get(query)
				&& let Some(value) = vectorgt::recall(answer, &hits)
			{
				tally.record(value);
			}
		}
		Ok((histogram, tally))
	}
}

/// Untimed queries issued per client before a vector leg is timed.
///
/// Enough to fault in the index structure and warm the caches the engine keeps
/// per connection, without materially adding to a run's wall time.
const VECTOR_WARMUP_QUERIES: u32 = 5;

/// Config name of a strategy's search-time knob, for labelling sweep legs.
fn search_param_label(strategy: &VectorIndexStrategy) -> &'static str {
	match strategy {
		VectorIndexStrategy::Hnsw {
			..
		} => "ef_search",
		VectorIndexStrategy::DiskAnn {
			..
		} => "l_search",
		VectorIndexStrategy::Bruteforce => "search",
	}
}

/// Single logical workload dispatched to [`BenchmarkClient`] (CRUD, scan, index, or batch).
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum BenchmarkOperation {
	/// Insert new keys up to the sample count.
	Create,
	/// Read by key.
	Read,
	/// Update existing keys.
	Update,
	/// Table or indexed query for a [`Scan`] and [`ScanContext`].
	Scan(Scan, ScanContext),
	/// KNN query against a pre-fetched holdout query set; only the call into
	/// the engine is timed (the read used to materialise the query lives in
	/// the holdout setup, not in this window).
	VectorScan(Scan, ScanContext, VectorQuerySet),
	/// Scan plus mixed writes according to [`ScanWithWrites`].
	ScanWithWrites(Scan, ScanContext, ScanWithWrites),
	/// Create backing index for the given analyzer/index id, tagged with the
	/// scan run name so two BuildIndex calls under the same scan id (e.g. the
	/// `count` vs `select` query shapes of the same field group) are
	/// distinguishable in phase markers and per-phase perf files.
	BuildIndex(Index, String, String),
	/// Create a vector index (HNSW / DiskANN) carrying the algorithm-specific knobs.
	BuildVectorIndex(Index, VectorQuerySpec, usize, String),
	/// Drop index by stable scan id, tagged with the scan run name for the
	/// same reason as [`BuildIndex`].
	RemoveIndex(String, String),
	/// Delete by key.
	Delete,
	/// Batch insert configured by [`BatchOperation`].
	BatchCreate(BatchOperation),
	/// Batch read by keys from [`BatchOperation`].
	BatchRead(BatchOperation),
	/// Batch update configured by [`BatchOperation`].
	BatchUpdate(BatchOperation),
	/// Batch delete configured by [`BatchOperation`].
	BatchDelete(BatchOperation),
}

/// Short slug for UI labels: heap scan vs index-backed scan.
fn scan_context_slug(ctx: ScanContext) -> &'static str {
	match ctx {
		ScanContext::WithoutIndex => "no-index",
		ScanContext::WithIndex => "indexed",
	}
}

impl Display for BenchmarkOperation {
	/// Human-readable phase name for logs and progress bars.
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Create => write!(f, "Create"),
			Self::Read => write!(f, "Read"),
			Self::Scan(_, ctx) => {
				write!(f, "Scan :: {}", scan_context_slug(*ctx))
			}
			Self::VectorScan(_, ctx, _) => {
				write!(f, "VectorScan :: {}", scan_context_slug(*ctx))
			}
			Self::BuildVectorIndex(_, _, _, _) => write!(f, "BuildVectorIndex"),
			Self::ScanWithWrites(_, ctx, spec) => {
				write!(
					f,
					"Scan :: {}, combined workload (ratio {}%)",
					scan_context_slug(*ctx),
					writes_ratio_percent(spec)
				)
			}
			Self::BuildIndex(_, _, _) => write!(f, "BuildIndex"),
			Self::RemoveIndex(_, _) => write!(f, "RemoveIndex"),
			Self::Update => write!(f, "Update"),
			Self::Delete => write!(f, "Delete"),
			Self::BatchCreate(b) => write!(f, "BatchCreate::{}", b.name),
			Self::BatchRead(b) => write!(f, "BatchRead::{}", b.name),
			Self::BatchUpdate(b) => write!(f, "BatchUpdate::{}", b.name),
			Self::BatchDelete(b) => write!(f, "BatchDelete::{}", b.name),
		}
	}
}

/// Grep-friendly marker label used in `--emit-phase-markers` lines.
///
/// `Display` collapses every scan onto `Scan :: <ctx>` and every BuildIndex /
/// RemoveIndex onto the bare op name, which is fine for the human-readable UI
/// but means dev.sh's profiling loop can't tell adjacent runs apart. This
/// helper expands the label with the scan id (and run name for plain scans)
/// so each marker line is unique within a benchmark run.
fn phase_marker_label(op: &BenchmarkOperation) -> String {
	match op {
		BenchmarkOperation::Scan(scan, ctx) => {
			format!("Scan :: {} :: {} :: {}", scan.id, scan.name, scan_context_slug(*ctx))
		}
		BenchmarkOperation::ScanWithWrites(scan, ctx, spec) => {
			format!(
				"Scan :: {} :: {} :: {}, writes {}%",
				scan.id,
				scan.name,
				scan_context_slug(*ctx),
				writes_ratio_percent(spec)
			)
		}
		BenchmarkOperation::BuildIndex(_, scan_id, scan_name) => {
			format!("BuildIndex :: {scan_id} :: {scan_name}")
		}
		BenchmarkOperation::RemoveIndex(scan_id, scan_name) => {
			format!("RemoveIndex :: {scan_id} :: {scan_name}")
		}
		_ => op.to_string(),
	}
}

/// Truncated label for the indicatif progress bar (scan/batch variants).
fn progress_short_label(operation: &BenchmarkOperation) -> String {
	const MAX: usize = 72;
	let s = match operation {
		BenchmarkOperation::Scan(_, ctx) => scan_context_slug(*ctx).to_string(),
		BenchmarkOperation::VectorScan(_, ctx, _) => {
			format!("vector knn :: {}", scan_context_slug(*ctx))
		}
		BenchmarkOperation::ScanWithWrites(_, ctx, spec) => {
			format!("{}, writes {}%", scan_context_slug(*ctx), writes_ratio_percent(spec))
		}
		BenchmarkOperation::BuildIndex(_, _, _) => "BuildIndex".to_string(),
		BenchmarkOperation::BuildVectorIndex(_, _, _, _) => "BuildVectorIndex".to_string(),
		BenchmarkOperation::RemoveIndex(_, _) => "RemoveIndex".to_string(),
		_ => operation.to_string(),
	};
	if s.len() > MAX {
		format!("{}…", &s[..MAX.saturating_sub(1)])
	} else {
		s
	}
}
