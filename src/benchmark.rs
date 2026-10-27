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
use crate::reuse::RunMode;
use crate::system::SystemInfo;
use crate::terminal::BenchUi;
use crate::util::format_duration;
use crate::valueprovider::{ColumnType, Columns};
use crate::valueprovider::{ValueProvider, ValueStream};
use crate::vectorfilter::{FilterField, VectorFilter};
use crate::vectorgt::{self, GroundTruth, RecallTally, VectorAnswer};
use crate::workloads;
use crate::{
	Args, BatchOperation, Batches, Index, Scan, ScanWithWrites, Scans, VectorHoldout,
	VectorIndexStrategy, VectorQuerySpec,
};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use futures::future::try_join_all;
use hdrhistogram::Histogram;
use indicatif::ProgressBar;
use log::{debug, info};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};

use std::collections::VecDeque;
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

/// How long a phase runs: a fixed number of samples, or a time window.
///
/// A fixed count suits phases whose samples are distinct rows (create, delete)
/// and so define the dataset. A window suits repeatable queries, whose cost
/// should not scale with how many rows the dataset holds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Iterations {
	/// Run exactly this many samples.
	Fixed(u32),
	/// Run until the window elapses, bounded by `min` and `max` samples.
	Timed(Window),
}

impl From<u32> for Iterations {
	fn from(samples: u32) -> Self {
		Self::Fixed(samples)
	}
}

/// A measurement window with an untimed warm-up in front of it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Window {
	/// Untimed warm-up run before the window opens.
	pub(crate) warmup: Duration,
	/// Wall-clock length of the measured window.
	pub(crate) measure: Duration,
	/// Samples recorded even if the window has already elapsed.
	pub(crate) min: u32,
	/// Samples after which the window closes even if time remains.
	pub(crate) max: u32,
	/// When non-zero, start at most this many samples and admit more only as
	/// samples complete (see [`Iterations::admits`]). The warm-up runs this way,
	/// so a leg whose queries are slow never has every worker start one at once.
	pub(crate) ramp: u32,
}

/// How far past the warm-up's throughput times the window a leg's sample
/// budget reaches, so a leg that speeds up once warm still fills its window.
const WINDOW_BUDGET_HEADROOM: f64 = 1.5;

impl Window {
	/// The most samples the measured window should start: the warm-up's
	/// throughput over the length of the window, with headroom, and within the
	/// configured minimum and maximum. Without it every worker claims a sample
	/// as the window opens, so a leg of queries that each take longer than the
	/// window runs one per worker (thousands) rather than the few that fit.
	pub(crate) fn budget(&self, warmup_ops: f64) -> u32 {
		if !warmup_ops.is_finite() || warmup_ops <= 0.0 {
			return self.max;
		}
		let fits = (warmup_ops * self.measure.as_secs_f64() * WINDOW_BUDGET_HEADROOM).ceil();
		(fits.min(u32::MAX as f64) as u32).clamp(self.min.max(1), self.max.max(self.min.max(1)))
	}
}

impl Iterations {
	/// Count to report for a scan: the configured number when fixed, else the
	/// most samples any leg actually recorded in its window.
	fn reported(&self, runs: &[ScanRun]) -> u32 {
		match self {
			Self::Fixed(n) => *n,
			Self::Timed(_) => runs
				.iter()
				.filter_map(|r| r.result.as_ref().map(OperationResult::samples))
				.max()
				.unwrap_or(0),
		}
	}

	/// Window length in seconds for a timed run, so a result can say its
	/// iteration count was not configured.
	fn window_secs(&self) -> Option<f64> {
		match self {
			Self::Fixed(_) => None,
			Self::Timed(w) => Some(w.measure.as_secs_f64()),
		}
	}

	/// Whether a worker may start another sample, given how many have been
	/// claimed and how many have completed. Always true unless the window
	/// ramps: then at most `ramp` samples start at first, and each completion
	/// admits two more, so a fast leg reaches every worker within a few round
	/// trips while a slow one keeps only a handful of queries in flight.
	fn admits(&self, claimed: u32, completed: u32) -> bool {
		match self {
			Self::Timed(w) if w.ramp > 0 => {
				claimed < w.ramp.saturating_add(completed.saturating_mul(2))
			}
			_ => true,
		}
	}

	/// Whether a worker that has claimed `claimed` samples must stop, given the
	/// instant the measured window opened. A `Fixed` run ignores the clock.
	fn exhausted(&self, claimed: u32, now: Instant, opened: Instant) -> bool {
		match self {
			Self::Fixed(n) => claimed >= *n,
			Self::Timed(w) => {
				claimed >= w.max
					|| (claimed >= w.min && now.saturating_duration_since(opened) >= w.measure)
			}
		}
	}
}

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
	/// Share of the corpus this leg's filter admitted, measured during the
	/// ground-truth sweep. `None` for an unfiltered leg, and for a filtered one
	/// on a run with no corpus seed, where there is no key to measure it from.
	pub(crate) selectivity: Option<f64>,
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

/// Watchdog tracker for active worker operations.
#[derive(Default)]
struct WorkerWatchdog {
	/// Milliseconds since `base_instant` when the current operation started, or 0 when idle.
	active_start_ms: std::sync::atomic::AtomicU64,
}

impl WorkerWatchdog {
	#[inline]
	fn begin_op(&self, now: Instant, base: Instant) {
		// Reuse the caller's already-captured timestamp instead of taking a
		// second clock read here on every single operation.
		let ms = (now.duration_since(base).as_millis() as u64).max(1);
		self.active_start_ms.store(ms, Ordering::Release);
	}

	#[inline]
	fn end_op(&self) {
		self.active_start_ms.store(0, Ordering::Release);
	}
}
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
	/// Which phases run and whether the dataset is kept or reused
	pub(crate) mode: RunMode,
	/// Docker volume holding the dataset between runs, when the mode keeps one
	pub(crate) volume: Option<String>,
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
	/// Default window for scan legs that set no fixed `iterations`
	pub(crate) window: Window,
	/// Safety cap on warming a vector index before a timed leg
	pub(crate) vector_warmup_budget: Duration,
	/// JSON form of the configured value template. Ground truth keys its cache
	/// on it: a schema change alters the corpus even at an unchanged seed.
	pub(crate) value_template: String,
	/// Columns any vector scan filters on, resolved against the schema.
	///
	/// Engines whose vector index is built over a payload separate from the
	/// primary record — Redis mirrors embeddings into a `vec:{key}` HASH — have
	/// to mirror these columns alongside the embedding, and have to declare
	/// them when the index is created. Both happen before any scan is looked
	/// at, so the set cannot be discovered from the scan that needs it.
	pub(crate) vector_filter_fields: Vec<FilterField>,
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
			mode: args.mode,
			volume: (args.mode != RunMode::All).then(|| {
				let key = args
					.database
					.to_possible_value()
					.map(|v| v.get_name().to_string())
					.unwrap_or_default();
				crate::reuse::volume_name(&key, args.name.as_deref())
			}),
			sync: args.sync,
			pid: args.pid,
			persisted: args.persisted,
			optimised: args.optimised,
			operation_timeout: Duration::from_secs(args.operation_timeout),
			bench_ui: BenchUi::new(args.color),
			emit_phase_markers,
			ground_truth_cache: PathBuf::from(&args.ground_truth_cache),
			vector_warmup_budget: Duration::from_secs(args.vector_warmup_seconds),
			window: Window {
				warmup: Duration::from_secs_f64(args.warmup_time),
				measure: Duration::from_secs_f64(args.measurement_time),
				min: args.min_iterations,
				max: args.max_iterations,
				ramp: 0,
			},
			value_template: String::new(),
			vector_filter_fields: Vec::new(),
		}
	}

	/// How a scan leg is run: a fixed `iterations` when the scan sets one, else
	/// the scan's own window layered over the CLI default.
	fn scan_iterations(&self, scan: &Scan) -> Iterations {
		match scan.iterations {
			Some(n) => Iterations::Fixed(n as u32),
			None => Iterations::Timed(Window {
				warmup: scan.warmup_time.map(Duration::from_secs_f64).unwrap_or(self.window.warmup),
				measure: scan
					.measurement_time
					.map(Duration::from_secs_f64)
					.unwrap_or(self.window.measure),
				..self.window
			}),
		}
	}

	/// Record the filter columns vector scans need indexed, for engines that
	/// have to prepare them ahead of the write phase.
	pub(crate) fn set_vector_filter_fields(&mut self, fields: Vec<FilterField>) {
		self.vector_filter_fields = fields;
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

	/// Issue untimed queries until a freshly built vector index answers at a
	/// steady rate.
	///
	/// The first leg timed after a build otherwise absorbs the whole cost of
	/// warming the index, and not by a small margin: on a 50k-row HNSW the
	/// first leg measured ~295ms per query against ~0.5ms for the identical
	/// query in the leg that followed. Worse, it was *more accurate* — 0.935
	/// recall against 0.853 for the same `ef_search` run later — so the first
	/// leg is not merely slow, it answers differently. Reversing a sweep's order
	/// moved the effect to whichever value ran first, which is what identified
	/// it.
	///
	/// The cost is progressive rather than a single fault, so a fixed handful of
	/// queries does not clear it. Warming instead runs until queries settle:
	/// a run of consecutive ones close to the fastest seen, or a wall-clock cap
	/// so a slow engine cannot stall the benchmark.
	async fn warm_vector_index<C>(
		&self,
		clients: &[Arc<C>],
		scan: &Scan,
		query_set: &VectorQuerySet,
		kp: &KeyProvider,
		ctx: ScanContext,
	) -> Result<()>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let started = Instant::now();
		let ceiling = (self.samples / VECTOR_WARMUP_ROWS_PER_QUERY).max(VECTOR_WARMUP_MIN_QUERIES);
		// Set when a client stopped on a limit rather than on a settled rate.
		let mut truncated = false;
		for client in clients.iter() {
			// Recent window durations, oldest first, capped at the span. The
			// plateau test compares against the far end of this, not the near
			// one.
			let mut recent: VecDeque<Duration> =
				VecDeque::with_capacity(VECTOR_WARMUP_PLATEAU_SPAN);
			let mut q = 0u32;
			let mut plateaued = false;
			while started.elapsed() <= self.vector_warmup_budget && q < ceiling {
				// Time a whole window rather than single queries: while an index
				// is warming its latency is *falling*, and once warm it is flat.
				// Comparing windows detects that transition, where comparing
				// single queries to the fastest so far does not — a uniformly
				// slow cold index looks perfectly steady.
				let window = Instant::now();
				let mut budget_spent = false;
				for _ in 0..VECTOR_WARMUP_WINDOW {
					// Bound each query by what is left of the budget. The loop
					// condition above only runs between windows, so without
					// this a single stalled call ignores
					// `--vector-warmup-seconds` entirely and hangs the run
					// before it ever reaches a timed operation, where
					// `operation_timeout` would have caught it.
					let left = self.vector_warmup_budget.saturating_sub(started.elapsed());
					if left.is_zero() {
						budget_spent = true;
						break;
					}
					match timeout(left, client.scan_vector(scan, query_set.pick(q), kp, ctx)).await
					{
						// An engine that cannot serve this scan fails the same
						// way in the timed run, which is where it belongs in
						// the output.
						Ok(Err(_)) => return Ok(()),
						Err(_) => {
							budget_spent = true;
							break;
						}
						Ok(Ok(_)) => {}
					}
					q += 1;
				}
				if budget_spent {
					break;
				}
				let window = window.elapsed();
				// Stop once a window is no real improvement on the one a whole
				// span back. An index that was already warm trips this as soon
				// as the span fills, so a later sweep leg pays the span and no
				// more; an index still draining a backlog keeps improving
				// across the span and does not trip it.
				if warmup_has_plateaued(&recent, window) {
					plateaued = true;
					break;
				}
				if recent.len() == VECTOR_WARMUP_PLATEAU_SPAN {
					recent.pop_front();
				}
				recent.push_back(window);
			}
			if !plateaued {
				truncated = true;
			}
		}
		let waited = started.elapsed();
		if truncated {
			// A truncated warm-up leaves the index cold, and a cold index is
			// both slower and — because it answers by a different path — more
			// accurate. Reporting that silently is how this went unnoticed for
			// several runs, so say it loudly instead.
			self.bench_ui.println_muted(&format!(
				"Warm-up stopped on a limit after {} while latency was still falling: the next \
				 leg is timed against an index that is not finished settling, and the gap can be \
				 large — a 1M-row HNSW index measured ~600ms per query in this state against \
				 ~5ms once settled. Treat the next leg's latency as a lower bound on quality and \
				 an upper bound on speed rather than a measurement, and raise \
				 --vector-warmup-seconds until this line stops appearing.",
				format_duration(waited)
			));
		} else if waited > Duration::from_millis(500) {
			self.bench_ui
				.println_muted(&format!("Warmed the vector index in {}", format_duration(waited)));
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
	/// The wait that matters is inside the timed build, which runs until the
	/// index is queryable. This untimed re-check runs before every indexed leg:
	/// a leg that writes leaves a queue behind, and the next leg would otherwise
	/// inherit it and report a number that depends on what ran before it. Right
	/// after a build there is normally nothing left, and it returns after one
	/// status check.
	///
	/// Unrelated to `COMPACTION` / `ALTER SYSTEM COMPACT`, which compacts the
	/// storage keyspace and does nothing for this queue. Engines without such a
	/// queue no-op.
	async fn await_index_queryable<C>(&self, client: &Arc<C>, index: &str) -> Result<()>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let started = Instant::now();
		client.await_index_queryable(index, self.operation_timeout).await?;
		let waited = started.elapsed();
		if waited > Duration::from_millis(200) {
			self.bench_ui.println_muted(&format!(
				"Waited {} for index `{index}` to become queryable",
				format_duration(waited)
			));
		}
		Ok(())
	}

	/// Index each distinct filter column of a `filter_index` vector scan.
	///
	/// Untimed: it is schema setup for the legs, not the index under test. The
	/// engine returns once the index serves queries. Returns the `(column,
	/// index name)` pairs built, or `None` when the engine cannot index a
	/// filter column — the scan is then reported as skipped, since measuring it
	/// without the index would repeat the default scan under a misleading name.
	async fn build_filter_indexes<C>(
		&self,
		client: &Arc<C>,
		scan_id: &str,
		vq: &VectorQuerySpec,
		columns: &Columns,
	) -> Result<Option<Vec<(String, String)>>>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let mut built: Vec<(String, String)> = Vec::new();
		for filter in &vq.filters {
			if built.iter().any(|(column, _)| column == &filter.field) {
				continue;
			}
			let Some(ty) = filter.column_type(columns) else {
				// `validate` accepts only the types `column_type` maps.
				bail!("vector filter `{}` has no usable column type", filter.name);
			};
			let name = filter_index_name(scan_id, &filter.field);
			let started = Instant::now();
			match client.build_filter_index(&filter.field, ty, &name).await {
				Ok(()) => {
					self.bench_ui.println_muted(&format!(
						"  filter index on `{}` built in {}",
						filter.field,
						format_duration(started.elapsed())
					));
					built.push((filter.field.clone(), name));
				}
				Err(e) if e.to_string() == NOT_SUPPORTED_ERROR => {
					self.drop_filter_indexes(client, &built).await?;
					self.bench_ui.println_muted(
						"  this engine cannot index a filter column, so the scan is skipped",
					);
					return Ok(None);
				}
				Err(e) => return Err(e),
			}
		}
		Ok(Some(built))
	}

	/// Drop what [`Self::build_filter_indexes`] built.
	async fn drop_filter_indexes<C>(
		&self,
		client: &Arc<C>,
		built: &[(String, String)],
	) -> Result<()>
	where
		C: BenchmarkClient + Send + Sync,
	{
		for (column, name) in built {
			client.drop_filter_index(column, name).await?;
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
		// A query run opens a dataset a load run left behind, so it neither
		// recreates the schema nor writes any row.
		if self.mode.loads() {
			self.wait_for_client(&engine).await?.startup().await?;
		} else {
			// Still wait for the datastore to accept connections
			self.wait_for_client(&engine).await?;
		}
		// Setup the clients
		let clients = self.setup_clients(&engine).await?;
		// Start the benchmark (optional line for log-based profiling)
		if self.emit_phase_markers {
			self.bench_ui.println_plain("Benchmark starting");
		}
		let mode = self.mode;
		// Run the "creates" benchmark
		let creates = if mode.loads() {
			let creates = self
				.run_operation::<C, D>(
					&clients,
					BenchmarkOperation::Create,
					kp,
					vp.clone(),
					self.samples,
					self.threads,
				)
				.await?;
			// Compact the datastore
			self.maybe_compact_datastore::<C, E>(&engine).await?;
			creates
		} else {
			None
		};
		// Run the "reads" benchmark
		let reads = if mode.queries() {
			let reads = self
				.run_operation::<C, D>(
					&clients,
					BenchmarkOperation::Read,
					kp,
					vp.clone(),
					self.samples,
					self.threads,
				)
				.await?;
			// Compact the datastore
			self.maybe_compact_datastore::<C, E>(&engine).await?;
			reads
		} else {
			None
		};
		// Run the "updates" benchmark
		let updates = if mode.loads() {
			let updates = self
				.run_operation::<C, D>(
					&clients,
					BenchmarkOperation::Update,
					kp,
					vp.clone(),
					self.samples,
					self.threads,
				)
				.await?;
			// Compact the datastore
			self.maybe_compact_datastore::<C, E>(&engine).await?;
			updates
		} else {
			None
		};
		// A load run stops once the dataset is complete and compacted
		let scans: Scans = if mode.queries() {
			scans
		} else {
			Vec::new()
		};
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
			let iterations = self.scan_iterations(&scan);
			let write_specs = scan.with_writes.as_slice();
			let w = write_specs.len();
			// A scan may run at its own concurrency. A query answering in
			// well under a millisecond spends its time queueing rather than
			// searching at the default settings — a KNN leg measured 0.4ms at
			// one client and roughly 1500ms at sixty-four, which is the queue,
			// not the index — so its latency only means anything lower down.
			// Index DDL is unaffected; it always runs on a single client.
			let leg_clients = match scan.clients {
				Some(requested) => {
					let capped = (requested as usize).clamp(1, clients.len());
					if capped < requested as usize {
						self.bench_ui.println_muted(&format!(
							"Scan `{}` asked for {requested} clients; the pool holds {capped}",
							scan.id
						));
					}
					&clients[..capped]
				}
				None => &clients[..],
			};
			let leg_threads = scan.threads.unwrap_or(self.threads);
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
				let strategy_needs_index = vq.index_strategy.requires_index();
				let query_set = self.build_vector_query_set(&scan, &vq, &vp)?;
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
						leg_threads,
					)
					.await?;
				if vec_index_build.is_some() {
					// The timed build already waited until the index was
					// queryable; this re-check is untimed and normally instant.
					self.await_index_queryable(&clients[0], &id).await?;
					self.maybe_compact_datastore::<C, E>(&engine).await?;
				}
				// Filter-column indexes, when asked for, go in after the vector
				// index and before any leg, so every leg — the unfiltered
				// baseline included — runs against the same schema. Not built
				// when the legs will not run.
				let filter_indexes = if vq.filter_index
					&& (!vq.index_strategy.requires_index() || vec_index_build.is_some())
				{
					self.build_filter_indexes(&clients[0], &id, &vq, &vp.columns()).await?
				} else {
					Some(Vec::new())
				};
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
				// A filtered scan runs one leg per predicate, plus an unfiltered
				// one first. The baseline belongs under the *same* index build
				// and the same warm index as the filtered legs: "what does
				// filtering cost?" is the question, and answering it against a
				// baseline measured on some other build answers a different one.
				let filter_legs: Vec<Option<VectorFilter>> = if vq.filters.is_empty() {
					vec![None]
				} else {
					std::iter::once(None).chain(vq.filters.iter().cloned().map(Some)).collect()
				};
				let mut sweep_results: Vec<VectorLeg> = Vec::new();
				if !strategy_needs_index || vec_index_build.is_some() {
					// Bruteforce has no search budget, so it runs once with no
					// value to report.
					let search_legs: Vec<Option<u32>> = if sweep.len() > 1 {
						sweep.iter().map(|v| Some(*v)).collect()
					} else {
						vec![None]
					};
					// Set once a leg comes back skipped. An engine that cannot
					// serve this scan shape will not start being able to at the
					// next predicate, and every answer key costs a full corpus
					// sweep — so the remaining filters are recorded as skipped
					// rather than each buying a key nothing will score against.
					// An engine that could not build the filter indexes this scan
					// asked for skips every leg, the baseline included.
					let mut unsupported = filter_indexes.is_none();
					// Filters outermost: each has its own answer key, computed
					// once and reused across that filter's search values.
					for filter in &filter_legs {
						if unsupported {
							for leg in &search_legs {
								sweep_results.push(VectorLeg {
									label: leg_heading(&vq, *leg, filter.as_ref(), None),
									selectivity: None,
									result: None,
								});
							}
							continue;
						}
						let mut leg_query_set = query_set.clone();
						self.attach_ground_truth(
							&scan,
							&vq,
							&vp,
							&kp,
							&mut leg_query_set,
							filter.as_ref(),
						)?;
						let selectivity = leg_query_set.selectivity;
						for leg in &search_legs {
							// Pin the spec to this leg's value and predicate so
							// adapters only ever see a resolved strategy.
							let mut leg_scan = scan.clone();
							if let Some(lvq) = leg_scan.vector_query.as_mut() {
								*lvq = lvq.with_filter(filter.as_ref());
								if let Some(value) = leg {
									lvq.index_strategy =
										vq.index_strategy.with_search_value(*value);
								}
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
							let heading = leg_heading(&vq, *leg, filter.as_ref(), selectivity);
							if let Some(heading) = &heading {
								self.bench_ui.println_scan_run(&format!("{name} · {heading}"));
							}
							// Only an index needs priming. A bruteforce leg has none,
							// and every warm-up query there is a full linear scan —
							// which is both pointless and the most expensive query
							// the benchmark can issue.
							if strategy_needs_index {
								self.warm_vector_index(
									leg_clients,
									&leg_scan,
									&leg_query_set,
									&kp,
									ctx,
								)
								.await?;
							}
							let result = self
								.run_operation::<C, D>(
									leg_clients,
									BenchmarkOperation::VectorScan(
										leg_scan,
										ctx,
										leg_query_set.clone(),
									),
									kp,
									vp.clone(),
									iterations,
									leg_threads,
								)
								.await?;
							unsupported |= result.is_none();
							sweep_results.push(VectorLeg {
								label: heading,
								selectivity,
								result,
							});
						}
					}
				} else {
					sweep_results.push(VectorLeg {
						label: None,
						selectivity: None,
						result: None,
					});
				}
				if let Some(built) = &filter_indexes {
					self.drop_filter_indexes(&clients[0], built).await?;
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
						leg_threads,
					)
					.await?
				} else {
					None
				};
				// What varied has to be legible from the scan name: `(sweep)`
				// has always meant the search parameter, and a filtered scan
				// that borrowed the word would read as a budget sweep that
				// never happened.
				let varied = match (sweep.len() > 1, !vq.filters.is_empty()) {
					(true, true) => Some("sweep × filtered"),
					(true, false) => Some("sweep"),
					(false, true) => Some("filtered"),
					(false, false) => None,
				};
				for leg in sweep_results {
					runs.push(ScanRun {
						workload: ScanWorkload::Read,
						indexed: strategy_needs_index,
						result: leg.result.map(|r| r.with_filter_selectivity(leg.selectivity)),
						label: leg.label,
					});
				}
				ScanResult {
					id: id.clone(),
					// A scan with several legs under one build has to say so, and
					// say which axis they differ along — the per-leg label
					// carries the value, this carries the shape.
					name: match varied {
						Some(kind) => format!("{name} ({kind})"),
						None => name,
					},
					iterations: iterations.reported(&runs),
					measurement_secs: iterations.window_secs(),
					index_build: vec_index_build,
					index_remove: vec_index_remove,
					runs,
					vector_query: Some(vq.clone()),
					clients: leg_clients.len() as u32,
					threads: leg_threads,
				}
			} else if let Some(index_spec) = index_spec {
				// Indexed scan: heap legs → build index → indexed legs → drop index
				let mut runs = Vec::with_capacity(2 + 2 * w);
				// Table-scan / heap query (no physical index)
				let without_index = self
					.run_operation::<C, D>(
						leg_clients,
						BenchmarkOperation::Scan(scan.clone(), ScanContext::WithoutIndex),
						kp,
						vp.clone(),
						iterations,
						leg_threads,
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
							leg_clients,
							BenchmarkOperation::ScanWithWrites(
								scan.clone(),
								ScanContext::WithoutIndex,
								spec.clone(),
							),
							kp,
							vp.clone(),
							iterations,
							leg_threads,
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
						leg_threads,
					)
					.await?;
				let (with_index, index_remove, indexed_write_results) = if index_build.is_some() {
					// Compact the datastore so the indexed-scan phases benchmark a compacted index.
					self.maybe_compact_datastore::<C, E>(&engine).await?;
					// The timed build already waited until the index was
					// queryable; this is the untimed re-check every indexed leg
					// gets, and here it normally returns after one status check.
					self.await_index_queryable(&clients[0], &id).await?;
					// Same query shape using the new index
					let with_index = self
						.run_operation::<C, D>(
							leg_clients,
							BenchmarkOperation::Scan(scan.clone(), ScanContext::WithIndex),
							kp,
							vp.clone(),
							iterations,
							leg_threads,
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
								leg_clients,
								BenchmarkOperation::ScanWithWrites(
									scan.clone(),
									ScanContext::WithIndex,
									spec.clone(),
								),
								kp,
								vp.clone(),
								iterations,
								leg_threads,
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
							leg_threads,
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
					iterations: iterations.reported(&runs),
					measurement_secs: iterations.window_secs(),
					index_build,
					index_remove,
					runs,
					vector_query: None,
					clients: leg_clients.len() as u32,
					threads: leg_threads,
				}
			} else {
				// No index spec (or index skipped): only heap scan + optional write-mix legs
				let mut runs = Vec::with_capacity(1 + w);
				let without_index = self
					.run_operation::<C, D>(
						leg_clients,
						BenchmarkOperation::Scan(scan.clone(), ScanContext::WithoutIndex),
						kp,
						vp.clone(),
						iterations,
						leg_threads,
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
							leg_clients,
							BenchmarkOperation::ScanWithWrites(
								scan.clone(),
								ScanContext::WithoutIndex,
								spec.clone(),
							),
							kp,
							vp.clone(),
							iterations,
							leg_threads,
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
					iterations: iterations.reported(&runs),
					measurement_secs: iterations.window_secs(),
					index_build: None,
					index_remove: None,
					runs,
					vector_query: None,
					clients: leg_clients.len() as u32,
					threads: leg_threads,
				}
			};
			scan_results.push(result);
		}
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		// Deletes and batches consume or mutate the dataset, so a split run
		// leaves them out and keeps the data intact for the runs that follow.
		let deletes = if mode.consumes() {
			self.bench_ui.section_header("Delete");
			// Run the "deletes" benchmark
			let deletes = self
				.run_operation::<C, D>(
					&clients,
					BenchmarkOperation::Delete,
					kp,
					vp.clone(),
					self.samples,
					self.threads,
				)
				.await?;
			// Compact the datastore
			self.maybe_compact_datastore::<C, E>(&engine).await?;
			deletes
		} else {
			None
		};
		let batches: Batches = if mode.consumes() {
			batches
		} else {
			Vec::new()
		};
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
			let duration = self
				.run_operation::<C, D>(
					&clients,
					operation,
					kp,
					vp.clone(),
					iterations,
					self.threads,
				)
				.await?;
			// Store the batch benchmark result
			batch_results.push((name, iterations, groups, duration));
		}
		// Mark the benchmark as complete
		if self.emit_phase_markers {
			self.bench_ui.println_plain("Benchmark complete");
		}
		// Shut down the datastore
		self.wait_for_client(&engine).await?.shutdown(!mode.wipes_on_shutdown()).await?;
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
			selectivity: None,
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
		filter: Option<&VectorFilter>,
	) -> Result<()> {
		let Some(corpus_seed) = vp.seed() else {
			// Recall needs a reconstructible corpus. Without a seed the scan is
			// still perfectly valid as a latency measurement, so say what is
			// missing and carry on rather than failing the run.
			//
			// A filtered leg loses its selectivity for the same reason: the
			// share of rows a predicate admits is a fact about a specific
			// corpus, and without a seed there is no specific corpus to measure.
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
			filter: filter.cloned(),
		};
		let started = Instant::now();
		let (gt, cached) =
			vectorgt::load_or_compute(&request, vp, &query_set.queries, &self.ground_truth_cache)
				.with_context(|| format!("scan `{}`: computing vector ground truth", scan.name))?;
		if !cached {
			self.bench_ui.println_muted(&format!(
				"Computed exact ground truth for {} queries over {} rows{} in {}",
				query_set.queries.len(),
				self.samples,
				match filter {
					Some(f) => format!(" matching filter `{}`", f.name),
					None => String::new(),
				},
				format_duration(started.elapsed())
			));
		}
		query_set.accept =
			Some(Arc::new(vectorgt::build_answers(&gt, kp, vq.top_k, vq.tie_epsilon)?));
		// Only a filtered leg has a selectivity worth reporting; an unfiltered
		// one is 100% by construction and the column would say nothing.
		query_set.selectivity = filter.and_then(|_| gt.selectivity());
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
	///
	/// A timed phase first runs its untimed warm-up, then opens the measured
	/// window; only the window is reported.
	async fn run_operation<C, D>(
		&self,
		clients: &[Arc<C>],
		operation: BenchmarkOperation,
		kp: KeyProvider,
		vp: ValueProvider,
		iterations: impl Into<Iterations>,
		threads: u32,
	) -> Result<Option<OperationResult>>
	where
		C: BenchmarkClient + Send + Sync,
		D: Dialect,
	{
		let mut iterations = iterations.into();
		if let Iterations::Timed(window) = iterations
			&& !window.warmup.is_zero()
		{
			// The warm-up ramps up from `min` queries in flight, so a slow leg
			// does not start one query per worker just to warm up.
			let warmup = Iterations::Timed(Window {
				measure: window.warmup,
				min: 1,
				ramp: window.min.max(1),
				..window
			});
			let warmed = self
				.run_phase::<C, D>(
					clients,
					operation.clone(),
					kp,
					vp.clone(),
					warmup,
					threads,
					true,
				)
				.await?;
			let Some(warmed) = warmed else {
				return Ok(None);
			};
			// The measured window starts no more samples than the warm-up's
			// throughput says it can finish.
			iterations = Iterations::Timed(Window {
				max: window.budget(warmed.ops()),
				..window
			});
		}
		self.run_phase::<C, D>(clients, operation, kp, vp, iterations, threads, false).await
	}

	/// One pass over the workers. `quiet` suppresses all output and the
	/// post-phase quiesce, for warm-up passes whose result is discarded.
	#[allow(clippy::too_many_arguments)]
	async fn run_phase<C, D>(
		&self,
		clients: &[Arc<C>],
		operation: BenchmarkOperation,
		kp: KeyProvider,
		vp: ValueProvider,
		iterations: Iterations,
		threads: u32,
		quiet: bool,
	) -> Result<Option<OperationResult>>
	where
		C: BenchmarkClient + Send + Sync,
		D: Dialect,
	{
		let rows = self.samples;
		// Optional line for log-based profiling (`dev.sh`, grep over captured logs).
		// `phase_marker_label` includes the scan id / run name / ctx so per-scan and
		// per-index DDL windows are uniquely greppable.
		if self.emit_phase_markers && !quiet {
			self.bench_ui.println_plain(&format!("{} starting", phase_marker_label(&operation)));
		}
		let progress = if quiet {
			None
		} else {
			match iterations {
				Iterations::Fixed(n) => {
					self.bench_ui.progress_bar(n as u64, &progress_short_label(&operation))
				}
				// A window has no sample total to count towards, so the bar
				// tracks elapsed milliseconds against the window instead.
				Iterations::Timed(w) => {
					let bar = self.bench_ui.progress_bar(
						w.measure.as_millis() as u64,
						&progress_short_label(&operation),
					);
					if let Some(bar) = &bar {
						let bar = bar.clone();
						let opened = Instant::now();
						tokio::spawn(async move {
							while !bar.is_finished() {
								bar.set_position(opened.elapsed().as_millis() as u64);
								tokio::time::sleep(Duration::from_millis(200)).await;
							}
						});
					}
					bar
				}
			}
		};
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
		let base_instant = Instant::now();
		let watchdog_notify = Arc::new(tokio::sync::Notify::new());
		let timeout_ms = self.operation_timeout.as_millis() as u64;
		let operation_timeout = self.operation_timeout;
		let mut watchdogs = Vec::new();
		// Measure the starting time
		let metric = OperationMetric::new(self.pid);
		// Loop over the clients
		for (client, _) in clients.iter().cloned().zip(1..) {
			// Loop over the threads
			for _ in 0..threads {
				let error = error.clone();
				let skip = skip.clone();
				let current = current.clone();
				let complete = complete.clone();
				let client = client.clone();
				let progress = progress.clone();
				let vp = vp.clone();
				let operation = operation.clone();
				let watchdog = Arc::new(WorkerWatchdog::default());
				watchdogs.push(watchdog.clone());
				tasks.spawn(async move {
					match Self::operation_loop::<C, D>(
						client,
						(iterations, rows),
						&error,
						&current,
						&complete,
						operation,
						watchdog,
						base_instant,
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
		// Background watchdog task to monitor for stuck worker operations without
		// paying per-iteration timer creation overhead in the hot path.
		let wd_notify = watchdog_notify.clone();
		let wds = watchdogs.clone();
		let error_flag = error.clone();
		let watchdog_handle = tokio::spawn(async move {
			let mut interval = tokio::time::interval(Duration::from_millis(500));
			interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			loop {
				interval.tick().await;
				if error_flag.load(Ordering::Relaxed) {
					break;
				}
				let now_ms = base_instant.elapsed().as_millis() as u64;
				for wd in &wds {
					let start = wd.active_start_ms.load(Ordering::Acquire);
					if start != 0 && now_ms.saturating_sub(start) >= timeout_ms {
						wd_notify.notify_one();
						return;
					}
				}
			}
		});
		// Wait for the threads to complete, aborting the remaining tasks on the first failure.
		let mut global_histogram = Histogram::new(3)?;
		let mut global_recall = RecallTally::default();
		let mut global_build_returned: Option<Duration> = None;
		let mut timed_out = false;
		loop {
			tokio::select! {
				result = tasks.join_next() => {
					match result {
						Some(Ok(Ok(Some((histogram, recall, build_returned))))) => {
							global_histogram.add(histogram)?;
							global_recall.merge(recall);
							// Only a build sets this, as one sample on one worker, so
							// taking the larger just picks the one that exists.
							global_build_returned = global_build_returned.max(build_returned);
						}
						Some(Ok(Ok(None))) => {}
						Some(Ok(Err(e))) => {
							error.store(true, Ordering::Relaxed);
							watchdog_handle.abort();
							tasks.abort_all();
							while tasks.join_next().await.is_some() {}
							if let Some(ref pb) = progress {
								pb.finish_and_clear();
							}
							return Err(e).with_context(|| format!("{operation} worker failed"));
						}
						Some(Err(e)) => {
							error.store(true, Ordering::Relaxed);
							watchdog_handle.abort();
							tasks.abort_all();
							while tasks.join_next().await.is_some() {}
							if let Some(ref pb) = progress {
								pb.finish_and_clear();
							}
							return Err(e).with_context(|| format!("{operation} task failed"));
						}
						None => break,
					}
				}
				_ = watchdog_notify.notified() => {
					timed_out = true;
					error.store(true, Ordering::Relaxed);
					break;
				}
			}
		}
		watchdog_handle.abort();
		if timed_out {
			tasks.abort_all();
			while tasks.join_next().await.is_some() {}
			if let Some(ref pb) = progress {
				pb.finish_and_clear();
			}
			bail!(
				"{operation} did not complete within {:?}{}",
				self.operation_timeout,
				timeout_hint(&operation)
			);
		}
		// Finish the progress bar at 100% before tearing it down
		if let Some(ref pb) = progress {
			pb.set_position(pb.length().unwrap_or(0));
			pb.finish_and_clear();
		}
		if error.load(Ordering::Relaxed) {
			bail!("Task failure");
		}
		// Histogram + sysinfo snapshots → OperationResult; then print phase timing line
		let result = OperationResult::new(metric, global_histogram)
			.with_recall(global_recall.summarise())
			.with_build_returned(global_build_returned);
		if quiet {
			return Ok((!skip.load(Ordering::Relaxed)).then_some(result));
		}
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
				// Say how a build split, when it split at all: for an engine that
				// finishes in the background, the gap is most of the build and the
				// reason the figure above is not the build call's own time.
				if let Some(returned) = result.build_returned() {
					let queryable = result.slowest().saturating_sub(returned);
					if queryable > Duration::from_millis(200) {
						self.bench_ui.println_muted(&format!(
							"  build call returned after {}, then {} until the index was queryable",
							format_duration(returned),
							format_duration(queryable)
						));
					}
				}
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
		// `rows` is the dataset size, which keys derived from a sample index
		// wrap around: a timed run can claim more samples than there are rows.
		(iterations, rows): (Iterations, u32),
		error: &AtomicBool,
		current: &AtomicU32,
		complete: &AtomicU32,
		operation: BenchmarkOperation,
		watchdog: Arc<WorkerWatchdog>,
		base_instant: Instant,
		// Enforced by the watchdog; passed in so a build can budget its wait
		// until the index is queryable against what is left of it.
		operation_timeout: Duration,
		(mut kp, mut vp, progress): (KeyProvider, ValueProvider, Option<Arc<ProgressBar>>),
	) -> Result<(Histogram<u64>, RecallTally, Option<Duration>)>
	where
		C: BenchmarkClient,
		D: Dialect,
	{
		let mut histogram = Histogram::new(3)?;
		let mut tally = RecallTally::default();
		// When an index build's own call returned, measured from the start of
		// the timed build. The build is timed until the index is queryable, so
		// this is the only place the split survives. Set by build operations
		// alone, and a build runs as a single sample.
		let mut build_returned: Option<Duration> = None;
		// A ramping window admits new samples only as earlier ones complete.
		let ramp = matches!(iterations, Iterations::Timed(w) if w.ramp > 0);
		// Check if we have encountered an error
		'claim: while !error.load(Ordering::Relaxed) {
			if ramp {
				while !iterations
					.admits(current.load(Ordering::Relaxed), complete.load(Ordering::Relaxed))
				{
					// Nothing more to admit once the window is over: stop rather
					// than wait for a slot this leg will not use.
					if iterations.exhausted(
						complete.load(Ordering::Relaxed),
						Instant::now(),
						base_instant,
					) || error.load(Ordering::Relaxed)
					{
						break 'claim;
					}
					tokio::time::sleep(Duration::from_millis(1)).await;
				}
			}
			// Get the current sample number
			let sample = current.fetch_add(1, Ordering::Relaxed);
			// Have we produced enough samples
			if iterations.exhausted(sample, Instant::now(), base_instant) {
				// We are done
				break;
			}
			// KNN hits, kept so recall can be scored after the latency is
			// recorded rather than inside the measured window.
			let mut scored: Option<(usize, Vec<KnnKey>)> = None;
			// Pre-generate payloads and query parameters outside the timed window
			// so that client-side generation CPU and memory allocations are not
			// attributed to the datastore's measured latency.
			let pregen_value = match &operation {
				BenchmarkOperation::Create => {
					Some(vp.generate_value_for(ValueStream::Create, sample))
				}
				BenchmarkOperation::Update => {
					Some(vp.generate_value_for(ValueStream::Update, sample))
				}
				_ => None,
			};
			let vec_query = match &operation {
				BenchmarkOperation::VectorScan(_, _, qs) => {
					Some((qs.pick(sample), qs.query_index(sample)))
				}
				_ => None,
			};

			let time = Instant::now();
			watchdog.begin_op(time, base_instant);
			let res = match &operation {
				BenchmarkOperation::Create => {
					client
						.create(sample, pregen_value.expect("payload pre-generated"), &mut kp)
						.await
				}
				BenchmarkOperation::Read => client.read(sample, &mut kp).await.map(|_| ()),
				BenchmarkOperation::Update => {
					client
						.update(sample, pregen_value.expect("payload pre-generated"), &mut kp)
						.await
				}
				BenchmarkOperation::Scan(s, ctx) => client.scan(s, &kp, *ctx).await,
				BenchmarkOperation::VectorScan(s, ctx, _) => {
					let (q, q_idx) = vec_query.expect("vector query pre-picked");
					let hits = client.scan_vector(s, q, &kp, *ctx).await?;
					scored = Some((q_idx, hits));
					Ok(())
				}
				BenchmarkOperation::ScanWithWrites(scan, ctx, spec) => {
					workloads::run_scan_with_writes(
						&*client, scan, *ctx, spec, sample, rows, &mut kp,
					)
					.await
				}
				// A build is timed until the index serves at index speed, which
				// for some engines is long after the build call returns. The two
				// coincide for an engine whose build call does all the work, and
				// are minutes to hours apart for one that finishes in the
				// background — only the later one lets their builds share a
				// column.
				BenchmarkOperation::BuildIndex(spec, id, _) => {
					match client.build_index(spec, id.as_str()).await {
						Ok(()) => {
							build_returned = Some(time.elapsed());
							let budget = queryable_budget(operation_timeout, time.elapsed());
							client.await_index_queryable(id.as_str(), budget).await
						}
						Err(e) => Err(e),
					}
				}
				BenchmarkOperation::BuildVectorIndex(spec, vq, dim, name) => {
					match client.build_vector_index(spec, vq, *dim, name.as_str()).await {
						Ok(()) => {
							build_returned = Some(time.elapsed());
							let budget = queryable_budget(operation_timeout, time.elapsed());
							client.await_index_queryable(name.as_str(), budget).await
						}
						Err(e) => Err(e),
					}
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
			};
			watchdog.end_op();
			res?;
			// Only touch the shared completion counter when a progress bar is
			// actually rendering; otherwise this is pure cross-task cache-line
			// contention on every single sample for no observer.
			// A timed run's bar tracks elapsed time on its own, so only a
			// fixed-count run reports completions into it.
			if let (Some(pb), Iterations::Fixed(total)) = (&progress, iterations) {
				let sample = complete.fetch_add(1, Ordering::Relaxed);
				let done = (sample + 1).min(total) as u64;
				pb.set_position(done);
			} else if ramp {
				// A ramping window admits by completions, so it always counts them.
				complete.fetch_add(1, Ordering::Relaxed);
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
		Ok((histogram, tally, build_returned))
	}
}

/// How far inside the operation's deadline the queryable wait's own deadline
/// is set.
///
/// Both bound the same wait. The operation timeout cancels the future and can
/// only say that the build did not finish; the adapter's deadline fails with
/// what it last saw — entries pending, still compacting, percent indexed — which
/// is the part worth reading. Setting the adapter's slightly earlier makes its
/// error the one that surfaces. The margin only has to exceed one poll interval
/// (at most 250ms) plus one status round trip.
const QUERYABLE_DEADLINE_MARGIN: Duration = Duration::from_secs(1);

/// Name for the index on a filter column: scoped by scan so two scans never
/// share one, and reduced to characters every engine accepts in an identifier
/// (a nested column such as `geography.code` would not be).
fn filter_index_name(scan_id: &str, column: &str) -> String {
	let column: String = column
		.chars()
		.map(|c| {
			if c.is_ascii_alphanumeric() {
				c
			} else {
				'_'
			}
		})
		.collect();
	format!("{scan_id}_filter_{column}")
}

/// Budget for the queryable wait inside a timed build: what is left of the
/// operation timeout after the build call, less [`QUERYABLE_DEADLINE_MARGIN`].
fn queryable_budget(operation_timeout: Duration, spent: Duration) -> Duration {
	operation_timeout.saturating_sub(spent).saturating_sub(QUERYABLE_DEADLINE_MARGIN)
}

/// Extra context for a timeout error, naming the knob that resolves it.
///
/// An index build now includes the wait until the index is queryable, which
/// for a large index finished in the background can take far longer than the
/// build call itself — over an hour for a 1M-row SurrealDB HNSW index — so a
/// build that used to fit inside the default can stop fitting.
fn timeout_hint(operation: &BenchmarkOperation) -> &'static str {
	match operation {
		BenchmarkOperation::BuildIndex(..) | BenchmarkOperation::BuildVectorIndex(..) => {
			"; an index build includes the wait until the index is queryable, which for a \
			 large index finished in the background can far exceed the build call. Raise \
			 --operation-timeout to allow for it"
		}
		_ => "",
	}
}

/// Warm-up queries measured together before checking for a plateau.
const VECTOR_WARMUP_WINDOW: u32 = 25;

/// Floor on the warm-up query ceiling, for small corpora.
const VECTOR_WARMUP_MIN_QUERIES: u32 = 500;

/// Rows per permitted warm-up query, so the ceiling grows with the index.
///
/// The plateau check compares consecutive windows, and on a noisy machine a
/// window can beat the one before it by chance for a long time — which kept
/// warm-up running for thousands of queries against a 10k-row index that needed
/// a few hundred, and blew a CI step's budget. A ceiling bounds that, but a
/// fixed one under-warms a large index just as silently, so it scales: 500
/// queries at 10k rows, 4000 at 200k.
const VECTOR_WARMUP_ROWS_PER_QUERY: u32 = 50;

/// A window this close to the one `VECTOR_WARMUP_PLATEAU_SPAN` back means
/// warming has plateaued. Expressed as a percentage so the comparison stays in
/// integer arithmetic.
const VECTOR_WARMUP_PLATEAU_PCT: u32 = 90;

/// How many windows back the plateau check compares against.
///
/// Comparing against the *immediately preceding* window asks whether latency is
/// locally flat, which is not the same question as whether the index is warm. A
/// backlog draining steadily improves each window by less than the threshold
/// while still falling steeply over any longer horizon, so a per-step check
/// calls it settled and the leg is then timed against a half-materialised
/// index. Measured against a 1M-row HNSW index: warm-up reported a plateau
/// after ~2 minutes and the leg that followed ran at ~600ms per query, roughly
/// 100x the same query on the same index once its backlog had drained.
///
/// Spanning several windows catches that: a decline too gradual to trip the
/// per-step test still compounds across the span. Five windows is enough to see
/// through it and costs an already-warm index ~125 extra queries, which at
/// sub-millisecond latencies is noise.
const VECTOR_WARMUP_PLATEAU_SPAN: usize = 4;

/// Whether warm-up has settled: this window is no real improvement on the one a
/// full span back.
///
/// Returns false until the span has filled, so the decision is never taken on
/// less evidence than it needs.
fn warmup_has_plateaued(recent: &VecDeque<Duration>, window: Duration) -> bool {
	match recent.front() {
		Some(&oldest) if recent.len() == VECTOR_WARMUP_PLATEAU_SPAN => {
			window * 100 >= oldest * VECTOR_WARMUP_PLATEAU_PCT
		}
		_ => false,
	}
}

/// Config name of a strategy's search-time knob, for labelling sweep legs.
/// One timed vector leg: the pinned search value and predicate it ran under,
/// and what it measured.
struct VectorLeg {
	/// How this leg is told apart from its siblings in the results — the search
	/// value, the filter name, or both. `None` for a scan that ran once.
	label: Option<String>,
	/// Measured share of the corpus this leg's filter admitted.
	selectivity: Option<f64>,
	/// `None` when the engine skipped the leg.
	result: Option<OperationResult>,
}

/// Row label distinguishing one leg of a vector scan from its siblings.
///
/// The selectivity rides along with the filter name because the name alone is
/// arbitrary — `tier0` says nothing about how much of the corpus it keeps, and
/// that share is what every filtered number has to be read against.
fn leg_heading(
	vq: &VectorQuerySpec,
	search: Option<u32>,
	filter: Option<&VectorFilter>,
	selectivity: Option<f64>,
) -> Option<String> {
	let mut parts = Vec::with_capacity(2);
	if let Some(value) = search {
		parts.push(format!("{} = {value}", search_param_label(&vq.index_strategy)));
	}
	if !vq.filters.is_empty() {
		parts.push(match filter {
			Some(f) => match selectivity {
				Some(s) => format!("filter {} ({})", f.name, format_selectivity(s)),
				None => format!("filter {}", f.name),
			},
			None => "unfiltered".to_string(),
		});
	}
	(!parts.is_empty()).then(|| parts.join(" · "))
}

/// Selectivity as a percentage, with enough precision to keep a 0.1%-selective
/// predicate distinguishable from a 1% one.
pub(crate) fn format_selectivity(selectivity: f64) -> String {
	let pct = selectivity * 100.0;
	if pct >= 10.0 {
		format!("{pct:.0}%")
	} else if pct >= 1.0 {
		format!("{pct:.1}%")
	} else {
		format!("{pct:.2}%")
	}
}

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

#[cfg(test)]
mod test {
	use super::{
		Benchmark, BenchmarkOperation, Iterations, QUERYABLE_DEADLINE_MARGIN,
		VECTOR_WARMUP_PLATEAU_SPAN, Window, warmup_has_plateaued,
	};
	use crate::dialect::DefaultDialect;
	use crate::engine::BenchmarkClient;
	use crate::keyprovider::KeyProvider;
	use crate::value::BenchValue;
	use crate::valueprovider::ValueProvider;
	use crate::{Args, Index, KeyType, VectorQuerySpec};
	use anyhow::Result;
	use clap::Parser;
	use std::collections::VecDeque;
	use std::sync::{Arc, Mutex};
	use std::time::Duration;
	use tokio::time::Instant;

	fn window(measure_ms: u64, min: u32, max: u32) -> Iterations {
		Iterations::Timed(Window {
			warmup: Duration::ZERO,
			measure: Duration::from_millis(measure_ms),
			min,
			max,
			ramp: 0,
		})
	}

	fn ramped(min: u32) -> Iterations {
		Iterations::Timed(Window {
			warmup: Duration::ZERO,
			measure: Duration::from_secs(5),
			min: 1,
			max: u32::MAX,
			ramp: min,
		})
	}

	#[test]
	fn a_window_without_a_ramp_admits_every_worker() {
		assert!(window(100, 1, 1000).admits(6_144, 0));
		assert!(Iterations::Fixed(10).admits(1_000_000, 0));
	}

	#[test]
	fn a_ramp_starts_at_its_floor() {
		let it = ramped(10);
		assert!(it.admits(9, 0));
		assert!(!it.admits(10, 0));
	}

	#[test]
	fn each_completion_admits_two_more() {
		let it = ramped(10);
		assert!(!it.admits(12, 1));
		assert!(it.admits(11, 1));
		assert!(it.admits(29, 10));
		assert!(!it.admits(30, 10));
	}

	fn budget_window(measure_secs: u64, min: u32, max: u32) -> Window {
		Window {
			warmup: Duration::from_secs(5),
			measure: Duration::from_secs(measure_secs),
			min,
			max,
			ramp: 0,
		}
	}

	#[test]
	fn a_slow_leg_is_budgeted_what_fits_its_window() {
		// Two full scans a second over a 30 s window, with headroom: 90, not a
		// query per worker.
		assert_eq!(budget_window(30, 10, 10_000_000).budget(2.0), 90);
	}

	#[test]
	fn a_budget_never_falls_below_the_minimum() {
		assert_eq!(budget_window(30, 10, 10_000_000).budget(0.01), 10);
	}

	#[test]
	fn a_budget_never_exceeds_the_maximum() {
		assert_eq!(budget_window(30, 10, 1_000).budget(1_000_000.0), 1_000);
	}

	#[test]
	fn an_unmeasured_warmup_leaves_the_maximum() {
		assert_eq!(budget_window(30, 10, 5_000).budget(0.0), 5_000);
		assert_eq!(budget_window(30, 10, 5_000).budget(f64::NAN), 5_000);
	}

	#[test]
	fn a_fixed_run_ignores_the_clock() {
		let opened = Instant::now();
		let later = opened + Duration::from_secs(3600);
		let it = Iterations::Fixed(3);
		assert!(!it.exhausted(2, later, opened));
		assert!(it.exhausted(3, opened, opened));
	}

	#[test]
	fn a_window_runs_until_it_elapses() {
		let opened = Instant::now();
		let it = window(100, 1, 1000);
		assert!(!it.exhausted(50, opened + Duration::from_millis(99), opened));
		assert!(it.exhausted(50, opened + Duration::from_millis(100), opened));
	}

	#[test]
	fn a_window_honours_the_minimum_past_its_deadline() {
		let opened = Instant::now();
		let late = opened + Duration::from_secs(10);
		let it = window(100, 5, 1000);
		assert!(!it.exhausted(4, late, opened));
		assert!(it.exhausted(5, late, opened));
	}

	#[test]
	fn a_window_closes_at_the_maximum_even_with_time_left() {
		let opened = Instant::now();
		let it = window(60_000, 1, 10);
		assert!(!it.exhausted(9, opened, opened));
		assert!(it.exhausted(10, opened, opened));
	}

	#[test]
	fn a_timed_scan_reports_the_most_any_leg_recorded() {
		let it = window(100, 1, 1000);
		assert_eq!(it.reported(&[]), 0);
		assert_eq!(Iterations::Fixed(7).reported(&[]), 7);
	}

	fn ms(v: u64) -> Duration {
		Duration::from_millis(v)
	}

	fn recent(values: &[u64]) -> VecDeque<Duration> {
		values.iter().map(|v| ms(*v)).collect()
	}

	/// No verdict before the span has filled — the whole point is to decide on
	/// a horizon rather than on one step.
	#[test]
	fn holds_off_until_the_span_fills() {
		for n in 0..VECTOR_WARMUP_PLATEAU_SPAN {
			let window = recent(&vec![100; n]);
			assert!(!warmup_has_plateaued(&window, ms(100)), "decided on {n} windows");
		}
	}

	/// An index that was already warm settles as soon as it has the evidence,
	/// so a later sweep leg pays the span and no more.
	#[test]
	fn flat_latency_plateaus_once_the_span_fills() {
		assert!(warmup_has_plateaued(&recent(&[100, 100, 100, 100]), ms(100)));
	}

	/// The regression this exists for. Each step improves by ~7%, under the 10%
	/// per-step threshold, so the old consecutive-window check called it
	/// settled — while latency was still falling by a third across the span.
	/// Measured against a 1M-row HNSW index, that mistake timed the next leg at
	/// ~600ms per query against ~5ms once the index had actually settled.
	#[test]
	fn a_gradual_decline_does_not_read_as_settled() {
		let window = recent(&[1500, 1395, 1297, 1206]);
		assert!(
			!warmup_has_plateaued(&window, ms(1122)),
			"a run still improving 25% across the span is not warm"
		);
	}

	/// A single lucky window inside a still-declining series must not end
	/// warm-up either: the comparison is against the span, not the neighbour.
	#[test]
	fn one_fast_window_does_not_end_a_decline() {
		let window = recent(&[2000, 1800, 1600, 1400]);
		assert!(!warmup_has_plateaued(&window, ms(1350)));
	}

	/// Noise upward is settled, not a reason to keep warming.
	#[test]
	fn a_slower_window_counts_as_settled() {
		assert!(warmup_has_plateaued(&recent(&[100, 102, 98, 101]), ms(140)));
	}

	/// Index names are scoped by scan and reduced to identifier characters, so
	/// a nested column cannot produce DDL an engine rejects.
	#[test]
	fn filter_index_names_are_scoped_and_identifier_safe() {
		assert_eq!(super::filter_index_name("knn", "number"), "knn_filter_number");
		assert_eq!(super::filter_index_name("knn", "geography.code"), "knn_filter_geography_code");
		assert_ne!(super::filter_index_name("a", "n"), super::filter_index_name("b", "n"));
	}

	// ------------------------------------------------------------------
	// Index build accounting
	// ------------------------------------------------------------------

	/// An engine shaped like the ones this accounting exists for: its build
	/// call returns after `build`, and the index needs a further `queryable`
	/// before it serves at index speed. `queryable = 0` is a synchronous engine.
	struct Builder {
		build: Duration,
		queryable: Duration,
		/// The budget the harness handed to the queryable wait.
		budget_seen: Mutex<Option<Duration>>,
	}

	impl Builder {
		fn new(build: Duration, queryable: Duration) -> Arc<Self> {
			Arc::new(Self {
				build,
				queryable,
				budget_seen: Mutex::new(None),
			})
		}
	}

	impl BenchmarkClient for Builder {
		type ReadRow = BenchValue;
		async fn create_u32(&self, _: u32, _: BenchValue) -> Result<()> {
			Ok(())
		}
		async fn create_string(&self, _: String, _: BenchValue) -> Result<()> {
			Ok(())
		}
		async fn read_u32(&self, _: u32) -> Result<BenchValue> {
			Ok(BenchValue::Null)
		}
		async fn read_string(&self, _: String) -> Result<BenchValue> {
			Ok(BenchValue::Null)
		}
		async fn update_u32(&self, _: u32, _: BenchValue) -> Result<()> {
			Ok(())
		}
		async fn update_string(&self, _: String, _: BenchValue) -> Result<()> {
			Ok(())
		}
		async fn delete_u32(&self, _: u32) -> Result<()> {
			Ok(())
		}
		async fn delete_string(&self, _: String) -> Result<()> {
			Ok(())
		}
		async fn build_index(&self, _: &Index, _: &str) -> Result<()> {
			tokio::time::sleep(self.build).await;
			Ok(())
		}
		async fn build_vector_index(
			&self,
			_: &Index,
			_: &VectorQuerySpec,
			_: usize,
			_: &str,
		) -> Result<()> {
			tokio::time::sleep(self.build).await;
			Ok(())
		}
		async fn await_index_queryable(&self, _: &str, timeout: Duration) -> Result<()> {
			*self.budget_seen.lock().unwrap() = Some(timeout);
			if !self.queryable.is_zero() {
				tokio::time::sleep(self.queryable).await;
			}
			Ok(())
		}
	}

	fn bench(operation_timeout_secs: u64) -> Benchmark {
		let secs = operation_timeout_secs.to_string();
		let args = Args::try_parse_from([
			"crud-bench",
			"-d",
			"dry",
			"-s",
			"1",
			"--operation-timeout",
			&secs,
		])
		.expect("test args parse");
		Benchmark::new(&args)
	}

	fn index() -> Index {
		Index {
			skip: false,
			fields: vec!["e".to_string()],
			unique: None,
			index_type: None,
		}
	}

	fn vector_build() -> BenchmarkOperation {
		let vq: VectorQuerySpec = serde_json::from_str(
			r#"{ "field": "e", "top_k": 10, "distance": "cosine",
			   "index_strategy": { "kind": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 64 } }"#,
		)
		.expect("spec parses");
		BenchmarkOperation::BuildVectorIndex(index(), vq, 8, "idx".to_string())
	}

	fn plain_build() -> BenchmarkOperation {
		BenchmarkOperation::BuildIndex(index(), "idx".to_string(), "idx".to_string())
	}

	async fn run_build(
		bench: &Benchmark,
		client: &Arc<Builder>,
		op: BenchmarkOperation,
	) -> Result<Option<crate::result::OperationResult>> {
		let kp = KeyProvider::new(KeyType::Integer, false);
		let vp = ValueProvider::new(r#"{ "e": "vector:8" }"#).expect("template parses");
		let clients = std::slice::from_ref(client);
		bench.run_operation::<Builder, DefaultDialect>(clients, op, kp, vp, 1, 1).await
	}

	/// The accounting this exists for. An engine whose build call returns
	/// quickly but whose index needs longer to become queryable is reported at
	/// the full time — the wait lands inside the build — while the build call's
	/// own time survives as the split.
	///
	/// Every bound here rests on one guarantee, that a sleep never returns
	/// early, so machine load can only make them hold more easily.
	#[tokio::test]
	async fn a_build_is_timed_until_the_index_is_queryable() {
		for op in [vector_build(), plain_build()] {
			let name = op.to_string();
			let client = Builder::new(ms(40), ms(300));
			let result = run_build(&bench(60), &client, op).await.unwrap().expect("build ran");
			assert!(
				result.slowest() >= ms(340),
				"{name}: build reported {:?}, but the index took 340ms to become queryable",
				result.slowest()
			);
			let returned = result.build_returned().expect("a build records its split");
			assert!(
				returned >= ms(40),
				"{name}: split {returned:?} is before the build call ended"
			);
			assert!(
				result.slowest() - returned >= ms(300),
				"{name}: split {returned:?} of {:?} swallowed the 300ms queryable wait",
				result.slowest()
			);
		}
	}

	/// A synchronous engine is not penalised: with nothing left to wait for,
	/// the harness adds no poll interval of its own, so the build is the build
	/// call. The bound is set well under the 250ms poll interval a regression
	/// would add, and well over scheduling noise, on the operation's own clock.
	#[tokio::test]
	async fn a_synchronous_build_is_not_inflated() {
		let client = Builder::new(ms(60), Duration::ZERO);
		let result = run_build(&bench(60), &client, vector_build()).await.unwrap().expect("ran");
		let returned = result.build_returned().expect("a build records its split");
		assert!(
			result.slowest() - returned < ms(100),
			"a synchronous build call returned after {returned:?} but was reported as {:?}",
			result.slowest()
		);
	}

	/// The wait is bounded by what the operation timeout has left, less the
	/// margin that lets the adapter's own, more specific error surface first —
	/// not by a cap each adapter picks for itself.
	#[tokio::test]
	async fn the_wait_gets_the_rest_of_the_operation_budget() {
		let client = Builder::new(ms(40), Duration::ZERO);
		run_build(&bench(60), &client, vector_build()).await.unwrap().expect("ran");
		let budget = client.budget_seen.lock().unwrap().expect("the wait was called");
		let ceiling = Duration::from_secs(60) - QUERYABLE_DEADLINE_MARGIN;
		assert!(budget <= ceiling - ms(40), "budget {budget:?} ignores the 40ms already spent");
		// Not an adapter's own cap (they were 600s and 3600s), nor a small fixed
		// one: the remainder of the 60s operation budget.
		assert!(
			budget > ceiling - Duration::from_secs(5),
			"budget {budget:?} is not the remainder"
		);
	}

	/// A build that outlasts `--operation-timeout` fails, and says which knob
	/// to turn: the wait being part of the build is what makes a build that
	/// used to fit stop fitting.
	#[tokio::test]
	async fn a_build_past_the_timeout_names_the_knob() {
		let client = Builder::new(ms(10), Duration::from_secs(30));
		let Err(err) = run_build(&bench(1), &client, vector_build()).await else {
			panic!("a build past the operation timeout must fail");
		};
		let msg = format!("{err:#}");
		assert!(msg.contains("--operation-timeout"), "{msg}");
		assert!(msg.contains("queryable"), "{msg}");
	}
}
