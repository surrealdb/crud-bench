//! Orchestrates benchmark phases (CRUD, scans, batches) against a [`crate::engine::BenchmarkEngine`].
//!
//! Spawns concurrent clients/threads, records latency histograms, and aggregates
//! [`crate::result::OperationResult`] values for reporting.

use crate::dialect::Dialect;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::keyprovider::KeyProvider;
use crate::result::{
	BenchmarkMetadata, BenchmarkResult, OperationMetric, OperationResult, ScanResult, ScanRun,
	ScanWorkload, writes_ratio_percent,
};
use crate::system::SystemInfo;
use crate::terminal::BenchUi;
use crate::util::format_duration;
use crate::valueprovider::ValueProvider;
use crate::workloads;
use crate::{Args, BatchOperation, Batches, Index, Scan, ScanWithWrites, Scans};

use anyhow::{Context, Result, bail};
use futures::future::try_join_all;
use hdrhistogram::Histogram;
use indicatif::ProgressBar;
use log::{debug, info};
use tokio::task;
use tokio::time::Instant;

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

/// Maximum wait when polling until the first datastore client connects.
const TIMEOUT: Duration = Duration::from_secs(60);

/// Error string returned by adapters to mark an operation as unsupported (skipped, not fatal).
pub(crate) const NOT_SUPPORTED_ERROR: &str = "NotSupported";

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
		}
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
		}
		Ok(())
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
			let samples = scan.samples.map(|s| s as u32).unwrap_or(self.samples);
			let write_specs = scan.with_writes.as_slice();
			let w = write_specs.len();
			let index_spec = scan.with_index.as_ref().filter(|i| !i.skip);

			let result = if let Some(index_spec) = index_spec {
				// Indexed scan: heap legs → build index → indexed legs → drop index
				let mut runs = Vec::with_capacity(2 + 2 * w);
				// Table-scan / heap query (no physical index)
				let without_index = self
					.run_operation::<C, D>(
						&clients,
						BenchmarkOperation::Scan(scan.clone(), ScanContext::WithoutIndex),
						kp,
						vp.clone(),
						samples,
					)
					.await?;
				runs.push(ScanRun {
					workload: ScanWorkload::Read,
					indexed: false,
					result: without_index,
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
							samples,
						)
						.await?;
					runs.push(ScanRun {
						workload: ScanWorkload::ReadWrite {
							write_ratio_percent: writes_ratio_percent(spec),
						},
						indexed: false,
						result: mixed_without_index,
					});
				}
				// BuildIndex uses a single client to avoid races on DDL
				let index_build = self
					.run_operation::<C, D>(
						&clients[..1],
						BenchmarkOperation::BuildIndex(index_spec.clone(), id.clone()),
						kp,
						vp.clone(),
						1,
					)
					.await?;
				let (with_index, index_remove, indexed_write_results) = if index_build.is_some() {
					// Same query shape using the new index
					let with_index = self
						.run_operation::<C, D>(
							&clients,
							BenchmarkOperation::Scan(scan.clone(), ScanContext::WithIndex),
							kp,
							vp.clone(),
							samples,
						)
						.await?;
					let mut iw = Vec::with_capacity(w);
					for spec in write_specs {
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
								samples,
							)
							.await?,
						);
					}
					let index_remove = self
						.run_operation::<C, D>(
							&clients[..1],
							BenchmarkOperation::RemoveIndex(id.clone()),
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
					});
					for (spec, r) in write_specs.iter().zip(indexed_write_results) {
						runs.push(ScanRun {
							workload: ScanWorkload::ReadWrite {
								write_ratio_percent: writes_ratio_percent(spec),
							},
							indexed: true,
							result: r,
						});
					}
				} else {
					// Still emit indexed rows so CSV/HTML rows align; cells show "-" when result is None
					runs.push(ScanRun {
						workload: ScanWorkload::Read,
						indexed: true,
						result: None,
					});
					for spec in write_specs {
						runs.push(ScanRun {
							workload: ScanWorkload::ReadWrite {
								write_ratio_percent: writes_ratio_percent(spec),
							},
							indexed: true,
							result: None,
						});
					}
				}
				ScanResult {
					id: id.clone(),
					name,
					samples,
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
						samples,
					)
					.await?;
				runs.push(ScanRun {
					workload: ScanWorkload::Read,
					indexed: false,
					result: without_index,
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
							samples,
						)
						.await?;
					runs.push(ScanRun {
						workload: ScanWorkload::ReadWrite {
							write_ratio_percent: writes_ratio_percent(spec),
						},
						indexed: false,
						result: mixed_without_index,
					});
				}
				ScanResult {
					id: id.clone(),
					name,
					samples,
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
			let samples = batch.samples.map(|s| s as u32).unwrap_or(self.samples);
			// Determine the batch operation type
			let operation = match batch.operation {
				crate::BatchOperationType::Create => BenchmarkOperation::BatchCreate(batch.clone()),
				crate::BatchOperationType::Read => BenchmarkOperation::BatchRead(batch.clone()),
				crate::BatchOperationType::Update => BenchmarkOperation::BatchUpdate(batch.clone()),
				crate::BatchOperationType::Delete => BenchmarkOperation::BatchDelete(batch.clone()),
			};
			// Execute the batch benchmark
			let duration =
				self.run_operation::<C, D>(&clients, operation, kp, vp.clone(), samples).await?;
			// Store the batch benchmark result
			batch_results.push((name, samples, groups, duration));
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
		if self.emit_phase_markers {
			self.bench_ui.println_plain(&format!("{operation} starting"));
		}
		let progress =
			self.bench_ui.progress_bar(samples as u64, &progress_short_label(&operation));
		// Get the total concurrent futures
		let total = (self.clients * self.threads) as usize;
		// Whether we have experienced an error
		let error = Arc::new(AtomicBool::new(false));
		// Wether the test should be skipped
		let skip = Arc::new(AtomicBool::new(false));
		// The total records processed so far
		let current = Arc::new(AtomicU32::new(0));
		// The total records processed so far
		let complete = Arc::new(AtomicU32::new(0));
		// Store the futures in a vector
		let mut futures = Vec::with_capacity(total);
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
				futures.push(task::spawn(async move {
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
				}));
			}
		}
		// Wait for all the threads to complete
		let mut global_histogram = Histogram::new(3)?;
		let join = try_join_all(futures).await;
		// Finish the progress bar at 100% before tearing it down
		if let Some(ref pb) = progress {
			pb.set_position(samples as u64);
			pb.finish_and_clear();
		}
		match join {
			Ok(results) => {
				// Merge per-worker HDR histograms into one distribution for this phase
				for res in results {
					if let Some(histogram) = res? {
						global_histogram.add(histogram)?;
					}
				}
			}
			Err(e) => {
				// Join error (e.g. worker panic): surface after marking failure
				error.store(true, Ordering::Relaxed);
				Err(e)?;
			}
		};
		if error.load(Ordering::Relaxed) {
			bail!("Task failure");
		}
		// Histogram + sysinfo snapshots → OperationResult; then print phase timing line
		let result = OperationResult::new(metric, global_histogram);
		let took = result.total_time();
		match &operation {
			BenchmarkOperation::Scan(_, ctx) => {
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
		// Shall we skip the operation? (operation not supported)
		if skip.load(Ordering::Relaxed) {
			return Ok(None);
		}
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
	) -> Result<Histogram<u64>>
	where
		C: BenchmarkClient,
		D: Dialect,
	{
		let mut histogram = Histogram::new(3)?;
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
			// parking the worker task forever; `try_join_all` then
			// short-circuits with the operation name in the error
			// chain rather than hanging in `block_on`.
			let time = Instant::now();
			tokio::time::timeout(operation_timeout, async {
				match &operation {
					BenchmarkOperation::Create => {
						let value = vp.generate_value();
						client.create(sample, value, &mut kp).await
					}
					BenchmarkOperation::Read => client.read(sample, &mut kp).await.map(|_| ()),
					BenchmarkOperation::Update => {
						let value = vp.generate_value();
						client.update(sample, value, &mut kp).await
					}
					BenchmarkOperation::Scan(s, ctx) => client.scan(s, &kp, *ctx).await,
					BenchmarkOperation::ScanWithWrites(scan, ctx, spec) => {
						workloads::run_scan_with_writes(
							&*client, scan, *ctx, spec, sample, samples, &mut kp,
						)
						.await
					}
					BenchmarkOperation::BuildIndex(spec, name) => {
						client.build_index(spec, name.as_str()).await
					}
					BenchmarkOperation::RemoveIndex(name) => client.drop_index(name.as_str()).await,
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
		}
		Ok(histogram)
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
	/// Scan plus mixed writes according to [`ScanWithWrites`].
	ScanWithWrites(Scan, ScanContext, ScanWithWrites),
	/// Create backing index for the given analyzer/index id.
	BuildIndex(Index, String),
	/// Drop index by stable scan id.
	RemoveIndex(String),
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
			Self::ScanWithWrites(_, ctx, spec) => {
				write!(
					f,
					"Scan :: {}, combined workload (ratio {}%)",
					scan_context_slug(*ctx),
					writes_ratio_percent(spec)
				)
			}
			Self::BuildIndex(_, _) => write!(f, "BuildIndex"),
			Self::RemoveIndex(_) => write!(f, "RemoveIndex"),
			Self::Update => write!(f, "Update"),
			Self::Delete => write!(f, "Delete"),
			Self::BatchCreate(b) => write!(f, "BatchCreate::{}", b.name),
			Self::BatchRead(b) => write!(f, "BatchRead::{}", b.name),
			Self::BatchUpdate(b) => write!(f, "BatchUpdate::{}", b.name),
			Self::BatchDelete(b) => write!(f, "BatchDelete::{}", b.name),
		}
	}
}

/// Truncated label for the indicatif progress bar (scan/batch variants).
fn progress_short_label(operation: &BenchmarkOperation) -> String {
	const MAX: usize = 72;
	let s = match operation {
		BenchmarkOperation::Scan(_, ctx) => scan_context_slug(*ctx).to_string(),
		BenchmarkOperation::ScanWithWrites(_, ctx, spec) => {
			format!("{}, writes {}%", scan_context_slug(*ctx), writes_ratio_percent(spec))
		}
		BenchmarkOperation::BuildIndex(_, _) => "BuildIndex".to_string(),
		BenchmarkOperation::RemoveIndex(_) => "RemoveIndex".to_string(),
		_ => operation.to_string(),
	};
	if s.len() > MAX {
		format!("{}…", &s[..MAX.saturating_sub(1)])
	} else {
		s
	}
}
