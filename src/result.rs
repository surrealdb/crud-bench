//! Serializable benchmark outcomes: CRUD/scan/batch metrics, terminal tables, CSV, and HTML charts.

use crate::system::SystemInfo;
use crate::util::format_duration;
use bytesize::ByteSize;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Attribute, Cell, CellAlignment, Color, ContentArrangement, Table};
use csv::Writer;
use hdrhistogram::Histogram;
use serde::Serialize;
use serde_json::Value;
use std::fmt::{Display, Formatter};
use std::process;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::{
	DiskUsage, LoadAvg, Pid, Process, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System,
};
use tokio::task::JoinHandle;

/// Static inputs echoed in JSON results for reproducibility (CLI snapshot).
#[derive(Clone, Serialize)]
pub(crate) struct BenchmarkMetadata {
	/// Row count for core CRUD phases.
	pub(crate) samples: u32,
	/// Concurrent datastore connections.
	pub(crate) clients: u32,
	/// Worker tasks per client.
	pub(crate) threads: u32,
	/// Stringified [`crate::KeyType`].
	pub(crate) key_type: String,
	/// Whether primary keys were generated in random order.
	pub(crate) random: bool,
	/// Durability / fsync expectations where applicable.
	pub(crate) sync: bool,
	/// Redis-family append-only / persistence toggles.
	pub(crate) persisted: bool,
	/// Tuned server settings vs defaults where supported.
	pub(crate) optimised: bool,
}

/// Full benchmark output: timings per phase plus one representative generated [`serde_json::Value`].
#[derive(Serialize)]
pub(crate) struct BenchmarkResult {
	/// Display name of the datastore under test.
	pub(crate) database: Option<String>,
	/// Host snapshot (CPU, memory, disks) when collected.
	pub(crate) system: Option<SystemInfo>,
	/// CLI parameters snapshot ([`BenchmarkMetadata`]).
	pub(crate) metadata: Option<BenchmarkMetadata>,
	/// Single-record insert phase.
	pub(crate) creates: Option<OperationResult>,
	/// Single-record read phase.
	pub(crate) reads: Option<OperationResult>,
	/// Single-record update phase.
	pub(crate) updates: Option<OperationResult>,
	/// One entry per configured scan id (possibly multiple timed legs inside [`ScanResult::runs`]).
	pub(crate) scans: Vec<ScanResult>,
	/// `(batch_case_name, timed_iterations, records_per_batch, histogram_metrics_or_skip)`.
	pub(crate) batches: Vec<(String, u32, usize, Option<OperationResult>)>,
	/// Single-record delete phase.
	pub(crate) deletes: Option<OperationResult>,
	/// Example document produced by the value template (for inspection / stored results).
	pub(crate) sample: Value,
}

/// Tags each timed scan leg for JSON consumers (`kind` discriminators).
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ScanWorkload {
	/// Pure read/query workload.
	Read,
	/// Read path plus compensating writes at this percentage of samples.
	ReadWrite {
		write_ratio_percent: u32,
	},
}

/// One timed scan leg (read-only or read+writes, with or without a physical index).
#[derive(Serialize)]
pub(crate) struct ScanRun {
	/// Pure read vs read+writes ([`ScanWorkload`]).
	pub workload: ScanWorkload,
	/// Whether this leg used the indexed query path (vs table scan).
	pub indexed: bool,
	/// Latency histogram + resource stats; [`None`] when the backend skipped the leg.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub result: Option<OperationResult>,
}

/// Normalised `with_writes.ratio` for labels and serialised results.
pub(crate) fn writes_ratio_percent(spec: &crate::ScanWithWrites) -> u32 {
	(spec.ratio.clamp(0.0, 1.0) * 100.0).round() as u32
}

/// Table row title for a scan leg (matches `[S]can` markers in stdout tables).
pub(crate) fn scan_run_row_label(id: &str, name: &str, samples: u32, run: &ScanRun) -> String {
	let index_slug = if run.indexed {
		"indexed"
	} else {
		"no-index"
	};
	let mid = match &run.workload {
		ScanWorkload::Read => format!("reads - {index_slug}"),
		ScanWorkload::ReadWrite {
			write_ratio_percent: p,
		} => format!("reads+writes ({p}%) - {index_slug}"),
	};
	format!("[S]can · {id} · {name} - {mid} ({samples})")
}

impl ScanRun {
	/// Short label for charts (query text + leg description).
	pub(crate) fn chart_label(&self, query: &str) -> String {
		let index_slug = if self.indexed {
			"indexed"
		} else {
			"no-index"
		};
		match &self.workload {
			ScanWorkload::Read => format!("{query} - reads - {index_slug}"),
			ScanWorkload::ReadWrite {
				write_ratio_percent: p,
			} => format!("{query} - reads+writes ({p}%) - {index_slug}"),
		}
	}
}

#[derive(Serialize)]
/// Aggregated timings for one logical scan benchmark (`id`) including index ops when applicable.
pub(crate) struct ScanResult {
	/// Stable scan identifier from config.
	pub(crate) id: String,
	/// Human-readable scan title.
	pub(crate) name: String,
	/// Sample count for timed scan legs (may override global default).
	pub(crate) samples: u32,
	/// Index creation phase when an indexed leg exists.
	pub(crate) index_build: Option<OperationResult>,
	/// Index teardown phase.
	pub(crate) index_remove: Option<OperationResult>,
	/// Timed scan legs in benchmark order (baseline → optional write-mix → indexed variants).
	pub(crate) runs: Vec<ScanRun>,
}

/// Column titles for the ASCII summary table ([`BenchmarkResult`]'s [`Display`] impl).
const HEADERS: [&str; 12] = [
	"Test",
	"Total time",
	"Mean",
	"Max",
	"99th",
	"95th",
	"Min",
	"OPS",
	"CPU",
	"Memory",
	"Reads",
	"Writes",
];

/// Extended columns for CSV export (extra quantiles + load averages).
const CSV_HEADERS: [&str; 22] = [
	"Test",
	"Total time",
	"Mean",
	"Max",
	"99th",
	"95th",
	"75th",
	"50th",
	"25th",
	"1st",
	"Min",
	"IQR",
	"OPS",
	"CPU_avg",
	"CPU_min",
	"CPU_max",
	"Memory_peak",
	"Memory_avg",
	"Reads",
	"Writes",
	"System load",
	"System load (1m/5m/15m)",
];

/// Placeholder cells when a phase was skipped or unsupported.
const SKIP: [&str; 11] = ["-"; 11];
/// Placeholder row for wide CSV rows.
const CSV_SKIP: [&str; 21] = ["-"; 21];

/// ASCII summary table matching [`HEADERS`] (used by CLI stdout).
impl Display for BenchmarkResult {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let mut table = Table::new();
		table
			.load_preset(UTF8_FULL)
			.apply_modifier(UTF8_ROUND_CORNERS)
			.set_content_arrangement(ContentArrangement::Dynamic);
		// Set the benchmark table header row
		let headers = HEADERS.map(|h| Cell::new(h).add_attribute(Attribute::Bold).fg(Color::Blue));
		table.set_header(headers);
		// Add the [C]reate results to the output
		if let Some(res) = &self.creates {
			table.add_row(res.output("[C]reate"));
		}
		// Add the [R]eads results to the output
		if let Some(res) = &self.reads {
			table.add_row(res.output("[R]ead"));
		}
		// Add the [U]pdates results to the output
		if let Some(res) = &self.updates {
			table.add_row(res.output("[U]pdate"));
		}
		// Add the [D]eletes results to the output
		if let Some(res) = &self.deletes {
			table.add_row(res.output("[D]elete"));
		}
		for scan in &self.scans {
			for run in scan.runs.iter().filter(|r| !r.indexed) {
				let label = scan_run_row_label(&scan.id, &scan.name, scan.samples, run);
				if let Some(res) = &run.result {
					table.add_row(res.output(label));
				} else {
					let mut cells = vec![label];
					cells.extend(SKIP.iter().map(|s| s.to_string()));
					table.add_row(cells);
				}
			}
			if scan.runs.iter().any(|r| r.indexed) {
				let label = format!("[I]ndex · {} · build", scan.id);
				if let Some(res) = &scan.index_build {
					table.add_row(res.output(label));
				} else {
					let mut cells = vec![label];
					cells.extend(SKIP.iter().map(|s| s.to_string()));
					table.add_row(cells);
				}
			}
			for run in scan.runs.iter().filter(|r| r.indexed) {
				let label = scan_run_row_label(&scan.id, &scan.name, scan.samples, run);
				if let Some(res) = &run.result {
					table.add_row(res.output(label));
				} else {
					let mut cells = vec![label];
					cells.extend(SKIP.iter().map(|s| s.to_string()));
					table.add_row(cells);
				}
			}
			if scan.runs.iter().any(|r| r.indexed) {
				let label = format!("[R]emoveIndex · {}", scan.id);
				if let Some(res) = &scan.index_remove {
					table.add_row(res.output(label));
				} else {
					let mut cells = vec![label];
					cells.extend(SKIP.iter().map(|s| s.to_string()));
					table.add_row(cells);
				}
			}
		}
		for (name, samples, groups, result) in &self.batches {
			let name = format!("[B]atch::{name} ({samples} batches of {groups})");
			if let Some(res) = &result {
				table.add_row(res.output(name));
			} else {
				let mut cells = vec![name];
				cells.extend(SKIP.iter().map(|s| s.to_string()));
				table.add_row(cells);
			}
		}
		// Right align the `CPU` column
		let column = table.column_mut(8).expect("The table needs at least 9 columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Memory` column
		let column = table.column_mut(9).expect("The table needs at least 10 columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Reads` column
		let column = table.column_mut(10).expect("The table needs at least 11 columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Writes` column
		let column = table.column_mut(11).expect("The table needs at least 12 columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Output the formatted table
		write!(f, "{table}")
	}
}

impl BenchmarkResult {
	/// Writes [`CSV_HEADERS`] and one row per completed phase to `path`.
	pub(crate) fn to_csv(&self, path: &str) -> Result<(), csv::Error> {
		let mut w = Writer::from_path(path)?;
		// Write headers
		w.write_record(CSV_HEADERS)?;
		// Add the [C]reate results to the output
		if let Some(res) = &self.creates {
			w.write_record(res.output_csv("[C]reate"))?;
		}
		// Add the [R]eads results to the output
		if let Some(res) = &self.reads {
			w.write_record(res.output_csv("[R]ead"))?;
		}
		// Add the [U]pdates results to the output
		if let Some(res) = &self.updates {
			w.write_record(res.output_csv("[U]pdate"))?;
		}
		// Add the [D]eletes results to the output
		if let Some(res) = &self.deletes {
			w.write_record(res.output_csv("[D]elete"))?;
		}
		// Add the [S]cans results to the output
		for scan in &self.scans {
			for run in scan.runs.iter().filter(|r| !r.indexed) {
				let label = scan_run_row_label(&scan.id, &scan.name, scan.samples, run);
				if let Some(res) = &run.result {
					w.write_record(res.output_csv(label))?;
				} else {
					let mut cells = vec![label];
					cells.extend(CSV_SKIP.iter().map(|s| s.to_string()));
					w.write_record(cells)?;
				}
			}
			if scan.runs.iter().any(|r| r.indexed) {
				let label = format!("[I]ndex · {} · build", scan.id);
				if let Some(res) = &scan.index_build {
					w.write_record(res.output_csv(label))?;
				} else {
					let mut cells = vec![label];
					cells.extend(CSV_SKIP.iter().map(|s| s.to_string()));
					w.write_record(cells)?;
				}
			}
			for run in scan.runs.iter().filter(|r| r.indexed) {
				let label = scan_run_row_label(&scan.id, &scan.name, scan.samples, run);
				if let Some(res) = &run.result {
					w.write_record(res.output_csv(label))?;
				} else {
					let mut cells = vec![label];
					cells.extend(CSV_SKIP.iter().map(|s| s.to_string()));
					w.write_record(cells)?;
				}
			}
			if scan.runs.iter().any(|r| r.indexed) {
				let label = format!("[R]emoveIndex · {}", scan.id);
				if let Some(res) = &scan.index_remove {
					w.write_record(res.output_csv(label))?;
				} else {
					let mut cells = vec![label];
					cells.extend(CSV_SKIP.iter().map(|s| s.to_string()));
					w.write_record(cells)?;
				}
			}
		}
		// Add the [B]atch results to the output
		for (name, samples, groups, result) in &self.batches {
			let name = format!("[B]atch::{name} ({samples} batches of {groups})");
			if let Some(res) = &result {
				w.write_record(res.output_csv(name))?;
			} else {
				let mut cells = vec![name];
				cells.extend(CSV_SKIP.iter().map(|s| s.to_string()));
				w.write_record(cells)?;
			}
		}
		// Ensure all data is flushed to the file
		w.flush()?;
		Ok(())
	}

	/// Renders interactive latency charts via [`crate::chart::generate_html`].
	pub(crate) fn to_html_charts(
		&self,
		path: &str,
		database_name: &str,
	) -> Result<(), std::io::Error> {
		let html = crate::chart::generate_html(self, database_name);
		std::fs::write(path, html)
	}
}

/// Rolling samples of CPU, memory, and disk counters from [`OperationMetric`]'s background task.
struct StatsCollector {
	/// Normalised CPU usage snapshots (%).
	cpu_samples: Vec<f32>,
	/// Resident set size samples (bytes).
	memory_samples: Vec<u64>,
	/// Monotonic disk read deltas since baseline.
	disk_read_samples: Vec<u64>,
	/// Monotonic disk write deltas since baseline.
	disk_write_samples: Vec<u64>,
}

impl StatsCollector {
	/// Empty collector before the first background sample.
	fn new() -> Self {
		Self {
			cpu_samples: Vec::new(),
			memory_samples: Vec::new(),
			disk_read_samples: Vec::new(),
			disk_write_samples: Vec::new(),
		}
	}

	/// Append one polling snapshot from [`OperationMetric::background_monitor`].
	fn add_sample(&mut self, cpu: f32, memory: u64, disk_reads: u64, disk_writes: u64) {
		self.cpu_samples.push(cpu);
		self.memory_samples.push(memory);
		self.disk_read_samples.push(disk_reads);
		self.disk_write_samples.push(disk_writes);
	}

	/// Mean CPU across polled samples.
	fn cpu_average(&self) -> f32 {
		if self.cpu_samples.is_empty() {
			0.0
		} else {
			self.cpu_samples.iter().sum::<f32>() / self.cpu_samples.len() as f32
		}
	}

	/// Minimum observed CPU sample.
	fn cpu_min(&self) -> f32 {
		if self.cpu_samples.is_empty() {
			0.0
		} else {
			self.cpu_samples.iter().copied().fold(f32::INFINITY, f32::min)
		}
	}

	/// Maximum observed CPU sample.
	fn cpu_max(&self) -> f32 {
		if self.cpu_samples.is_empty() {
			0.0
		} else {
			self.cpu_samples.iter().copied().fold(f32::NEG_INFINITY, f32::max)
		}
	}

	/// Mean resident memory across polled samples.
	fn memory_average(&self) -> u64 {
		if self.memory_samples.is_empty() {
			0
		} else {
			self.memory_samples.iter().sum::<u64>() / self.memory_samples.len() as u64
		}
	}

	/// Lowest resident memory sample.
	fn memory_min(&self) -> u64 {
		self.memory_samples.iter().copied().min().unwrap_or(0)
	}

	/// Highest resident memory sample (peak RSS-style).
	fn memory_peak(&self) -> u64 {
		self.memory_samples.iter().copied().max().unwrap_or(0)
	}

	/// Last cumulative disk read delta observed by the monitor.
	fn disk_read_total(&self) -> u64 {
		self.disk_read_samples.last().copied().unwrap_or(0)
	}

	/// Last cumulative disk write delta observed by the monitor.
	fn disk_write_total(&self) -> u64 {
		self.disk_write_samples.last().copied().unwrap_or(0)
	}
}

/// Live [`sysinfo`] handle plus background polling used to build an [`OperationResult`].
pub(super) struct OperationMetric {
	/// Shared system state for synchronous refresh calls.
	system: System,
	/// Process under test (benchmark worker or explicit `--pid`).
	pid: Pid,
	/// Logical operation count for OPS calculation.
	samples: u32,
	/// Wall-clock start for elapsed time.
	start_time: Instant,
	/// Disk counters before the phase (subtracted for delta I/O).
	initial_disk_usage: DiskUsage,
	/// Which process fields to refresh each poll.
	refresh_kind: ProcessRefreshKind,
	/// Samples filled by [`StatsCollector`].
	stats_collector: Arc<Mutex<StatsCollector>>,
	/// Tokio task driving [`OperationMetric::background_monitor`].
	monitor_handle: Option<JoinHandle<()>>,
}

impl OperationMetric {
	/// Starts periodic polling for the given PID (defaults to current process).
	pub(super) fn new(pid: Option<u32>, samples: u32) -> Self {
		// We collect the PID
		let pid = Pid::from(pid.unwrap_or_else(process::id) as usize);
		let refresh_kind = ProcessRefreshKind::nothing().with_memory().with_cpu().with_disk_usage();
		let system =
			System::new_with_specifics(RefreshKind::nothing().with_processes(refresh_kind));

		// Create stats collector
		let stats_collector = Arc::new(Mutex::new(StatsCollector::new()));

		let mut metric = Self {
			pid,
			samples,
			system,
			start_time: Instant::now(),
			initial_disk_usage: DiskUsage::default(),
			refresh_kind,
			stats_collector: stats_collector.clone(),
			monitor_handle: None,
		};

		// We collect the disk usage before the test, so we can subtract it from the count after test
		if let Some(process) = metric.collect_process() {
			metric.initial_disk_usage = process.disk_usage();
		}
		metric.start_time = Instant::now();

		// Spawn background monitoring task
		let monitor_handle = tokio::spawn(Self::background_monitor(
			pid,
			refresh_kind,
			metric.initial_disk_usage,
			stats_collector,
		));
		metric.monitor_handle = Some(monitor_handle);

		metric
	}

	/// Refreshes and returns the watched [`Process`], if still alive.
	fn collect_process(&mut self) -> Option<&Process> {
		self.system.refresh_processes_specifics(
			ProcessesToUpdate::Some(&[self.pid]),
			true,
			self.refresh_kind,
		);
		self.system.process(self.pid)
	}

	/// Polls CPU/memory/disk for `pid` on a fixed interval until the process exits.
	async fn background_monitor(
		pid: Pid,
		refresh_kind: ProcessRefreshKind,
		initial_disk_usage: DiskUsage,
		stats_collector: Arc<Mutex<StatsCollector>>,
	) {
		let mut system =
			System::new_with_specifics(RefreshKind::nothing().with_processes(refresh_kind));
		let mut interval = tokio::time::interval(Duration::from_millis(250));

		loop {
			interval.tick().await;

			// Refresh process stats
			system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh_kind);

			if let Some(process) = system.process(pid) {
				let cpu = process.cpu_usage() / num_cpus::get() as f32;
				let memory = process.memory();
				let disk_usage = process.disk_usage();

				// Calculate disk I/O relative to initial values
				let disk_reads =
					disk_usage.total_read_bytes.saturating_sub(initial_disk_usage.total_read_bytes);
				let disk_writes = disk_usage
					.total_written_bytes
					.saturating_sub(initial_disk_usage.total_written_bytes);

				// Add sample to collector
				if let Ok(mut collector) = stats_collector.lock() {
					collector.add_sample(cpu, memory, disk_reads, disk_writes);
				}
			} else {
				// Process no longer exists, stop monitoring
				break;
			}
		}
	}
}

#[derive(Serialize)]
/// Histogram-backed latency stats plus resource usage for one benchmark phase.
pub(crate) struct OperationResult {
	/// Mean latency (microseconds, HDR histogram centroids).
	mean: f64,
	/// Minimum observed latency (µs).
	min: u64,
	/// Maximum observed latency (µs).
	max: u64,
	/// 99th percentile latency (µs).
	q99: u64,
	/// 95th percentile latency (µs).
	q95: u64,
	/// 75th percentile latency (µs).
	q75: u64,
	/// Median latency (µs).
	q50: u64,
	/// 25th percentile latency (µs).
	q25: u64,
	/// 1st percentile latency (µs).
	q01: u64,
	/// Inter-quartile range (`q75 - q25`).
	iqr: u64,
	/// Throughput: `samples / elapsed_seconds`.
	ops: f64,
	/// Wall-clock duration of the whole phase.
	elapsed: Duration,
	/// Number of logical iterations aggregated into `histogram`.
	samples: u32,
	/// Snapshot CPU at end of phase (normalised by core count).
	cpu_usage: f32,
	/// Min / max / avg from polled samples when available.
	cpu_min: f32,
	cpu_max: f32,
	cpu_avg: f32,
	/// Resident memory at final sysinfo snapshot (bytes).
	used_memory: u64,
	/// Lowest RSS sample from background polling (bytes).
	memory_min: u64,
	/// Peak RSS from polling (bytes).
	memory_max: u64,
	/// Mean RSS across polls (bytes).
	memory_avg: u64,
	/// Delta disk bytes read/written attributed to the process.
	disk_usage: DiskUsage,
	/// Host load averages at end of phase.
	load_avg: LoadAvg,
}

impl OperationResult {
	/// Finalises histogram + [`OperationMetric`] snapshots into serialisable stats.
	pub(crate) fn new(mut metric: OperationMetric, histogram: Histogram<u64>) -> Self {
		let elapsed = metric.start_time.elapsed();

		// Stop the background monitor and wait for it to complete
		if let Some(handle) = metric.monitor_handle.take() {
			handle.abort();
		}

		// Collect final stats from the background monitor
		let (
			cpu_min,
			cpu_max,
			cpu_avg,
			memory_min,
			memory_max,
			memory_avg,
			final_disk_reads,
			final_disk_writes,
		) = if let Ok(collector) = metric.stats_collector.lock() {
			(
				collector.cpu_min(),
				collector.cpu_max(),
				collector.cpu_average(),
				collector.memory_min(),
				collector.memory_peak(),
				collector.memory_average(),
				collector.disk_read_total(),
				collector.disk_write_total(),
			)
		} else {
			(0.0, 0.0, 0.0, 0, 0, 0, 0, 0)
		};

		// Get final process stats
		let (mut cpu_usage, used_memory, mut disk_usage) =
			if let Some(process) = metric.collect_process() {
				(process.cpu_usage(), process.memory(), process.disk_usage())
			} else {
				(0.0, 0, DiskUsage::default())
			};

		// Subtract the initial disk usage
		disk_usage.total_written_bytes -= metric.initial_disk_usage.total_written_bytes;
		disk_usage.total_read_bytes -= metric.initial_disk_usage.total_read_bytes;

		// Use monitored disk I/O if available and greater than final snapshot
		if final_disk_writes > 0 {
			disk_usage.total_written_bytes = disk_usage.total_written_bytes.max(final_disk_writes);
		}
		if final_disk_reads > 0 {
			disk_usage.total_read_bytes = disk_usage.total_read_bytes.max(final_disk_reads);
		}

		// Divide the cpu usage by the number of cpus to get a normalized valued
		cpu_usage /= num_cpus::get() as f32;

		// Metrics
		let q75 = histogram.value_at_quantile(0.75);
		let q25 = histogram.value_at_quantile(0.25);
		let ops = metric.samples as f64 / (elapsed.as_nanos() as f64 / 1_000_000_000.0);

		Self {
			samples: metric.samples,
			mean: histogram.mean(),
			min: histogram.min(),
			max: histogram.max(),
			q99: histogram.value_at_quantile(0.99),
			q95: histogram.value_at_quantile(0.95),
			q75,
			q50: histogram.value_at_quantile(0.50),
			q25,
			q01: histogram.value_at_quantile(0.01),
			iqr: q75 - q25,
			ops,
			elapsed,
			cpu_usage,
			cpu_min,
			cpu_max,
			cpu_avg,
			used_memory,
			memory_min,
			memory_max,
			memory_avg,
			disk_usage,
			load_avg: System::load_average(),
		}
	}
	/// Output the total time for this operation
	pub(crate) fn total_time(&self) -> String {
		format_duration(self.elapsed)
	}
	/// Output this operation as a table row
	pub(crate) fn output<S>(&self, name: S) -> Vec<String>
	where
		S: ToString,
	{
		// Format CPU as "avg% (min%-max%)" or just "avg%" if no samples
		let cpu_display = if self.cpu_avg > 0.0 || self.cpu_min > 0.0 || self.cpu_max > 0.0 {
			format!("{:.2}% ({:.2}%-{:.2}%)", self.cpu_avg, self.cpu_min, self.cpu_max)
		} else {
			format!("{:.2}%", self.cpu_usage)
		};
		// Format Memory as "peak" only (without average)
		let memory_display = if self.memory_max > 0 {
			format!("{}", ByteSize(self.memory_max))
		} else {
			format!("{}", ByteSize(self.used_memory))
		};
		// Output the result as a table row
		vec![
			name.to_string(),
			format_duration(self.elapsed),
			format!("{:.2} ms", self.mean / 1000.0),
			format!("{:.2} ms", self.max as f64 / 1000.0),
			format!("{:.2} ms", self.q99 as f64 / 1000.0),
			format!("{:.2} ms", self.q95 as f64 / 1000.0),
			format!("{:.2} ms", self.min as f64 / 1000.0),
			format!("{:.2}", self.ops),
			cpu_display,
			memory_display,
			format!("{}", ByteSize(self.disk_usage.total_written_bytes)),
			format!("{}", ByteSize(self.disk_usage.total_read_bytes)),
		]
	}

	/// Output this operation as a CSV row with separate columns for stats
	pub(crate) fn output_csv<S>(&self, name: S) -> Vec<String>
	where
		S: ToString,
	{
		// Use monitored stats if available, otherwise fall back to final snapshot
		let cpu_avg = if self.cpu_avg > 0.0 {
			self.cpu_avg
		} else {
			self.cpu_usage
		};
		let cpu_min = if self.cpu_min > 0.0 {
			self.cpu_min
		} else {
			self.cpu_usage
		};
		let cpu_max = if self.cpu_max > 0.0 {
			self.cpu_max
		} else {
			self.cpu_usage
		};
		let memory_peak = if self.memory_max > 0 {
			self.memory_max
		} else {
			self.used_memory
		};
		let memory_avg = if self.memory_avg > 0 {
			self.memory_avg
		} else {
			self.used_memory
		};

		vec![
			name.to_string(),
			format_duration(self.elapsed),
			format!("{:.2} ms", self.mean / 1000.0),
			format!("{:.2} ms", self.max as f64 / 1000.0),
			format!("{:.2} ms", self.q99 as f64 / 1000.0),
			format!("{:.2} ms", self.q95 as f64 / 1000.0),
			format!("{:.2} ms", self.q75 as f64 / 1000.0),
			format!("{:.2} ms", self.q50 as f64 / 1000.0),
			format!("{:.2} ms", self.q25 as f64 / 1000.0),
			format!("{:.2} ms", self.q01 as f64 / 1000.0),
			format!("{:.2} ms", self.min as f64 / 1000.0),
			format!("{:.2} ms", self.iqr as f64 / 1000.0),
			format!("{:.2}", self.ops),
			format!("{:.2}", cpu_avg),
			format!("{:.2}", cpu_min),
			format!("{:.2}", cpu_max),
			format!("{}", memory_peak),
			format!("{}", memory_avg),
			format!("{}", self.disk_usage.total_written_bytes),
			format!("{}", self.disk_usage.total_read_bytes),
			format!("{:.2}", self.load_avg.one),
			format!(
				"{:.2}/{:.2}/{:.2}",
				self.load_avg.one, self.load_avg.five, self.load_avg.fifteen
			),
		]
	}

	/// Get the operations per second
	pub(crate) fn ops(&self) -> f64 {
		self.ops
	}

	/// Get the mean duration
	pub(crate) fn mean(&self) -> f64 {
		self.mean
	}

	/// Get the minimum duration
	pub(crate) fn min(&self) -> u64 {
		self.min
	}

	/// Get the maximum duration
	pub(crate) fn max(&self) -> u64 {
		self.max
	}

	/// Get the 99th percentile duration
	pub(crate) fn q99(&self) -> u64 {
		self.q99
	}

	/// Get the 95th percentile duration
	pub(crate) fn q95(&self) -> u64 {
		self.q95
	}

	/// Get the 75th percentile duration
	pub(crate) fn q75(&self) -> u64 {
		self.q75
	}

	/// Get the 50th percentile duration
	pub(crate) fn q50(&self) -> u64 {
		self.q50
	}

	/// Get the 25th percentile duration
	pub(crate) fn q25(&self) -> u64 {
		self.q25
	}

	/// Get the 1st percentile duration
	pub(crate) fn q01(&self) -> u64 {
		self.q01
	}

	/// Get the CPU usage
	pub(crate) fn cpu_usage(&self) -> f32 {
		self.cpu_usage
	}

	/// Get the used memory
	pub(crate) fn used_memory(&self) -> u64 {
		self.used_memory
	}

	/// Get the disk usage
	pub(crate) fn disk_usage(&self) -> &DiskUsage {
		&self.disk_usage
	}
}
