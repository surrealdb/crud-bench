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

#[derive(Serialize)]
pub(crate) struct BenchmarkResult {
	pub(crate) creates: Option<OperationResult>,
	pub(crate) reads: Option<OperationResult>,
	pub(crate) updates: Option<OperationResult>,
	pub(crate) scans: Vec<ScanResult>,
	pub(crate) batches: Vec<(String, u32, usize, Option<OperationResult>)>,
	pub(crate) deletes: Option<OperationResult>,
	pub(crate) sample: Value,
}

#[derive(Serialize)]
pub(crate) struct ScanResult {
	pub(crate) name: String,
	pub(crate) samples: u32,
	pub(crate) without_index: Option<OperationResult>,
	pub(crate) index_build: Option<OperationResult>,
	pub(crate) with_index: Option<OperationResult>,
	pub(crate) has_index_spec: bool,
}

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

const SKIP: [&str; 11] = ["-"; 11];
const CSV_SKIP: [&str; 21] = ["-"; 21];

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
			// Scan without index
			let label = format!("[S]can::{} ({})", scan.name, scan.samples);
			if let Some(res) = &scan.without_index {
				table.add_row(res.output(label));
			} else {
				let mut cells = vec![label];
				cells.extend(SKIP.iter().map(|s| s.to_string()));
				table.add_row(cells);
			}
			// Index build (only for indexed scans)
			if scan.has_index_spec {
				let label = format!("[I]ndex::{}", scan.name);
				if let Some(res) = &scan.index_build {
					table.add_row(res.output(label));
				} else {
					let mut cells = vec![label];
					cells.extend(SKIP.iter().map(|s| s.to_string()));
					table.add_row(cells);
				}
			}
			// Scan with index (only for indexed scans)
			if scan.has_index_spec {
				let label = format!("[S]can::{}::indexed ({})", scan.name, scan.samples);
				if let Some(res) = &scan.with_index {
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
			// Scan without index
			let label = format!("[S]can::{} ({})", scan.name, scan.samples);
			if let Some(res) = &scan.without_index {
				w.write_record(res.output_csv(label))?;
			} else {
				let mut cells = vec![label];
				cells.extend(CSV_SKIP.iter().map(|s| s.to_string()));
				w.write_record(cells)?;
			}
			// Index build (only for indexed scans)
			if scan.has_index_spec {
				let label = format!("[I]ndex::{}", scan.name);
				if let Some(res) = &scan.index_build {
					w.write_record(res.output_csv(label))?;
				} else {
					let mut cells = vec![label];
					cells.extend(CSV_SKIP.iter().map(|s| s.to_string()));
					w.write_record(cells)?;
				}
			}
			// Scan with index (only for indexed scans)
			if scan.has_index_spec {
				let label = format!("[S]can::{}::indexed ({})", scan.name, scan.samples);
				if let Some(res) = &scan.with_index {
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
}

/// Stats collector for continuous monitoring during benchmark operations
struct StatsCollector {
	cpu_samples: Vec<f32>,
	memory_samples: Vec<u64>,
	disk_read_samples: Vec<u64>,
	disk_write_samples: Vec<u64>,
}

impl StatsCollector {
	fn new() -> Self {
		Self {
			cpu_samples: Vec::new(),
			memory_samples: Vec::new(),
			disk_read_samples: Vec::new(),
			disk_write_samples: Vec::new(),
		}
	}

	fn add_sample(&mut self, cpu: f32, memory: u64, disk_reads: u64, disk_writes: u64) {
		self.cpu_samples.push(cpu);
		self.memory_samples.push(memory);
		self.disk_read_samples.push(disk_reads);
		self.disk_write_samples.push(disk_writes);
	}

	fn cpu_average(&self) -> f32 {
		if self.cpu_samples.is_empty() {
			0.0
		} else {
			self.cpu_samples.iter().sum::<f32>() / self.cpu_samples.len() as f32
		}
	}

	fn cpu_min(&self) -> f32 {
		if self.cpu_samples.is_empty() {
			0.0
		} else {
			self.cpu_samples.iter().copied().fold(f32::INFINITY, f32::min)
		}
	}

	fn cpu_max(&self) -> f32 {
		if self.cpu_samples.is_empty() {
			0.0
		} else {
			self.cpu_samples.iter().copied().fold(f32::NEG_INFINITY, f32::max)
		}
	}

	fn memory_average(&self) -> u64 {
		if self.memory_samples.is_empty() {
			0
		} else {
			self.memory_samples.iter().sum::<u64>() / self.memory_samples.len() as u64
		}
	}

	fn memory_min(&self) -> u64 {
		self.memory_samples.iter().copied().min().unwrap_or(0)
	}

	fn memory_peak(&self) -> u64 {
		self.memory_samples.iter().copied().max().unwrap_or(0)
	}

	fn disk_read_total(&self) -> u64 {
		self.disk_read_samples.last().copied().unwrap_or(0)
	}

	fn disk_write_total(&self) -> u64 {
		self.disk_write_samples.last().copied().unwrap_or(0)
	}
}

pub(super) struct OperationMetric {
	system: System,
	pid: Pid,
	samples: u32,
	start_time: Instant,
	initial_disk_usage: DiskUsage,
	refresh_kind: ProcessRefreshKind,
	stats_collector: Arc<Mutex<StatsCollector>>,
	monitor_handle: Option<JoinHandle<()>>,
}

impl OperationMetric {
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

	fn collect_process(&mut self) -> Option<&Process> {
		self.system.refresh_processes_specifics(
			ProcessesToUpdate::Some(&[self.pid]),
			true,
			self.refresh_kind,
		);
		self.system.process(self.pid)
	}

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
pub(super) struct OperationResult {
	mean: f64,
	min: u64,
	max: u64,
	q99: u64,
	q95: u64,
	q75: u64,
	q50: u64,
	q25: u64,
	q01: u64,
	iqr: u64,
	ops: f64,
	elapsed: Duration,
	samples: u32,
	cpu_usage: f32,
	cpu_min: f32,
	cpu_max: f32,
	cpu_avg: f32,
	used_memory: u64,
	memory_min: u64,
	memory_max: u64,
	memory_avg: u64,
	disk_usage: DiskUsage,
	load_avg: LoadAvg,
}

impl OperationResult {
	/// Create a new operataion result
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
}

fn format_duration(duration: Duration) -> String {
	let secs = duration.as_secs();
	if secs >= 86400 {
		let days = secs / 86400;
		let hours = (secs % 86400) / 3600;
		format!("{days}d {hours}h")
	} else if secs >= 3600 {
		let hours = secs / 3600;
		let minutes = (secs % 3600) / 60;
		format!("{hours}h {minutes}m")
	} else if secs >= 60 {
		let minutes = secs / 60;
		let seconds = secs % 60;
		format!("{minutes}m {seconds}s")
	} else if secs > 0 {
		let seconds = secs;
		let millis = duration.subsec_millis();
		format!("{seconds}s {millis}ms")
	} else if duration.subsec_millis() > 0 {
		let millis = duration.subsec_millis();
		let micros = duration.subsec_micros() % 1000;
		format!("{millis}ms {micros}µs")
	} else if duration.subsec_micros() > 0 {
		let micros = duration.subsec_micros();
		let nanos = duration.subsec_nanos() % 1000;
		format!("{micros}µs {nanos}ns")
	} else {
		format!("{}ns", duration.subsec_nanos())
	}
}
