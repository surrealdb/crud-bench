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
use std::time::{Duration, Instant};
use sysinfo::{
	DiskUsage, LoadAvg, Pid, Process, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System,
};

#[derive(Serialize)]
pub(crate) struct BenchmarkResult {
	pub(crate) creates: Option<OperationResult>,
	pub(crate) reads: Option<OperationResult>,
	pub(crate) updates: Option<OperationResult>,
	pub(crate) scans: Vec<(String, u32, Option<OperationResult>)>,
	pub(crate) batches: Vec<(String, u32, usize, Option<OperationResult>)>,
	pub(crate) deletes: Option<OperationResult>,
	pub(crate) sample: Value,
}

const HEADERS: [&str; 18] = [
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
	"CPU",
	"Memory",
	"Reads",
	"Writes",
	"System load",
];

const SKIP: [&str; 17] = ["-"; 17];

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
		for (name, samples, result) in &self.scans {
			let name = format!("[S]can::{name} ({samples})");
			if let Some(res) = &result {
				table.add_row(res.output(name));
			} else {
				let mut cells = vec![name];
				cells.extend(SKIP.iter().map(|s| s.to_string()));
				table.add_row(cells);
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
		let column = table.column_mut(13).expect("The table needs at least 14 columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Memory` column
		let column = table.column_mut(14).expect("The table needs at least 15 columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Reads` column
		let column = table.column_mut(15).expect("The table needs at least 16 columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Writes` column
		let column = table.column_mut(16).expect("The table needs at least 17 columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Output the formatted table
		write!(f, "{table}")
	}
}

impl BenchmarkResult {
	pub(crate) fn to_csv(&self, path: &str) -> Result<(), csv::Error> {
		let mut w = Writer::from_path(path)?;

		// Write headers
		w.write_record(HEADERS)?;

		// Write rows
		// Add the [C]reate results to the output
		if let Some(res) = &self.creates {
			w.write_record(res.output("[C]reate"))?;
		}
		// Add the [R]eads results to the output
		if let Some(res) = &self.reads {
			w.write_record(res.output("[R]ead"))?;
		}
		// Add the [U]pdates results to the output
		if let Some(res) = &self.updates {
			w.write_record(res.output("[U]pdate"))?;
		}
		// Add the [D]eletes results to the output
		if let Some(res) = &self.deletes {
			w.write_record(res.output("[D]elete"))?;
		}
		// Add the [S]cans results to the output
		for (name, samples, result) in &self.scans {
			let name = format!("[S]can::{name} ({samples})");
			if let Some(res) = &result {
				w.write_record(res.output(name))?;
			} else {
				let mut cells = vec![name];
				cells.extend(SKIP.iter().map(|s| s.to_string()));
				w.write_record(cells)?;
			}
		}
		// Add the [B]atch results to the output
		for (name, samples, groups, result) in &self.batches {
			let name = format!("[B]atch::{name} ({samples} batches of {groups})");
			if let Some(res) = &result {
				w.write_record(res.output(name))?;
			} else {
				let mut cells = vec![name];
				cells.extend(SKIP.iter().map(|s| s.to_string()));
				w.write_record(cells)?;
			}
		}
		// Ensure all data is flushed to the file
		w.flush()?;
		Ok(())
	}
}

pub(super) struct OperationMetric {
	system: System,
	pid: Pid,
	samples: u32,
	start_time: Instant,
	initial_disk_usage: DiskUsage,
	refresh_kind: ProcessRefreshKind,
}

impl OperationMetric {
	pub(super) fn new(pid: Option<u32>, samples: u32) -> Self {
		// We collect the PID
		let pid = Pid::from(pid.unwrap_or_else(process::id) as usize);
		let refresh_kind = ProcessRefreshKind::nothing().with_memory().with_cpu().with_disk_usage();
		let system =
			System::new_with_specifics(RefreshKind::nothing().with_processes(refresh_kind));
		let mut metric = Self {
			pid,
			samples,
			system,
			start_time: Instant::now(),
			initial_disk_usage: DiskUsage::default(),
			refresh_kind,
		};
		// We collect the disk usage before the test, so we can't subtract it from the count after test
		if let Some(process) = metric.collect_process() {
			metric.initial_disk_usage = process.disk_usage();
		}
		metric.start_time = Instant::now();
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
	used_memory: u64,
	disk_usage: DiskUsage,
	load_avg: LoadAvg,
}

impl OperationResult {
	/// Create a new operataion result
	pub(crate) fn new(mut metric: OperationMetric, histogram: Histogram<u64>) -> Self {
		let elapsed = metric.start_time.elapsed();
		let (mut cpu_usage, used_memory, mut disk_usage) =
			if let Some(process) = metric.collect_process() {
				(process.cpu_usage(), process.memory(), process.disk_usage())
			} else {
				(0.0, 0, DiskUsage::default())
			};
		// Subtract the initial disk usage
		disk_usage.total_written_bytes -= metric.initial_disk_usage.total_written_bytes;
		disk_usage.total_read_bytes -= metric.initial_disk_usage.total_read_bytes;
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
			used_memory,
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
			format!("{:.2}%", self.cpu_usage),
			format!("{}", ByteSize(self.used_memory)),
			format!("{}", ByteSize(self.disk_usage.total_written_bytes)),
			format!("{}", ByteSize(self.disk_usage.total_read_bytes)),
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
