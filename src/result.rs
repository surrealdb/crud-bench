use bytesize::ByteSize;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Attribute, Cell, CellAlignment, Color, ContentArrangement, Table};
use hdrhistogram::Histogram;
use serde_json::Value;
use std::fmt::{Display, Formatter};
use std::process;
use std::time::{Duration, Instant};
use sysinfo::{
	DiskUsage, LoadAvg, Pid, Process, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System,
};

pub(crate) struct BenchmarkResult {
	pub(crate) creates: Option<OperationResult>,
	pub(crate) reads: Option<OperationResult>,
	pub(crate) updates: Option<OperationResult>,
	pub(crate) scans: Vec<(String, u32, Option<OperationResult>)>,
	pub(crate) deletes: Option<OperationResult>,
	pub(crate) sample: Value,
}

impl Display for BenchmarkResult {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let mut table = Table::new();
		table
			.load_preset(UTF8_FULL)
			.apply_modifier(UTF8_ROUND_CORNERS)
			.set_content_arrangement(ContentArrangement::Dynamic);
		// Set the benchmark table header row
		table.set_header(vec![
			Cell::new("Test").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("Total time").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("Mean").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("99th").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("95th").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("50th").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("1st").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("CPU").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("Memory").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("Reads").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("Writes").add_attribute(Attribute::Bold).fg(Color::Blue),
			Cell::new("System load").add_attribute(Attribute::Bold).fg(Color::Blue),
		]);
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
		for (name, samples, result) in &self.scans {
			if let Some(res) = &result {
				table.add_row(res.output(format!("[S]can::{name} ({samples})")));
			} else {
				table.add_row(vec![
					format!("[S]can::{name} ({samples})"),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
					"-".to_string(),
				]);
			}
		}
		// Add the [D]eletes results to the output
		if let Some(res) = &self.deletes {
			table.add_row(res.output("[D]elete"));
		}
		// Right align the `CPU` column
		let column = table.column_mut(7).expect("Our table has three columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Memory` column
		let column = table.column_mut(8).expect("Our table has three columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Reads` column
		let column = table.column_mut(9).expect("Our table has three columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Right align the `Writes` column
		let column = table.column_mut(10).expect("Our table has three columns");
		column.set_cell_alignment(CellAlignment::Right);
		// Output the formatted table
		write!(f, "{table}")
	}
}

pub(super) struct OperationMetric {
	system: System,
	pid: Pid,
	start_time: Instant,
	initial_disk_usage: DiskUsage,
	refresh_kind: ProcessRefreshKind,
}

impl OperationMetric {
	pub(super) fn new(pid: Option<u32>) -> Self {
		// We collect the PID
		let pid = Pid::from(pid.unwrap_or_else(process::id) as usize);
		let refresh_kind = ProcessRefreshKind::nothing().with_memory().with_cpu().with_disk_usage();
		let system =
			System::new_with_specifics(RefreshKind::nothing().with_processes(refresh_kind));
		let mut metric = Self {
			pid,
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

pub(super) struct OperationResult {
	histogram: Histogram<u64>,
	elapsed: Duration,
	cpu_usage: f32,
	used_memory: u64,
	disk_usage: DiskUsage,
	load_avg: LoadAvg,
}

impl OperationResult {
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
		Self {
			histogram,
			elapsed,
			cpu_usage,
			used_memory,
			disk_usage,
			load_avg: System::load_average(),
		}
	}
	pub(crate) fn output<S>(&self, name: S) -> Vec<String>
	where
		S: ToString,
	{
		vec![
			name.to_string(),
			format_duration(self.elapsed),
			format!("{:.2} ms", self.histogram.mean() / 1000.0),
			format!("{:.2} ms", self.histogram.value_at_quantile(0.99) as f64 / 1000.0),
			format!("{:.2} ms", self.histogram.value_at_quantile(0.95) as f64 / 1000.0),
			format!("{:.2} ms", self.histogram.value_at_quantile(0.50) as f64 / 1000.0),
			format!("{:.2} ms", self.histogram.value_at_quantile(0.01) as f64 / 1000.0),
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
		format!("{}d {}h", days, hours)
	} else if secs >= 3600 {
		let hours = secs / 3600;
		let minutes = (secs % 3600) / 60;
		format!("{}h {}m", hours, minutes)
	} else if secs >= 60 {
		let minutes = secs / 60;
		let seconds = secs % 60;
		format!("{}m {}s", minutes, seconds)
	} else if secs > 0 {
		let seconds = secs;
		let millis = duration.subsec_millis();
		format!("{}s {}ms", seconds, millis)
	} else if duration.subsec_millis() > 0 {
		let millis = duration.subsec_millis();
		let micros = duration.subsec_micros() % 1000;
		format!("{}ms {}µs", millis, micros)
	} else if duration.subsec_micros() > 0 {
		let micros = duration.subsec_micros();
		let nanos = duration.subsec_nanos() % 1000;
		format!("{}µs {}ns", micros, nanos)
	} else {
		format!("{}ns", duration.subsec_nanos())
	}
}
