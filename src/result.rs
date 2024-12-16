use bytesize::ByteSize;
use hdrhistogram::Histogram;
use std::fmt::{Display, Formatter};
use std::process;
use std::time::{Duration, Instant};
use sysinfo::{
	DiskUsage, LoadAvg, Pid, Process, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System,
};

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
	process_name: String,
	process_pid: Pid,
}

impl OperationResult {
	pub(super) fn new(mut metric: OperationMetric, histogram: Histogram<u64>) -> Self {
		let elapsed = metric.start_time.elapsed();
		let (mut cpu_usage, used_memory, mut disk_usage, process_name) =
			if let Some(process) = metric.collect_process() {
				(
					process.cpu_usage(),
					process.memory(),
					process.disk_usage(),
					process.name().to_string_lossy().to_string(),
				)
			} else {
				(0.0, 0, DiskUsage::default(), "-".to_string())
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
			process_name,
			process_pid: metric.pid,
		}
	}
}

impl Display for OperationResult {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"total: {} - avg: {:.2} ms - 99%: {:.2} ms - 95%: {:.2} ms - cpu: {:.2}% - memory: {} - writes: {} - reads: {} - load avg: {:.2}/{:.2}/{:.2} - process: {}/{}",
			format_duration(self.elapsed),
			self.histogram.mean() / 1000.0,
			self.histogram.value_at_quantile(0.99) as f64 / 1000.0,
			self.histogram.value_at_quantile(0.95) as f64 / 1000.0,
			self.cpu_usage,
			ByteSize(self.used_memory),
			ByteSize(self.disk_usage.total_written_bytes),
			ByteSize(self.disk_usage.total_read_bytes),
			self.load_avg.one,
			self.load_avg.five,
			self.load_avg.fifteen,
			self.process_name,
			self.process_pid
		)
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
