use bytesize::ByteSize;
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
	elapsed: Duration,
	cpu_usage: f32,
	used_memory: u64,
	disk_usage: DiskUsage,
	load_avg: LoadAvg,
	process_name: String,
	process_pid: Pid,
}

impl OperationResult {
	pub(super) fn new(mut metric: OperationMetric) -> Self {
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
			"{:?} - cpu: {:.2}% - memory: {} - writes: {} - reads: {} - load avg: {:.2}/{:.2}/{:.2} - process: {}/{}",
			self.elapsed,
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
