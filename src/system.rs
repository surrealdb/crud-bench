use serde::{Deserialize, Serialize};
use sysinfo::System;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
	pub timestamp: i64,
	pub hostname: String,
	pub os_name: String,
	pub os_version: String,
	pub kernel_version: String,
	pub cpu_cores: usize,
	pub cpu_physical_cores: usize,
	pub cpu_arch: String,
	pub total_memory: u64,
	pub available_memory: u64,
}

pub fn collect() -> SystemInfo {
	SystemInfo::collect()
}

impl SystemInfo {
	pub fn collect() -> Self {
		// Create a new system instance
		let mut sys = System::new_all();
		// Refresh the system information
		sys.refresh_all();
		// Get the system details
		let timestamp = chrono::Utc::now().timestamp();
		let hostname = System::host_name().unwrap_or_else(|| "unknown".to_string());
		let os_name = System::name().unwrap_or_else(|| "unknown".to_string());
		let os_version = System::os_version().unwrap_or_else(|| "unknown".to_string());
		let kernel_version = System::kernel_version().unwrap_or_else(|| "unknown".to_string());
		// Get the CPU details
		let cpu_arch = System::cpu_arch();
		let cpu_cores = num_cpus::get();
		let cpu_physical_cores = num_cpus::get_physical();
		// Get the memory details
		let total_memory = sys.total_memory();
		let available_memory = sys.available_memory();
		// Return the system information
		Self {
			timestamp,
			hostname,
			os_name,
			os_version,
			kernel_version,
			cpu_cores,
			cpu_physical_cores,
			cpu_arch,
			total_memory,
			available_memory,
		}
	}
}
