use sysinfo::System;

/// System memory information for database optimization
pub(crate) struct Config {
	/// Recommended memory allocation for database cache/buffer pools in GB
	pub cache_gb: u64,
}

impl Config {
	/// Get system memory information and calculate recommended database memory allocation
	pub fn new() -> Self {
		// Load the system attributed
		let system = System::new_all();
		// Get the total system memory
		let total_memory = system.total_memory();
		// Convert to GB for easier calculations
		let total_gb = total_memory / (1024 * 1024 * 1024);
		// Use ~75% of total memory for database cache
		let cache_gb = if total_gb <= 8 {
			// Small systems: use ~50% of memory
			(total_gb / 2).max(1)
		} else if total_gb <= 32 {
			// Medium systems: use ~60% of memory
			(total_gb * 3 / 5).max(4)
		} else {
			// Large systems: use ~75% of memory, but leave at least 8GB for OS
			(total_gb * 3 / 4).clamp(8, total_gb - 8)
		};
		// Return configuration
		Self {
			cache_gb,
		}
	}
}
