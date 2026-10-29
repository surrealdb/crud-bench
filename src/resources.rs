//! Which process the CPU, memory and disk columns of a result describe.
//!
//! The columns only mean something for a database when they sample the
//! process doing the database's work. For an embedded engine that is
//! crud-bench itself; for a networked database it is the server, which is a
//! different process and may run in a Docker container.
//!
//! A server may also be several processes. Postgres serves each connection
//! from its own backend, and every backend maps the same shared buffers, so
//! the memory of such a tree is not the sum of its resident set sizes.

use crate::docker::Container;
use anyhow::{Result, bail};
use serde::Serialize;
use std::fmt;
use std::io::ErrorKind;
use std::process;
use std::sync::Once;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// How the sampled process was chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResourceSource {
	/// Given with `--pid`.
	Pid,
	/// The main process of the Docker container crud-bench started.
	Docker,
	/// The crud-bench process itself, which is the database when it is embedded.
	Client,
}

/// The process the resource columns were sampled from, recorded in the result.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ResourceTarget {
	/// Process ID on the host.
	pub(crate) pid: u32,
	/// Process name as the operating system reports it.
	pub(crate) name: String,
	/// How the process was chosen.
	pub(crate) source: ResourceSource,
}

impl fmt::Display for ResourceTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let Self {
			pid,
			name,
			source,
		} = self;
		match source {
			ResourceSource::Pid => write!(f, "process {pid} ({name}) and its children, from --pid"),
			ResourceSource::Docker => {
				write!(f, "Docker container process {pid} ({name}) and its children")
			}
			ResourceSource::Client => write!(
				f,
				"crud-bench client process {pid} ({name}); this is the database only when it is embedded"
			),
		}
	}
}

/// Chooses the process to sample: an explicit `--pid` first, then the main
/// process of a container crud-bench started, else crud-bench itself.
pub(crate) fn resolve(
	pid: Option<u32>,
	container: Option<&Container>,
	endpoint: Option<&str>,
) -> Result<ResourceTarget> {
	if let Some(pid) = pid {
		let Some(name) = process_name(pid) else {
			bail!("--pid {pid} does not name a running process");
		};
		return Ok(ResourceTarget {
			pid,
			name,
			source: ResourceSource::Pid,
		});
	}
	if let Some(container) = container {
		match container.pid() {
			Ok(pid) => {
				if let Some(name) = process_name(pid) {
					return Ok(ResourceTarget {
						pid,
						name,
						source: ResourceSource::Docker,
					});
				}
				eprintln!("Warning: Docker container process {pid} is not visible from crud-bench");
			}
			Err(e) => eprintln!("Warning: Could not find the Docker container's process: {e}"),
		}
		eprintln!("Warning: Resource columns will measure the crud-bench client, not the database");
	} else if let Some(endpoint) = endpoint {
		eprintln!(
			"Warning: Resource columns measure the crud-bench client. If {endpoint} is a separate server, pass --pid <server pid> to measure it"
		);
	}
	let pid = process::id();
	Ok(ResourceTarget {
		pid,
		name: process_name(pid).unwrap_or_else(|| "crud-bench".to_string()),
		source: ResourceSource::Client,
	})
}

/// Name of a running process, or [`None`] when there is none with this ID.
fn process_name(pid: u32) -> Option<String> {
	let pid = Pid::from(pid as usize);
	let mut system = System::new();
	system.refresh_processes_specifics(
		ProcessesToUpdate::Some(&[pid]),
		true,
		ProcessRefreshKind::nothing(),
	);
	system.process(pid).map(|p| p.name().to_string_lossy().into_owned())
}

/// Memory of a tree of processes, with pages they share counted once (bytes).
///
/// The sum of each process's proportional set size (PSS) from
/// `/proc/<pid>/smaps_rollup`: a page shared by `n` processes counts `1/n` in
/// each of them, so the sum counts it once. Reading another user's
/// `smaps_rollup` needs root or `CAP_SYS_PTRACE`; without it, this falls back
/// to `/proc/<pid>/status`, which anyone can read, and sums each process's
/// private anonymous memory plus the largest shared and file-backed footprint
/// of any one process. That is a lower bound on the shared part, where the
/// sum of resident set sizes overcounts it once per process.
///
/// Returns [`None`] where `/proc` is not available.
pub(crate) fn tree_memory(pids: &[u32]) -> Option<u64> {
	if let Some(pss) = sum_fields(pids, "smaps_rollup", |text| proc_kb(text, "Pss:")) {
		return Some(pss);
	}
	static FALLBACK: Once = Once::new();
	FALLBACK.call_once(|| {
		eprintln!(
			"Warning: Cannot read the server's /proc/<pid>/smaps_rollup. The memory column counts private memory per process plus the largest shared footprint once; run crud-bench as root or with CAP_SYS_PTRACE for exact PSS"
		);
	});
	let mut private = 0;
	let mut shared = 0;
	for pid in pids {
		let Some(text) = read_proc(*pid, "status")? else {
			continue;
		};
		private += proc_kb(&text, "RssAnon:")?;
		shared = shared.max(proc_kb(&text, "RssFile:")? + proc_kb(&text, "RssShmem:")?);
	}
	Some(private + shared)
}

/// Sums one field over the `/proc/<pid>/<file>` of every process, skipping any
/// that has exited. [`None`] if any file cannot be read or parsed.
fn sum_fields(pids: &[u32], file: &str, field: impl Fn(&str) -> Option<u64>) -> Option<u64> {
	let mut total = 0;
	for pid in pids {
		if let Some(text) = read_proc(*pid, file)? {
			total += field(&text)?;
		}
	}
	Some(total)
}

/// Reads `/proc/<pid>/<file>`: `Some(None)` if the process has exited since
/// it was listed, [`None`] on any other error, such as a denied permission.
fn read_proc(pid: u32, file: &str) -> Option<Option<String>> {
	match std::fs::read_to_string(format!("/proc/{pid}/{file}")) {
		Ok(text) => Some(Some(text)),
		Err(e) if e.kind() == ErrorKind::NotFound => Some(None),
		// ESRCH: the process exited while it was being read
		Err(e) if e.raw_os_error() == Some(3) => Some(None),
		Err(_) => None,
	}
}

/// Value of a `Name:   1234 kB` line in a `/proc` file, in bytes.
fn proc_kb(text: &str, name: &str) -> Option<u64> {
	let line = text.lines().find(|line| line.starts_with(name))?;
	let kb: u64 = line[name.len()..].split_whitespace().next()?.parse().ok()?;
	Some(kb * 1024)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn explicit_pid_is_used() {
		let own = process::id();
		let target = resolve(Some(own), None, Some("ws://127.0.0.1:8000")).unwrap();
		assert_eq!(target.pid, own);
		assert_eq!(target.source, ResourceSource::Pid);
		assert!(!target.name.is_empty());
	}

	#[test]
	fn missing_pid_is_refused() {
		// Above the kernel's PID ceiling (4194304), so never a live process.
		let err = resolve(Some(u32::MAX - 1), None, None).unwrap_err();
		assert!(err.to_string().contains("does not name a running process"));
	}

	#[test]
	fn defaults_to_the_client() {
		let target = resolve(None, None, None).unwrap();
		assert_eq!(target.pid, process::id());
		assert_eq!(target.source, ResourceSource::Client);
	}

	#[test]
	fn proc_fields_are_parsed_in_bytes() {
		let rollup = "55d0c0000000-7ffd00000000 ---p 00000000 00:00 0 [rollup]\nRss:   10240 kB\nPss:    2048 kB\nPss_Anon:   1024 kB\n";
		assert_eq!(proc_kb(rollup, "Pss:"), Some(2048 * 1024));
		assert_eq!(proc_kb(rollup, "Rss:"), Some(10240 * 1024));
		assert_eq!(proc_kb(rollup, "Swap:"), None);
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn shared_memory_is_counted_once() {
		// Our own smaps_rollup is readable, so this measures PSS
		let own = process::id();
		let rss = |pid: u32| {
			let text = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
			proc_kb(&text, "VmRSS:").unwrap()
		};
		let mut children: Vec<_> =
			(0..4).map(|_| process::Command::new("sleep").arg("30").spawn().unwrap()).collect();
		let pids: Vec<u32> = std::iter::once(own).chain(children.iter().map(|c| c.id())).collect();
		let tree = tree_memory(&pids).unwrap();
		let summed: u64 = pids.iter().map(|p| rss(*p)).sum();
		for child in &mut children {
			child.kill().unwrap();
			child.wait().unwrap();
		}
		// The sleeps share their binary and libraries, which RSS counts per process
		assert!(tree > 0);
		assert!(tree < summed, "PSS {tree} should be below summed RSS {summed}");
		// A process that has exited is skipped, not an error
		assert!(tree_memory(&[own, u32::MAX - 1]).is_some());
	}

	#[test]
	fn target_is_named_in_the_output() {
		let target = ResourceTarget {
			pid: 42,
			name: "surreal".to_string(),
			source: ResourceSource::Pid,
		};
		assert_eq!(target.to_string(), "process 42 (surreal) and its children, from --pid");
		let json = serde_json::to_value(&target).unwrap();
		assert_eq!(json["source"], "pid");
	}
}
