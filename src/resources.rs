//! Which process the CPU, memory and disk columns of a result describe.
//!
//! The columns only mean something for a database when they sample the
//! process doing the database's work. For an embedded engine that is
//! crud-bench itself; for a networked database it is the server, which is a
//! different process and may run in a Docker container.

use crate::docker::Container;
use anyhow::{Result, bail};
use serde::Serialize;
use std::fmt;
use std::process;
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
