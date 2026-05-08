//! Single-file benchmark workload definition (`config/bench.toml`).

use crate::{BatchOperation, ScanSpec};
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub(crate) struct BenchToml {
	#[serde(default)]
	pub(crate) scans: Vec<ScanSpec>,
	#[serde(default)]
	pub(crate) batches: Vec<BatchOperation>,
	pub(crate) value: Value,
}

pub(crate) fn load_bench_toml(path: &str) -> Result<BenchToml> {
	let text = std::fs::read_to_string(path)
		.with_context(|| format!("Failed to read config file '{path}'"))?;
	toml::from_str(&text).with_context(|| format!("Failed to parse benchmark TOML '{path}'"))
}
