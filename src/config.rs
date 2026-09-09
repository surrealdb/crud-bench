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
	/// Corpus seed. When set, generated row content is a pure function of this
	/// seed and the sample index, so a run is reproducible and the corpus can
	/// be reconstructed for vector-search ground truth without reading rows
	/// back. Omitted means the historical behaviour: values drawn from entropy.
	#[serde(default)]
	pub(crate) seed: Option<u64>,
}

pub(crate) fn load_bench_toml(path: &str) -> Result<BenchToml> {
	let text = std::fs::read_to_string(path)
		.with_context(|| format!("Failed to read config file '{path}'"))?;
	toml::from_str(&text).with_context(|| format!("Failed to parse benchmark TOML '{path}'"))
}
