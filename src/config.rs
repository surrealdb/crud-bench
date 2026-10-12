//! Single-file benchmark workload definition (`config/bench.toml`).

use crate::{BatchOperation, ScanSpec};
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[cfg(test)]
mod test {
	/// Every shipped config must parse. With `deny_unknown_fields` this is also
	/// the guard that a key has not drifted to the wrong nesting level: before
	/// it existed, `samples` sat on the `count_count_idx` scan in two of these
	/// files, was silently dropped, and that leg ran `--samples` iterations
	/// instead of the configured count — against a `count` leg that honoured
	/// it, so the pair being compared were never running the same workload.
	#[test]
	fn every_shipped_config_parses() {
		let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config");
		let mut checked = 0;
		for entry in std::fs::read_dir(&dir).expect("config dir") {
			let path = entry.expect("dir entry").path();
			if path.extension().and_then(|e| e.to_str()) != Some("toml") {
				continue;
			}
			let name = path.display().to_string();
			super::load_bench_toml(&name)
				.unwrap_or_else(|e| panic!("{name} failed to parse: {e:#}"));
			checked += 1;
		}
		assert!(checked >= 4, "expected the shipped configs, found {checked}");
	}

	/// Parsing is not enough for a vector config: a filter naming a column the
	/// value template does not have parses perfectly and fails at startup, and
	/// a KNN scan pointed at a non-vector column does the same. Put every
	/// shipped config through the checks `run` performs before a container is
	/// started.
	#[test]
	fn every_shipped_config_validates() {
		use crate::valueprovider::ValueProvider;
		let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config");
		for entry in std::fs::read_dir(&dir).expect("config dir") {
			let path = entry.expect("dir entry").path();
			if path.extension().and_then(|e| e.to_str()) != Some("toml") {
				continue;
			}
			let name = path.display().to_string();
			let toml = super::load_bench_toml(&name).expect("parses");
			let value_json = serde_json::to_string(&toml.value).expect("template is JSON");
			let vp = ValueProvider::new(&value_json)
				.unwrap_or_else(|e| panic!("{name}: value template rejected: {e:#}"));
			let scans = crate::expand_scan_specs(toml.scans)
				.unwrap_or_else(|e| panic!("{name}: scan expansion failed: {e:#}"));
			crate::validate_scan_index_ids(&scans)
				.unwrap_or_else(|e| panic!("{name}: scan validation failed: {e:#}"));
			crate::collect_vector_filter_fields(&scans, &vp.columns())
				.unwrap_or_else(|e| panic!("{name}: vector filters rejected: {e:#}"));
			// A vector scan without a corpus seed can never report recall, and
			// that is the axis these configs exist to measure.
			if scans.iter().any(|s| s.vector_query.is_some()) {
				assert!(toml.seed.is_some(), "{name}: a vector config must set `seed`");
			}
		}
	}
}
