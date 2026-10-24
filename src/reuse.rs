//! Splits a benchmark into a load run and any number of query runs.
//!
//! A load run creates and updates the dataset and leaves it on disk. A query
//! run opens that dataset without touching it and runs only the repeatable
//! phases (reads and scans), so a scan can be measured repeatedly without
//! paying for the load each time.
//!
//! The dataset a query run sees is only meaningful if it is the one the run
//! was configured for, so a load run writes a [`Manifest`] of everything that
//! shapes the data, and a query run refuses to start against a manifest that
//! differs from its own configuration.

use crate::vectorfilter::FilterField;
use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// File inside the data directory recording how the dataset was loaded.
const MANIFEST_FILE: &str = "manifest.json";

/// Bumped when the manifest's meaning changes, so an older file is rejected
/// instead of being compared field by field against a different contract.
const MANIFEST_VERSION: u32 = 1;

/// Which phases a run executes and whether it keeps or reuses the dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[value(rename_all = "lower")]
pub(crate) enum RunMode {
	/// Load, query and delete in one run, leaving nothing behind.
	All,
	/// Create and update the dataset, then keep it for later query runs.
	Load,
	/// Run reads and scans against a dataset an earlier load run left behind.
	Query,
}

impl RunMode {
	/// Whether the store is emptied before the run starts. A query run must
	/// open the data a load run left.
	pub(crate) fn wipes_on_setup(self) -> bool {
		self != Self::Query
	}

	/// Whether the store is removed when the run ends. Only a run that is
	/// self-contained cleans up; a load run's data is for the next run.
	pub(crate) fn wipes_on_shutdown(self) -> bool {
		self == Self::All
	}

	/// Whether the run creates the dataset (and so owns the schema).
	pub(crate) fn loads(self) -> bool {
		self != Self::Query
	}

	/// Whether the run executes the repeatable read phases.
	pub(crate) fn queries(self) -> bool {
		self != Self::Load
	}

	/// Whether the run executes the phases that mutate or consume the dataset
	/// after loading (deletes and batches). A split run keeps the dataset
	/// intact for the query runs that follow it.
	pub(crate) fn consumes(self) -> bool {
		self == Self::All
	}
}

/// Everything that determines what is in a loaded dataset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Manifest {
	version: u32,
	database: String,
	endpoint: Option<String>,
	samples: u32,
	key_type: String,
	random: bool,
	corpus_seed: Option<u64>,
	template_digest: String,
	persisted: bool,
	optimised: bool,
	sync: bool,
	/// Vector filter columns engines prepared alongside the rows, as
	/// `name:kind`. They shape the stored payload for some engines.
	vector_filter_fields: Vec<String>,
}

/// The configuration a manifest is built from or checked against.
pub(crate) struct Shape<'a> {
	pub(crate) database: &'a str,
	pub(crate) endpoint: Option<&'a str>,
	pub(crate) samples: u32,
	pub(crate) key_type: String,
	pub(crate) random: bool,
	pub(crate) corpus_seed: Option<u64>,
	pub(crate) template_digest: &'a str,
	pub(crate) persisted: bool,
	pub(crate) optimised: bool,
	pub(crate) sync: bool,
	pub(crate) vector_filter_fields: &'a [FilterField],
}

impl Manifest {
	/// Captures the configuration that shapes the dataset.
	pub(crate) fn new(shape: &Shape<'_>) -> Self {
		Self {
			version: MANIFEST_VERSION,
			database: shape.database.to_string(),
			endpoint: shape.endpoint.map(str::to_string),
			samples: shape.samples,
			key_type: shape.key_type.clone(),
			random: shape.random,
			corpus_seed: shape.corpus_seed,
			template_digest: shape.template_digest.to_string(),
			persisted: shape.persisted,
			optimised: shape.optimised,
			sync: shape.sync,
			vector_filter_fields: shape
				.vector_filter_fields
				.iter()
				.map(|f| format!("{}:{:?}", f.name, f.kind))
				.collect(),
		}
	}

	/// Describes every way `other` differs from `self`, or nothing when the
	/// two describe the same dataset. `self` is the stored manifest.
	fn differences(&self, other: &Self) -> Vec<String> {
		let mut out = Vec::new();
		macro_rules! field {
			($name:ident) => {
				if self.$name != other.$name {
					out.push(format!(
						"{}: loaded with {:?}, now {:?}",
						stringify!($name),
						self.$name,
						other.$name
					));
				}
			};
		}
		field!(database);
		field!(endpoint);
		field!(samples);
		field!(key_type);
		field!(random);
		field!(corpus_seed);
		field!(template_digest);
		field!(persisted);
		field!(optimised);
		field!(sync);
		field!(vector_filter_fields);
		out
	}
}

/// Directory holding the manifest for one database and run name.
pub(crate) fn data_dir(root: &str, database: &str, name: Option<&str>) -> PathBuf {
	let leaf = match name {
		Some(n) => format!("{database}-{n}"),
		None => database.to_string(),
	};
	Path::new(root).join(leaf)
}

/// Name of the Docker volume holding a database's data between runs.
pub(crate) fn volume_name(database: &str, name: Option<&str>) -> String {
	match name {
		Some(n) => format!("crud-bench-{database}-{n}"),
		None => format!("crud-bench-{database}"),
	}
}

/// Records a finished load run so later query runs can check it.
pub(crate) fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
	std::fs::create_dir_all(dir)
		.with_context(|| format!("creating data directory {}", dir.display()))?;
	let path = dir.join(MANIFEST_FILE);
	std::fs::write(&path, serde_json::to_vec_pretty(manifest)?)
		.with_context(|| format!("writing {}", path.display()))
}

/// Removes any manifest, so a load run that fails part-way cannot leave a
/// dataset that a query run would accept.
pub(crate) fn clear_manifest(dir: &Path) -> Result<()> {
	match std::fs::remove_file(dir.join(MANIFEST_FILE)) {
		Ok(()) => Ok(()),
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(e) => Err(e).with_context(|| format!("removing manifest in {}", dir.display())),
	}
}

/// Checks that the dataset a load run left matches this run's configuration.
pub(crate) fn verify(dir: &Path, expected: &Manifest) -> Result<()> {
	let path = dir.join(MANIFEST_FILE);
	let raw = std::fs::read(&path).with_context(|| {
		format!(
			"no loaded dataset found ({} is missing); run with `--mode load` first",
			path.display()
		)
	})?;
	let stored: Manifest = serde_json::from_slice(&raw)
		.with_context(|| format!("{} is not a valid manifest", path.display()))?;
	if stored.version != MANIFEST_VERSION {
		bail!(
			"{} was written by manifest version {}, this build reads version {}; reload with `--mode load`",
			path.display(),
			stored.version,
			MANIFEST_VERSION
		);
	}
	let differences = stored.differences(expected);
	if !differences.is_empty() {
		bail!(
			"the loaded dataset does not match this run's configuration:\n  {}\nreload with `--mode load`, or pass the settings the data was loaded with",
			differences.join("\n  ")
		);
	}
	Ok(())
}

#[cfg(test)]
mod test {
	use super::*;

	fn manifest() -> Manifest {
		Manifest {
			version: MANIFEST_VERSION,
			database: "surrealkv".into(),
			endpoint: None,
			samples: 1000,
			key_type: "Integer".into(),
			random: false,
			corpus_seed: Some(7),
			template_digest: "abc".into(),
			persisted: false,
			optimised: false,
			sync: false,
			vector_filter_fields: vec![],
		}
	}

	fn scratch(tag: &str) -> PathBuf {
		let dir =
			std::env::temp_dir().join(format!("crud-bench-reuse-{tag}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		dir
	}

	#[test]
	fn modes_split_the_phases() {
		assert!(RunMode::All.loads() && RunMode::All.queries() && RunMode::All.consumes());
		assert!(RunMode::Load.loads() && !RunMode::Load.queries() && !RunMode::Load.consumes());
		assert!(!RunMode::Query.loads() && RunMode::Query.queries() && !RunMode::Query.consumes());
		assert!(RunMode::All.wipes_on_setup() && RunMode::Load.wipes_on_setup());
		assert!(!RunMode::Query.wipes_on_setup());
		assert!(RunMode::All.wipes_on_shutdown());
		assert!(!RunMode::Load.wipes_on_shutdown() && !RunMode::Query.wipes_on_shutdown());
	}

	#[test]
	fn a_matching_manifest_verifies() {
		let dir = scratch("match");
		write_manifest(&dir, &manifest()).unwrap();
		verify(&dir, &manifest()).unwrap();
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn a_missing_manifest_says_to_load_first() {
		let dir = scratch("missing");
		let err = verify(&dir, &manifest()).unwrap_err();
		assert!(format!("{err:#}").contains("--mode load"), "{err:#}");
	}

	#[test]
	fn every_mismatch_is_named() {
		let dir = scratch("mismatch");
		write_manifest(&dir, &manifest()).unwrap();
		let mut now = manifest();
		now.samples = 2000;
		now.key_type = "String26".into();
		let err = format!("{:#}", verify(&dir, &now).unwrap_err());
		assert!(err.contains("samples"), "{err}");
		assert!(err.contains("key_type"), "{err}");
		assert!(!err.contains("random"), "{err}");
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn clearing_removes_the_manifest_and_tolerates_none() {
		let dir = scratch("clear");
		write_manifest(&dir, &manifest()).unwrap();
		clear_manifest(&dir).unwrap();
		clear_manifest(&dir).unwrap();
		assert!(verify(&dir, &manifest()).is_err());
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn names_are_scoped_by_database_and_run_name() {
		assert_eq!(volume_name("postgres", None), "crud-bench-postgres");
		assert_eq!(volume_name("postgres", Some("a")), "crud-bench-postgres-a");
		assert_eq!(data_dir(".d", "mysql", Some("x")), Path::new(".d").join("mysql-x"));
	}
}
