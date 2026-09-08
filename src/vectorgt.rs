//! Exact nearest-neighbour ground truth for the vector-search benchmark.
//!
//! Recall is only comparable across engines when every engine is scored against
//! the same answer key, so the key is computed here rather than taken from any
//! engine's own exact-search path. An engine scored against itself reports how
//! well its index agrees with its own bruteforce leg, which is a useful
//! regression signal but not a figure that can be placed beside another
//! engine's — a quirk shared by an engine's exact and approximate paths cancels
//! out, and divergent metric definitions leave every engine at ~1.0 against
//! itself with nothing to reveal it.
//!
//! Both inputs are reproducible: the corpus is a pure function of the corpus
//! seed and the queries of the query seed. That makes the whole table a pure
//! function of its [`Request`] fingerprint, so it is computed once and cached
//! for reuse across every engine and every later run. It also means the corpus
//! is reconstructed locally for scoring rather than read back out of the
//! database under test.

use crate::VectorDistance;
use crate::valueprovider::{ValueProvider, ValueStream};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use twox_hash::XxHash64;

/// One true neighbour of a query vector.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct Neighbour {
	/// Sample index of the neighbouring row. This is the benchmark's sample
	/// number, not the engine's key — [`crate::keyprovider::KeyProvider`] maps
	/// it to whatever key type the run is using.
	pub(crate) sample: u32,
	/// Distance from the query in ranking orientation: lower is always nearer,
	/// whatever the metric's native convention.
	pub(crate) distance: f32,
}

/// The answer key: for each query, its true nearest neighbours, best first.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct GroundTruth {
	pub(crate) neighbours: Vec<Vec<Neighbour>>,
}

/// Everything the answer key depends on. Two runs sharing a fingerprint share
/// their ground truth, which is what makes the cache reusable across engines.
#[derive(Clone, Debug)]
pub(crate) struct Request {
	/// Name of the `vector:<dim>` column being searched.
	pub(crate) field: String,
	/// Number of rows in the corpus.
	pub(crate) samples: u32,
	/// Neighbours to record per query.
	pub(crate) top_k: usize,
	/// Metric the scan ranks by.
	pub(crate) metric: VectorDistance,
	/// Seed the corpus was generated from.
	pub(crate) corpus_seed: u64,
	/// Seed the query set was generated from.
	pub(crate) query_seed: u64,
	/// Number of query vectors.
	pub(crate) query_count: usize,
	/// The value template itself: changing the schema changes the corpus, even
	/// at an unchanged seed.
	pub(crate) template: String,
}

impl Request {
	/// Stable digest of every input, used as the cache file name.
	fn fingerprint(&self) -> u64 {
		let Request {
			field,
			samples,
			top_k,
			metric,
			corpus_seed,
			query_seed,
			query_count,
			template,
		} = self;
		// Field-separated so no two distinct requests can render to the same
		// string by shifting a boundary.
		let key = format!(
			"v1\u{1f}{field}\u{1f}{samples}\u{1f}{top_k}\u{1f}{metric:?}\u{1f}{corpus_seed}\u{1f}{query_seed}\u{1f}{query_count}\u{1f}{template}"
		);
		XxHash64::oneshot(0, key.as_bytes())
	}

	/// Path this request's answer key is cached at.
	fn cache_path(&self, dir: &Path) -> PathBuf {
		dir.join(format!("gt-{:016x}.json", self.fingerprint()))
	}
}

/// Load the answer key from `cache_dir`, computing and storing it on a miss.
///
/// A cache that cannot be read or written is not fatal — the key is recomputed
/// and the run continues, since the cache is an optimisation rather than a
/// source of truth.
pub(crate) fn load_or_compute(
	request: &Request,
	vp: &ValueProvider,
	queries: &[Vec<f32>],
	cache_dir: &Path,
) -> Result<(GroundTruth, bool)> {
	let path = request.cache_path(cache_dir);
	if let Ok(text) = fs::read_to_string(&path) {
		match serde_json::from_str::<GroundTruth>(&text) {
			Ok(gt) if gt.neighbours.len() == queries.len() => return Ok((gt, true)),
			// A truncated or stale file is simply a miss: recompute over it.
			_ => {}
		}
	}
	let gt = compute(request, vp, queries)?;
	if let Err(e) = store(&gt, &path) {
		eprintln!("vector ground truth: could not cache to {}: {e:#}", path.display());
	}
	Ok((gt, false))
}

fn store(gt: &GroundTruth, path: &Path) -> Result<()> {
	if let Some(dir) = path.parent() {
		fs::create_dir_all(dir)
			.with_context(|| format!("creating ground-truth cache dir {}", dir.display()))?;
	}
	let text = serde_json::to_string(gt)?;
	fs::write(path, text).with_context(|| format!("writing {}", path.display()))?;
	Ok(())
}

/// Compute exact top-k for every query by scanning the whole corpus.
///
/// The corpus is regenerated in place rather than held in memory: at 1M rows of
/// 3072 dimensions the full set is over 12 GB, so each worker streams its own
/// slice and keeps only the running top-k. Workers partition the corpus rather
/// than the queries, so every worker sees each of its rows exactly once and the
/// per-query accumulators merge at the end.
pub(crate) fn compute(
	request: &Request,
	vp: &ValueProvider,
	queries: &[Vec<f32>],
) -> Result<GroundTruth> {
	if request.top_k == 0 {
		bail!("ground truth: top_k must be > 0");
	}
	if queries.is_empty() {
		bail!("ground truth: query set is empty");
	}
	if vp.seed().is_none() {
		bail!(
			"ground truth needs a reproducible corpus: set `seed` in the benchmark TOML or pass --corpus-seed"
		);
	}
	let dim = queries[0].len();
	if let Some(bad) = queries.iter().position(|q| q.len() != dim) {
		bail!("ground truth: query {bad} has {} dimensions, expected {dim}", queries[bad].len());
	}

	let workers = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).min(
		// One worker per row at most; tiny corpora do not need a thread each.
		(request.samples as usize).max(1),
	);
	let chunk = (request.samples as usize).div_ceil(workers.max(1)) as u32;

	let partials: Vec<Vec<TopK>> = std::thread::scope(|scope| -> Result<Vec<Vec<TopK>>> {
		let mut handles = Vec::with_capacity(workers);
		for w in 0..workers {
			let start = (w as u32) * chunk;
			if start >= request.samples {
				break;
			}
			let end = start.saturating_add(chunk).min(request.samples);
			let mut worker_vp = vp.clone();
			let field = request.field.as_str();
			let (top_k, metric) = (request.top_k, request.metric);
			handles.push(scope.spawn(move || -> Result<Vec<TopK>> {
				let mut acc: Vec<TopK> = (0..queries.len()).map(|_| TopK::new(top_k)).collect();
				for n in start..end {
					// Scans run after the update phase, so the corpus a scan
					// observes is the one the update phase left behind.
					let row = worker_vp.generate_value_for(ValueStream::Update, n);
					let Some(v) = row.get_field(field).and_then(|v| v.as_float_vector()) else {
						bail!("ground truth: sample {n} has no vector at field {field:?}");
					};
					if v.len() != dim {
						bail!(
							"ground truth: sample {n} has {} dimensions, queries have {dim}",
							v.len()
						);
					}
					for (q, slot) in queries.iter().zip(acc.iter_mut()) {
						slot.offer(n, distance(metric, q, v));
					}
				}
				Ok(acc)
			}));
		}
		handles.into_iter().map(|h| h.join().expect("ground-truth worker panicked")).collect()
	})?;

	let mut merged: Vec<TopK> = (0..queries.len()).map(|_| TopK::new(request.top_k)).collect();
	for partial in &partials {
		for (slot, part) in merged.iter_mut().zip(partial.iter()) {
			slot.merge(part);
		}
	}
	Ok(GroundTruth {
		neighbours: merged.into_iter().map(|t| t.best).collect(),
	})
}

/// Distance between two vectors in ranking orientation: lower is always nearer.
///
/// Only the ordering matters for recall, so metrics whose native convention is
/// "higher is nearer" are negated rather than rescaled — the absolute values
/// stay comparable within a metric without pretending to match any particular
/// engine's numeric output.
fn distance(metric: VectorDistance, a: &[f32], b: &[f32]) -> f32 {
	match metric {
		VectorDistance::Cosine => {
			let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
			for (x, y) in a.iter().zip(b.iter()) {
				dot += x * y;
				na += x * x;
				nb += y * y;
			}
			let denom = na.sqrt() * nb.sqrt();
			// A zero vector has no direction; treat it as maximally distant
			// rather than producing a NaN that would corrupt the ordering.
			if denom == 0.0 {
				2.0
			} else {
				1.0 - dot / denom
			}
		}
		VectorDistance::Euclidean => {
			let mut sum = 0.0f32;
			for (x, y) in a.iter().zip(b.iter()) {
				let d = x - y;
				sum += d * d;
			}
			sum.sqrt()
		}
		VectorDistance::InnerProduct => {
			let mut dot = 0.0f32;
			for (x, y) in a.iter().zip(b.iter()) {
				dot += x * y;
			}
			-dot
		}
		VectorDistance::Manhattan => {
			let mut sum = 0.0f32;
			for (x, y) in a.iter().zip(b.iter()) {
				sum += (x - y).abs();
			}
			sum
		}
	}
}

/// Bounded best-first accumulator holding at most `k` neighbours sorted by
/// ascending distance.
///
/// The overwhelmingly common case is a row that cannot displace the current
/// worst, and that costs a single comparison; only an improvement pays for an
/// insertion.
struct TopK {
	k: usize,
	best: Vec<Neighbour>,
}

impl TopK {
	fn new(k: usize) -> Self {
		Self {
			k,
			best: Vec::with_capacity(k),
		}
	}

	fn offer(&mut self, sample: u32, distance: f32) {
		if self.k == 0 {
			return;
		}
		if self.best.len() == self.k {
			if distance >= self.best[self.k - 1].distance {
				return;
			}
			self.best.pop();
		}
		let at = self.best.partition_point(|n| n.distance <= distance);
		self.best.insert(
			at,
			Neighbour {
				sample,
				distance,
			},
		);
	}

	fn merge(&mut self, other: &TopK) {
		for n in &other.best {
			self.offer(n.sample, n.distance);
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;

	const TEMPLATE: &str = r#"{ "name": "string:8", "embedding": "vector:16" }"#;

	fn request(samples: u32, top_k: usize, metric: VectorDistance) -> Request {
		Request {
			field: "embedding".to_string(),
			samples,
			top_k,
			metric,
			corpus_seed: 42,
			query_seed: 7,
			query_count: 3,
			template: TEMPLATE.to_string(),
		}
	}

	fn provider() -> ValueProvider {
		ValueProvider::new(TEMPLATE).unwrap().with_seed(42)
	}

	/// The whole design rests on the corpus being reconstructible, so assert it
	/// directly: independent providers must agree sample by sample.
	#[test]
	fn seeded_corpus_is_reproducible_across_providers() {
		let (mut a, mut b) = (provider(), provider().clone());
		for n in [0u32, 1, 17, 999] {
			let va = a.generate_value_for(ValueStream::Update, n);
			let vb = b.generate_value_for(ValueStream::Update, n);
			assert_eq!(va, vb, "sample {n} differed between providers");
		}
	}

	/// Create and update must not collide, or the update phase would rewrite
	/// identical bytes and ground truth would be ambiguous about which it meant.
	#[test]
	fn streams_are_independent() {
		let mut vp = provider();
		let create = vp.generate_value_for(ValueStream::Create, 5);
		let update = vp.generate_value_for(ValueStream::Update, 5);
		assert_ne!(create, update);
	}

	/// An unseeded provider keeps its old behaviour, so every non-vector config
	/// is unaffected by this machinery.
	#[test]
	fn unseeded_provider_stays_random() {
		let mut vp = ValueProvider::new(TEMPLATE).unwrap();
		let a = vp.generate_value_for(ValueStream::Create, 3);
		let b = vp.generate_value_for(ValueStream::Create, 3);
		assert_ne!(a, b, "unseeded provider should not be reproducible");
	}

	/// Queries are extensible: asking for more must not renumber the ones
	/// already computed, or a cached answer key would silently mismatch.
	#[test]
	fn query_set_extends_rather_than_reshuffles() {
		let vp = provider();
		let few = vp.generate_vectors("embedding", 3, 7).unwrap();
		let many = vp.generate_vectors("embedding", 10, 7).unwrap();
		assert_eq!(few.as_slice(), &many[..3]);
	}

	#[test]
	fn rejects_non_vector_field() {
		let vp = provider();
		let err = vp.generate_vectors("name", 1, 7).unwrap_err().to_string();
		assert!(err.contains("not a `vector:<dim>` column"), "{err}");
	}

	/// The parallel path partitions the corpus, so it has to agree with a
	/// single-threaded sweep over the same data.
	#[test]
	fn matches_a_naive_sweep() {
		for metric in [
			VectorDistance::Cosine,
			VectorDistance::Euclidean,
			VectorDistance::InnerProduct,
			VectorDistance::Manhattan,
		] {
			let vp = provider();
			let samples = 500u32;
			let top_k = 5;
			let queries = vp.generate_vectors("embedding", 3, 7).unwrap();
			let gt = compute(&request(samples, top_k, metric), &vp, &queries).unwrap();

			let mut naive = vp.clone();
			let corpus: Vec<Vec<f32>> = (0..samples)
				.map(|n| {
					naive
						.generate_value_for(ValueStream::Update, n)
						.get_field("embedding")
						.and_then(|v| v.as_float_vector())
						.map(|v| v.to_vec())
						.unwrap()
				})
				.collect();
			for (qi, q) in queries.iter().enumerate() {
				let mut all: Vec<(u32, f32)> = corpus
					.iter()
					.enumerate()
					.map(|(n, v)| (n as u32, distance(metric, q, v)))
					.collect();
				all.sort_by(|a, b| a.1.total_cmp(&b.1));
				let expected: Vec<u32> = all[..top_k].iter().map(|(n, _)| *n).collect();
				let got: Vec<u32> = gt.neighbours[qi].iter().map(|n| n.sample).collect();
				assert_eq!(expected, got, "{metric:?} query {qi}");
			}
		}
	}

	/// Neighbours must come back best-first; recall@k and the tie epsilon both
	/// read positionally.
	#[test]
	fn neighbours_are_sorted_best_first() {
		let vp = provider();
		let queries = vp.generate_vectors("embedding", 2, 7).unwrap();
		let gt = compute(&request(300, 8, VectorDistance::Euclidean), &vp, &queries).unwrap();
		for row in &gt.neighbours {
			assert_eq!(row.len(), 8);
			assert!(row.windows(2).all(|w| w[0].distance <= w[1].distance), "{row:?}");
		}
	}

	/// A corpus smaller than k must return what exists rather than padding.
	#[test]
	fn corpus_smaller_than_k() {
		let vp = provider();
		let queries = vp.generate_vectors("embedding", 1, 7).unwrap();
		let gt = compute(&request(3, 10, VectorDistance::Cosine), &vp, &queries).unwrap();
		assert_eq!(gt.neighbours[0].len(), 3);
	}

	/// Changing any input has to change the cache key, or a stale answer key
	/// would be served for a different corpus.
	#[test]
	fn fingerprint_covers_every_input() {
		let base = request(100, 10, VectorDistance::Cosine);
		let mut seen = vec![base.fingerprint()];
		let mut variants = vec![
			Request {
				samples: 101,
				..base.clone()
			},
			Request {
				top_k: 11,
				..base.clone()
			},
			Request {
				metric: VectorDistance::Euclidean,
				..base.clone()
			},
			Request {
				corpus_seed: 43,
				..base.clone()
			},
			Request {
				query_seed: 8,
				..base.clone()
			},
			Request {
				query_count: 4,
				..base.clone()
			},
			Request {
				field: "other".to_string(),
				..base.clone()
			},
			Request {
				template: "{}".to_string(),
				..base.clone()
			},
		];
		for v in variants.drain(..) {
			let f = v.fingerprint();
			assert!(!seen.contains(&f), "fingerprint collision for {v:?}");
			seen.push(f);
		}
	}

	#[test]
	fn cache_round_trips() {
		let dir = std::env::temp_dir().join(format!("crud-bench-gt-test-{}", std::process::id()));
		let _ = fs::remove_dir_all(&dir);
		let vp = provider();
		let req = request(200, 4, VectorDistance::Cosine);
		let queries = vp.generate_vectors("embedding", 3, 7).unwrap();

		let (first, hit) = load_or_compute(&req, &vp, &queries, &dir).unwrap();
		assert!(!hit, "first call should miss");
		let (second, hit) = load_or_compute(&req, &vp, &queries, &dir).unwrap();
		assert!(hit, "second call should hit the cache");

		let ids = |g: &GroundTruth| -> Vec<Vec<u32>> {
			g.neighbours.iter().map(|r| r.iter().map(|n| n.sample).collect()).collect()
		};
		assert_eq!(ids(&first), ids(&second));
		let _ = fs::remove_dir_all(&dir);
	}
}
