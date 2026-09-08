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
use crate::engine::KnnKey;
use crate::keyprovider::{IntegerKeyProvider, KeyProvider, StringKeyProvider};
use crate::valueprovider::{ValueProvider, ValueStream};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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
///
/// More than `top_k` neighbours are stored so the tie tolerance has candidates
/// beyond the cut to admit; the first `top_k` remain the strict answer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct GroundTruth {
	pub(crate) neighbours: Vec<Vec<Neighbour>>,
}

/// How many neighbours to store for a given `top_k`.
///
/// Twice the cut is enough for any realistic tolerance: it is a scoring-time
/// parameter, so widening the tolerance never invalidates a cached answer key.
fn storage_depth(top_k: usize) -> usize {
	top_k.saturating_mul(2)
}

/// One query's accepted answers, resolved into the run's key shape.
///
/// Built once per scan rather than per iteration: deriving `top_k` keys from
/// the [`KeyProvider`] on every KNN call would put avoidable work next to the
/// timed window.
#[derive(Clone, Debug)]
pub(crate) struct VectorAnswer {
	/// Number of true neighbours that exist, which is `min(top_k, corpus)`.
	/// Recall divides by this, so a corpus smaller than k is not penalised.
	truth_len: usize,
	/// Keys that count as a correct hit, including any admitted by the tie
	/// tolerance.
	keys: HashSet<KnnKey>,
}

/// Resolve the answer key into the run's key shape, applying the tie tolerance.
///
/// A hit counts when the engine returned a row inside the true top-k, or one
/// whose true distance is within `tie_epsilon` (relative) of the k-th true
/// distance. Engines compute distances at different precisions, so rows
/// straddling the k-th boundary can swap places without any real quality
/// difference; without a tolerance that shows up as a recall gap that is not
/// really there. The default is 0.0 — strict recall@k.
pub(crate) fn build_answers(
	gt: &GroundTruth,
	kp: &KeyProvider,
	top_k: usize,
	tie_epsilon: f64,
) -> Vec<VectorAnswer> {
	gt.neighbours
		.iter()
		.map(|row| {
			let truth_len = row.len().min(top_k);
			// Everything at or inside the k-th distance, widened by the
			// tolerance. `cut` is the k-th distance when one exists.
			let limit = row.get(truth_len.saturating_sub(1)).map(|n| {
				let cut = n.distance as f64;
				// Relative on magnitude, with an absolute floor so a cut at or
				// near zero still admits its neighbours.
				cut + tie_epsilon * cut.abs().max(f64::EPSILON)
			});
			let keys = row
				.iter()
				.enumerate()
				.filter(|(i, n)| match limit {
					Some(limit) => *i < truth_len || (n.distance as f64) <= limit,
					None => false,
				})
				.map(|(_, n)| key_for(kp, n.sample))
				.collect();
			VectorAnswer {
				truth_len,
				keys,
			}
		})
		.collect()
}

/// Map a benchmark sample index into the run's key shape.
fn key_for(kp: &KeyProvider, sample: u32) -> KnnKey {
	// `KeyProvider` is `Copy` and its `key` needs `&mut`, so score against a
	// local copy rather than threading mutability through the caller.
	let mut kp = *kp;
	match &mut kp {
		KeyProvider::OrderedInteger(p) => KnnKey::Integer(p.key(sample)),
		KeyProvider::UnorderedInteger(p) => KnnKey::Integer(p.key(sample)),
		KeyProvider::OrderedString(p) => KnnKey::Text(p.key(sample)),
		KeyProvider::UnorderedString(p) => KnnKey::Text(p.key(sample)),
	}
}

/// Recall of one KNN result: the share of a query's true neighbours the engine
/// actually returned. `None` when the query has no true neighbours to find.
pub(crate) fn recall(answer: &VectorAnswer, returned: &[KnnKey]) -> Option<f64> {
	if answer.truth_len == 0 {
		return None;
	}
	let found = returned.iter().filter(|k| answer.keys.contains(k)).count();
	Some((found as f64 / answer.truth_len as f64).min(1.0))
}

/// Per-iteration recall values for one scan, merged across workers.
#[derive(Clone, Debug, Default)]
pub(crate) struct RecallTally {
	values: Vec<f64>,
}

impl RecallTally {
	pub(crate) fn record(&mut self, value: f64) {
		self.values.push(value);
	}

	pub(crate) fn merge(&mut self, other: RecallTally) {
		self.values.extend(other.values);
	}

	/// Collapse into the reported figures.
	///
	/// The mean alone hides the shape: an index can average well and still
	/// answer a tail of queries badly, which is exactly the failure mode a
	/// graph index has on data it indexed poorly. `p5` and `min` surface it.
	pub(crate) fn summarise(&self) -> Option<RecallSummary> {
		if self.values.is_empty() {
			return None;
		}
		let mut sorted = self.values.clone();
		sorted.sort_by(f64::total_cmp);
		let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
		// Nearest-rank percentile, clamped into range for tiny samples.
		let idx = ((0.05 * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
		Some(RecallSummary {
			mean,
			p5: sorted[idx],
			min: sorted[0],
			queries: sorted.len(),
		})
	}
}

/// Reported recall figures for one vector scan.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct RecallSummary {
	/// Mean recall@k across scored iterations.
	pub(crate) mean: f64,
	/// 5th-percentile recall — the tail the mean hides.
	pub(crate) p5: f64,
	/// Worst single query.
	pub(crate) min: f64,
	/// Number of iterations scored.
	pub(crate) queries: usize,
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
			// Depth as well as width: a key stored before the tie window was
			// widened would silently score against too few candidates.
			Ok(gt)
				if gt.neighbours.len() == queries.len()
					&& gt.neighbours.iter().all(|n| {
						n.len() >= storage_depth(request.top_k).min(request.samples as usize)
					}) =>
			{
				return Ok((gt, true));
			}
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
			let (depth, metric) = (storage_depth(request.top_k), request.metric);
			handles.push(scope.spawn(move || -> Result<Vec<TopK>> {
				let mut acc: Vec<TopK> = (0..queries.len()).map(|_| TopK::new(depth)).collect();
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

	let mut merged: Vec<TopK> =
		(0..queries.len()).map(|_| TopK::new(storage_depth(request.top_k))).collect();
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
				// The key stores beyond `top_k` for the tie window; the strict
				// answer is its prefix.
				let expected: Vec<u32> = all[..top_k].iter().map(|(n, _)| *n).collect();
				let got: Vec<u32> =
					gt.neighbours[qi].iter().take(top_k).map(|n| n.sample).collect();
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
			assert_eq!(row.len(), storage_depth(8));
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

	fn answer_key(rows: Vec<Vec<(u32, f32)>>) -> GroundTruth {
		GroundTruth {
			neighbours: rows
				.into_iter()
				.map(|r| {
					r.into_iter()
						.map(|(sample, distance)| Neighbour {
							sample,
							distance,
						})
						.collect()
				})
				.collect(),
		}
	}

	fn integer_kp() -> KeyProvider {
		KeyProvider::new(crate::KeyType::Integer, false)
	}

	/// Engines return keys, not sample indices, so tests have to address rows
	/// the same way — the two are not the same number.
	fn keys(kp: &KeyProvider, samples: &[u32]) -> Vec<KnnKey> {
		samples.iter().map(|n| key_for(kp, *n)).collect()
	}

	#[test]
	fn recall_counts_only_true_neighbours() {
		let gt = answer_key(vec![vec![(1, 0.1), (2, 0.2), (3, 0.3), (9, 0.9), (8, 1.0), (7, 1.1)]]);
		let kp = integer_kp();
		let answers = build_answers(&gt, &kp, 3, 0.0);
		let hit = |ids: &[u32]| -> Option<f64> { recall(&answers[0], &keys(&kp, ids)) };
		assert_eq!(hit(&[1, 2, 3]), Some(1.0));
		assert_eq!(hit(&[1, 2, 99]), Some(2.0 / 3.0));
		assert_eq!(hit(&[97, 98, 99]), Some(0.0));
		// Rows outside the true top-k earn nothing, even though the answer key
		// stores them for the tie window.
		assert_eq!(hit(&[9, 8, 7]), Some(0.0));
	}

	/// A near-tie at the k-th place should not read as a quality gap: engines
	/// compute distances at different precisions and can legitimately swap the
	/// rows straddling the cut.
	#[test]
	fn tie_epsilon_admits_a_boundary_neighbour() {
		let gt = answer_key(vec![vec![(1, 0.10), (2, 0.20), (3, 0.2001), (4, 0.9)]]);
		let kp = integer_kp();
		let returned = keys(&kp, &[1, 3]);

		let strict = build_answers(&gt, &kp, 2, 0.0);
		assert_eq!(recall(&strict[0], &returned), Some(0.5));

		let tolerant = build_answers(&gt, &kp, 2, 0.01);
		assert_eq!(recall(&tolerant[0], &returned), Some(1.0));

		// The tolerance must stay narrow: a genuinely distant row is still wrong.
		assert_eq!(recall(&tolerant[0], &keys(&kp, &[1, 4])), Some(0.5));
	}

	/// Recall divides by the neighbours that exist, so a corpus smaller than k
	/// is not scored as a miss.
	#[test]
	fn short_answer_key_is_not_penalised() {
		let gt = answer_key(vec![vec![(1, 0.1), (2, 0.2)]]);
		let kp = integer_kp();
		let answers = build_answers(&gt, &kp, 10, 0.0);
		assert_eq!(recall(&answers[0], &keys(&kp, &[1, 2])), Some(1.0));
	}

	/// Ground truth stores sample indices; scoring compares engine keys. The
	/// mapping between them has to be the run's own KeyProvider or every hit
	/// silently misses.
	#[test]
	fn answers_use_the_runs_key_shape() {
		let gt = answer_key(vec![vec![(0, 0.1), (1, 0.2)]]);

		let mut kp = KeyProvider::new(crate::KeyType::Integer, true);
		let KeyProvider::UnorderedInteger(p) = &mut kp else {
			panic!("expected an unordered integer provider");
		};
		let expected: Vec<KnnKey> = [0u32, 1].iter().map(|n| KnnKey::Integer(p.key(*n))).collect();
		let answers = build_answers(&gt, &kp, 2, 0.0);
		assert_eq!(recall(&answers[0], &expected), Some(1.0));

		let mut string_kp = KeyProvider::new(crate::KeyType::String26, false);
		let KeyProvider::OrderedString(sp) = &mut string_kp else {
			panic!("expected an ordered string provider");
		};
		let expected: Vec<KnnKey> = [0u32, 1].iter().map(|n| KnnKey::Text(sp.key(*n))).collect();
		let answers = build_answers(&gt, &string_kp, 2, 0.0);
		assert_eq!(recall(&answers[0], &expected), Some(1.0));
	}

	#[test]
	fn summary_reports_the_tail_not_just_the_mean() {
		let mut tally = RecallTally::default();
		for _ in 0..95 {
			tally.record(1.0);
		}
		for _ in 0..5 {
			tally.record(0.2);
		}
		let s = tally.summarise().unwrap();
		assert_eq!(s.queries, 100);
		assert!((s.mean - 0.96).abs() < 1e-9, "{}", s.mean);
		// A mean of 0.96 hides a 5% tail answering at 0.2.
		assert_eq!(s.p5, 0.2);
		assert_eq!(s.min, 0.2);
		assert!(RecallTally::default().summarise().is_none());
	}

	/// Tallies are merged across workers, so the summary must not depend on
	/// which worker happened to score which query.
	#[test]
	fn tallies_merge_across_workers() {
		let mut a = RecallTally::default();
		a.record(1.0);
		let mut b = RecallTally::default();
		b.record(0.0);
		a.merge(b);
		let s = a.summarise().unwrap();
		assert_eq!(s.queries, 2);
		assert!((s.mean - 0.5).abs() < 1e-9);
	}

	/// The clustered generator exists because uniform components give a corpus
	/// with no neighbourhood structure. Assert the structure is actually there:
	/// a query's nearest neighbour should sit far closer, relative to a random
	/// point, than it does under the uniform generator.
	#[test]
	fn clustered_data_has_neighbourhood_structure() {
		fn profile(template: &str) -> (f32, f32) {
			let vp = ValueProvider::new(template).unwrap().with_seed(42);
			let mut src = vp.clone();
			let corpus: Vec<Vec<f32>> = (0..4_000u32)
				.map(|i| {
					src.generate_value_for(ValueStream::Update, i)
						.get_field("embedding")
						.and_then(|v| v.as_float_vector())
						.unwrap()
						.to_vec()
				})
				.collect();
			let queries = vp.generate_vectors("embedding", 10, 7).unwrap();
			let (mut nearest, mut mean) = (0.0f32, 0.0f32);
			for q in &queries {
				let ds: Vec<f32> =
					corpus.iter().map(|v| distance(VectorDistance::Cosine, q, v)).collect();
				mean += ds.iter().sum::<f32>() / ds.len() as f32;
				nearest += ds.iter().copied().fold(f32::INFINITY, f32::min);
			}
			let n = queries.len() as f32;
			(nearest / n, mean / n)
		}

		let (u_near, u_mean) = profile(r#"{ "embedding": "vector:64" }"#);
		let (c_near, c_mean) = profile(r#"{ "embedding": "vector:64:clustered:50" }"#);

		// Under uniform components the nearest neighbour is only modestly
		// closer than an arbitrary point — the distance concentration that
		// makes every index look perfect.
		assert!(u_near / u_mean > 0.4, "uniform nn ratio {}", u_near / u_mean);
		// Clustered draws put the nearest neighbour an order of magnitude
		// closer, so there is a real neighbourhood for a search to miss.
		assert!(c_near / c_mean < 0.2, "clustered nn ratio {}", c_near / c_mean);
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
