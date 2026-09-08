use crate::value::BenchValue;
use anyhow::{Result, anyhow, bail};
use chrono::{TimeZone, Utc};
use log::debug;
use rand::RngExt as RandGen;
use rand::SeedableRng;
use rand::prelude::SmallRng;
use rust_decimal::Decimal;
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// Which derivation stream a generated payload belongs to.
///
/// A seeded provider salts each stream differently so they stay independent.
/// The create and update phases both write every sample, and salting them apart
/// means updates write genuinely different content rather than rewriting
/// identical bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueStream {
	/// Payloads written by the create phase.
	Create,
	/// Payloads written by the update phase. Scans run after the update phase,
	/// so this is the content a scan actually observes — and therefore the
	/// stream vector-search ground truth has to be derived from.
	Update,
	/// Query vectors for vector search. Never inserted, so a query set drawn
	/// from this stream is disjoint from the corpus by construction.
	Query,
}

impl ValueStream {
	/// Per-stream salt. Distinct arbitrary constants; the values carry no
	/// meaning beyond being unrelated to one another and to [`SAMPLE_ODD`].
	const fn salt(self) -> u64 {
		match self {
			ValueStream::Create => 0x243F_6A88_85A3_08D3,
			ValueStream::Update => 0xB7E1_5162_8AED_2A6B,
			ValueStream::Query => 0xC13F_A9A9_02A6_328E,
		}
	}
}

/// Odd multiplier spreading consecutive sample indices before mixing.
const SAMPLE_ODD: u64 = 0x9E37_79B9_7F4A_7C15;

/// Derive the RNG seed for one sample of one stream.
///
/// SplitMix64's finalisation step decorrelates neighbouring sample indices, so
/// sample `n` and sample `n + 1` do not produce visibly related payloads.
fn sample_seed(seed: u64, stream: ValueStream, n: u32) -> u64 {
	let mut z = seed ^ stream.salt() ^ (n as u64).wrapping_mul(SAMPLE_ODD);
	z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
	z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
	z ^ (z >> 31)
}

/// Generates synthetic [`BenchValue`] payloads from a JSON template authored in
/// `bench.toml`. The template is parsed once into a [`ValueGenerator`] tree and
/// each call to [`Self::generate_value`] produces a fresh randomised
/// [`BenchValue`] following the schema.
///
/// A provider carrying a corpus seed (see [`Self::with_seed`]) can additionally
/// produce the payload for a given sample index as a pure function of that
/// seed, so the same index yields the same row in every client, every run and
/// every engine. Vector-search ground truth depends on that property: it lets
/// the corpus be reconstructed for scoring without reading a single row back.
pub(crate) struct ValueProvider {
	generator: ValueGenerator,
	rng: SmallRng,
	columns: Columns,
	seed: Option<u64>,
}

impl ValueProvider {
	/// Compile a [`ValueProvider`] from the JSON form of the configured value
	/// template.
	pub(crate) fn new(json: &str) -> Result<Self> {
		// Decode the JSON string
		let val = serde_json::from_str(json)?;
		debug!("Value template: {val:#}");
		// Compile a value generator
		let generator = ValueGenerator::new(val)?;
		// Identify the top-level columns for column-oriented backends
		let columns = Columns::new(&generator)?;
		Ok(Self {
			generator,
			columns,
			rng: rand::make_rng(),
			seed: None,
		})
	}

	/// Attach a corpus seed, making [`Self::generate_value_for`] deterministic.
	///
	/// Cluster centres are re-drawn from the seed as well, so two seeds give two
	/// genuinely different datasets rather than the same clusters populated
	/// differently.
	pub(crate) fn with_seed(mut self, seed: u64) -> Self {
		self.seed = Some(seed);
		self.generator.reseed_clusters(seed);
		self
	}

	/// The corpus seed, when this provider was built with one.
	pub(crate) fn seed(&self) -> Option<u64> {
		self.seed
	}

	/// Returns the schema's columns in their declared order.
	pub(crate) fn columns(&self) -> Columns {
		self.columns.clone()
	}

	/// Produce a single randomised [`BenchValue`] payload.
	pub(crate) fn generate_value(&mut self) -> BenchValue {
		self.generator.generate(&mut self.rng)
	}

	/// Produce the payload for sample `n` of `stream`.
	///
	/// With a corpus seed this is a pure function of `(seed, stream, n)`.
	/// Without one it draws from the provider's own stream and behaves exactly
	/// like [`Self::generate_value`], which keeps unseeded runs — every
	/// non-vector config — byte-for-byte unchanged.
	pub(crate) fn generate_value_for(&mut self, stream: ValueStream, n: u32) -> BenchValue {
		match self.seed {
			Some(seed) => {
				let mut rng = SmallRng::seed_from_u64(sample_seed(seed, stream, n));
				self.generator.generate(&mut rng)
			}
			None => self.generator.generate(&mut self.rng),
		}
	}

	/// Generate `count` standalone vectors shaped like the schema's `field`
	/// column, drawn deterministically from `seed`.
	///
	/// Queries come from the same generator as the corpus, so they follow the
	/// corpus distribution by construction rather than by a parallel
	/// implementation that could drift from it. Each query is seeded on its own
	/// index, so raising `count` extends the set instead of reshuffling it.
	pub(crate) fn generate_vectors(
		&self,
		field: &str,
		count: usize,
		seed: u64,
	) -> Result<Vec<Vec<f32>>> {
		let generator = self.vector_generator(field)?;
		let mut out = Vec::with_capacity(count);
		for i in 0..count {
			let mut rng = SmallRng::seed_from_u64(sample_seed(seed, ValueStream::Query, i as u32));
			match generator.generate(&mut rng) {
				BenchValue::FloatVector(v) => out.push(v),
				_ => bail!("field {field:?} did not generate a vector"),
			}
		}
		Ok(out)
	}

	/// Locate the generator for a top-level `vector:<dim>` column.
	fn vector_generator(&self, field: &str) -> Result<&ValueGenerator> {
		let ValueGenerator::Object(fields) = &self.generator else {
			bail!("value template must be an object");
		};
		let Some((_, generator)) = fields.iter().find(|(name, _)| name == field) else {
			bail!("field {field:?} is not present in the value template");
		};
		match generator {
			ValueGenerator::Vector {
				..
			}
			| ValueGenerator::ClusteredVector {
				..
			} => Ok(generator),
			other => bail!("field {field:?} is {other:?}, not a `vector:<dim>` column"),
		}
	}
}

impl Clone for ValueProvider {
	fn clone(&self) -> Self {
		Self {
			generator: self.generator.clone(),
			rng: rand::make_rng(),
			columns: self.columns.clone(),
			seed: self.seed,
		}
	}
}

#[derive(Clone, Debug)]
enum ValueGenerator {
	Bool,
	String(Length<usize>),
	Text(Length<usize>),
	Words(Length<usize>, Vec<String>),
	Integer,
	Float,
	DateTime,
	Uuid,
	Decimal,
	Bytes(Length<usize>),
	// We use i32 for better compatibility across DBs
	IntegerRange(Range<i32>),
	// We use f32 by default for better compatibility across DBs
	FloatRange(Range<f32>),
	DecimalRange(Range<f64>),
	StringEnum(Vec<String>),
	IntegerEnum(Vec<Number>),
	FloatEnum(Vec<Number>),
	DecimalEnum(Vec<Decimal>),
	Array(Vec<ValueGenerator>),
	Object(Vec<(String, ValueGenerator)>),
	/// Fixed-dimension f32 vector with a per-component uniform distribution.
	/// Used by the vector-search benchmark.
	Vector {
		dim: usize,
		lo: f32,
		hi: f32,
	},
	/// Fixed-dimension f32 vector drawn from a mixture of clusters on the unit
	/// sphere: pick a centroid, add Gaussian noise, renormalise.
	///
	/// Uniform components (see [`ValueGenerator::Vector`]) give a corpus with no
	/// neighbourhood structure — in high dimensions every pair sits at roughly
	/// the same distance, so a query's true neighbours are conspicuous and any
	/// graph walks straight to them. Recall then reads 1.0 for every index and
	/// discriminates nothing. Clusters put many plausible near-neighbours at
	/// similar distances, which is what an approximate index actually has to
	/// get right, and what real embeddings look like.
	ClusteredVector {
		dim: usize,
		/// Shared cluster centres. Every row must land in the same structure
		/// regardless of which client generated it, so these are fixed for the
		/// provider rather than drawn per row.
		centroids: Arc<Vec<Vec<f32>>>,
		/// Noise length relative to a unit centroid.
		sigma: f32,
	},
}

/// Cluster spread when `vector:<dim>:clustered:<n>` omits it.
///
/// At 0.35 a point sits well inside its own cluster while the cluster still has
/// real internal spread, so a query has many same-cluster candidates to confuse
/// an index without the clusters bleeding into one another.
const DEFAULT_CLUSTER_SIGMA: f32 = 0.35;

/// Seed for cluster centres before a corpus seed is attached.
const DEFAULT_CENTROID_SEED: u64 = 0x5EED_C0DE_CE47_401D;

/// Draw `clusters` unit-length centres, deterministically from `seed`.
///
/// Gaussian components normalised to length 1 give directions spread evenly over
/// the sphere; sampling components uniformly would bunch them toward the corners
/// of the cube instead.
fn cluster_centroids(dim: usize, clusters: usize, seed: u64) -> Vec<Vec<f32>> {
	let mut rng = SmallRng::seed_from_u64(seed);
	(0..clusters)
		.map(|_| {
			let mut v: Vec<f32> = (0..dim).map(|_| standard_normal(&mut rng)).collect();
			normalise(&mut v);
			v
		})
		.collect()
}

/// One draw from N(0, 1) by the Box-Muller transform.
fn standard_normal(rng: &mut SmallRng) -> f32 {
	// `u1` must exclude 0 or `ln` diverges.
	let u1: f32 = RandGen::random_range(rng, f32::EPSILON..1.0);
	let u2: f32 = RandGen::random_range(rng, 0.0..1.0);
	(-2.0 * u1.ln()).sqrt() * (std::f32::consts::TAU * u2).cos()
}

/// Scale a vector to unit length, leaving an all-zero vector untouched.
fn normalise(v: &mut [f32]) {
	let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
	if norm > 0.0 {
		for x in v.iter_mut() {
			*x /= norm;
		}
	}
}

const CHARSET: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn string(rng: &mut SmallRng, size: usize) -> String {
	(0..size)
		.map(|_| {
			let idx = RandGen::random_range(&mut *rng, 0..CHARSET.len());
			CHARSET[idx] as char
		})
		.collect()
}

fn string_range(rng: &mut SmallRng, range: Range<usize>) -> String {
	let l = RandGen::random_range(rng, range);
	string(rng, l)
}

fn text(rng: &mut SmallRng, size: usize) -> String {
	let mut l = 0;
	let mut words = Vec::with_capacity(size / 5);
	let mut i = 0;
	while l < size {
		let w = string_range(rng, 2..10);
		l += w.len();
		words.push(w);
		l += i;
		// We ignore the first whitespace, but not the following ones
		i = 1;
	}
	words.join(" ")
}

fn text_range(rng: &mut SmallRng, range: Range<usize>) -> String {
	let l = RandGen::random_range(rng, range);
	text(rng, l)
}

fn words(rng: &mut SmallRng, size: usize, dictionary: &[String]) -> String {
	let mut l = 0;
	let mut words = Vec::with_capacity(size / 5);
	let mut i = 0;
	while l < size {
		let w = dictionary[rng.random_range(0..dictionary.len())].as_str();
		l += w.len();
		words.push(w);
		l += i;
		// We ignore the first whitespace, but not the following ones
		i = 1;
	}
	words.join(" ")
}

fn words_range(rng: &mut SmallRng, range: Range<usize>, dictionary: &[String]) -> String {
	let l = rng.random_range(range);
	words(rng, l, dictionary)
}

fn bytes(rng: &mut SmallRng, size: usize) -> Vec<u8> {
	let mut buf = vec![0u8; size];
	for byte in buf.iter_mut() {
		*byte = RandGen::random_range(&mut *rng, 0u32..256u32) as u8;
	}
	buf
}

fn bytes_range(rng: &mut SmallRng, range: Range<usize>) -> Vec<u8> {
	let l = RandGen::random_range(rng, range);
	bytes(rng, l)
}

impl ValueGenerator {
	/// Re-draw cluster centres from the corpus seed, in place, throughout the
	/// template tree.
	fn reseed_clusters(&mut self, seed: u64) {
		match self {
			ValueGenerator::ClusteredVector {
				dim,
				centroids,
				..
			} => {
				// Salted so the centres are not the same draw as sample 0 of any
				// stream, which would correlate the structure with a row.
				let clusters = centroids.len();
				*centroids =
					Arc::new(cluster_centroids(*dim, clusters, seed ^ DEFAULT_CENTROID_SEED));
			}
			ValueGenerator::Array(items) => {
				for g in items {
					g.reseed_clusters(seed);
				}
			}
			ValueGenerator::Object(fields) => {
				for (_, g) in fields {
					g.reseed_clusters(seed);
				}
			}
			_ => {}
		}
	}

	fn new(value: Value) -> Result<Self> {
		match value {
			Value::Null => bail!("Unsupported type: Null"),
			Value::Bool(_) => bail!("Unsupported type: Bool"),
			Value::Number(_) => bail!("Unsupported type: Number"),
			Value::String(s) => Self::new_string(s),
			Value::Array(a) => Self::new_array(a),
			Value::Object(o) => Self::new_object(o),
		}
	}

	fn new_string(s: String) -> Result<Self> {
		let s = s.to_lowercase();
		let r = if let Some(i) = s.strip_prefix("string:") {
			Self::String(Length::new(i)?)
		} else if let Some(i) = s.strip_prefix("text:") {
			Self::Text(Length::new(i)?)
		} else if let Some(i) = s.strip_prefix("words:") {
			// Parse format: "words:50;word1,word2,word3"
			let parts: Vec<&str> = i.splitn(2, ';').collect();
			if parts.len() != 2 {
				bail!(
					"Words format requires length and dictionary separated by semicolon: words:50;word1,word2"
				);
			}
			let length = Length::new(parts[0])?;
			let dictionary: Vec<String> = parts[1].split(',').map(|s| s.to_string()).collect();
			if dictionary.is_empty() {
				bail!("Words dictionary cannot be empty");
			}
			Self::Words(length, dictionary)
		} else if let Some(i) = s.strip_prefix("int:") {
			if let Length::Range(r) = Length::new(i)? {
				Self::IntegerRange(r)
			} else {
				bail!("Expected a range but got: {i}");
			}
		} else if let Some(i) = s.strip_prefix("float:") {
			if let Length::Range(r) = Length::new(i)? {
				Self::FloatRange(r)
			} else {
				bail!("Expected a range but got: {i}");
			}
		} else if let Some(i) = s.strip_prefix("decimal:") {
			if let Length::Range(r) = Length::<f64>::new(i)? {
				Self::DecimalRange(r)
			} else {
				bail!("Expected a range but got: {i}");
			}
		} else if let Some(i) = s.strip_prefix("bytes:") {
			Self::Bytes(Length::new(i)?)
		} else if let Some(rest) = s.strip_prefix("vector:") {
			// `vector:<dim>` (uniform [-1, 1]), `vector:<dim>:<lo>..<hi>`, or
			// `vector:<dim>:clustered:<clusters>[:<sigma>]`.
			let (dim_str, range) = match rest.split_once(':') {
				Some((d, r)) => (d, Some(r)),
				None => (rest, None),
			};
			let dim: usize = dim_str
				.parse()
				.map_err(|e| anyhow!("invalid vector dimension {dim_str:?}: {e}"))?;
			if dim == 0 {
				bail!("vector dimension must be > 0");
			}
			if let Some(spec) = range.and_then(|r| r.strip_prefix("clustered:")) {
				let (clusters_str, sigma_str) = match spec.split_once(':') {
					Some((c, g)) => (c, Some(g)),
					None => (spec, None),
				};
				let clusters: usize = clusters_str
					.parse()
					.map_err(|e| anyhow!("invalid cluster count {clusters_str:?}: {e}"))?;
				if clusters == 0 {
					bail!("vector cluster count must be > 0");
				}
				// Scaled by 1/sqrt(dim) so the default means the same spread at
				// any dimension: the noise vector's length is `sigma` relative
				// to a unit-length centroid, rather than growing with `dim`.
				let sigma: f32 = match sigma_str {
					Some(g) => g.parse().map_err(|e| anyhow!("invalid cluster sigma: {e}"))?,
					None => DEFAULT_CLUSTER_SIGMA,
				};
				// Also rejects NaN, which would silently poison every centroid.
				if !sigma.is_finite() || sigma <= 0.0 {
					bail!("vector cluster sigma must be a finite value > 0");
				}
				return Ok(Self::ClusteredVector {
					dim,
					sigma,
					centroids: Arc::new(cluster_centroids(dim, clusters, DEFAULT_CENTROID_SEED)),
				});
			}
			let (lo, hi) = if let Some(r) = range {
				let parts: Vec<&str> = r.split("..").collect();
				if parts.len() != 2 {
					bail!("vector range must be lo..hi, got {r:?}");
				}
				let lo: f32 = parts[0].parse().map_err(|e| anyhow!("vector lo: {e}"))?;
				let hi: f32 = parts[1].parse().map_err(|e| anyhow!("vector hi: {e}"))?;
				if hi <= lo {
					bail!("vector range hi must be greater than lo");
				}
				(lo, hi)
			} else {
				(-1.0_f32, 1.0_f32)
			};
			Self::Vector {
				dim,
				lo,
				hi,
			}
		} else if let Some(s) = s.strip_prefix("string_enum:") {
			let labels = s.split(",").map(|s| s.to_string()).collect();
			Self::StringEnum(labels)
		} else if let Some(s) = s.strip_prefix("int_enum:") {
			let split: Vec<&str> = s.split(",").collect();
			let mut numbers = Vec::with_capacity(split.len());
			for s in split {
				numbers.push(s.parse::<i32>()?.into());
			}
			Self::IntegerEnum(numbers)
		} else if let Some(s) = s.strip_prefix("float_enum:") {
			let split: Vec<&str> = s.split(",").collect();
			let mut numbers = Vec::with_capacity(split.len());
			for s in split {
				numbers.push(Number::from_f64(s.parse::<f32>()? as f64).unwrap());
			}
			Self::FloatEnum(numbers)
		} else if let Some(s) = s.strip_prefix("decimal_enum:") {
			let split: Vec<&str> = s.split(",").collect();
			let mut numbers = Vec::with_capacity(split.len());
			for s in split {
				numbers.push(
					Decimal::from_str(s.trim())
						.map_err(|e| anyhow!("invalid decimal {s:?}: {e}"))?,
				);
			}
			Self::DecimalEnum(numbers)
		} else if s.eq("bool") {
			Self::Bool
		} else if s.eq("int") {
			Self::Integer
		} else if s.eq("float") {
			Self::Float
		} else if s.eq("decimal") {
			Self::Decimal
		} else if s.eq("datetime") {
			Self::DateTime
		} else if s.eq("uuid") {
			Self::Uuid
		} else {
			bail!("Unsupported type: {s}");
		};
		Ok(r)
	}

	fn new_array(a: Vec<Value>) -> Result<ValueGenerator> {
		let mut array = Vec::with_capacity(a.len());
		for v in a {
			array.push(ValueGenerator::new(v)?);
		}
		Ok(Self::Array(array))
	}

	fn new_object(o: Map<String, Value>) -> Result<ValueGenerator> {
		// BTreeMap sorts keys alphabetically; the column order should match
		// the JSON template's iteration order (which itself is alphabetical
		// from `serde_json::Map` once the template TOML is round-tripped).
		// Keep BTreeMap-equivalent ordering by sorting on insertion to remain
		// deterministic across runs and platforms.
		let mut tmp = BTreeMap::new();
		for (k, v) in o {
			tmp.insert(k, Self::new(v)?);
		}
		let map = tmp.into_iter().collect();
		Ok(Self::Object(map))
	}

	fn generate(&self, rng: &mut SmallRng) -> BenchValue {
		match self {
			ValueGenerator::Bool => {
				let v = RandGen::random_bool(&mut *rng, 0.5);
				BenchValue::Bool(v)
			}
			ValueGenerator::String(l) => {
				let val = match l {
					Length::Range(r) => string_range(rng, r.clone()),
					Length::Fixed(l) => string(rng, *l),
				};
				BenchValue::String(val)
			}
			ValueGenerator::Text(l) => {
				let val = match l {
					Length::Range(r) => text_range(rng, r.clone()),
					Length::Fixed(l) => text(rng, *l),
				};
				BenchValue::String(val)
			}
			ValueGenerator::Words(l, dictionary) => {
				let val = match l {
					Length::Range(r) => words_range(rng, r.clone(), dictionary),
					Length::Fixed(l) => words(rng, *l, dictionary),
				};
				BenchValue::String(val)
			}
			ValueGenerator::Integer => {
				let v: i32 = RandGen::random_range(&mut *rng, i32::MIN..i32::MAX);
				BenchValue::Int(v as i64)
			}
			ValueGenerator::Float => {
				let v = RandGen::random_range(&mut *rng, f32::MIN..f32::MAX);
				BenchValue::Float(v as f64)
			}
			ValueGenerator::DateTime => {
				// Number of seconds from Epoch to 31/12/2030
				let s = RandGen::random_range(&mut *rng, 0..1_924_991_999i64);
				let dt = Utc
					.timestamp_opt(s, 0)
					.single()
					.unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());
				BenchValue::DateTime(dt)
			}
			ValueGenerator::Uuid => BenchValue::Uuid(Uuid::new_v4()),
			ValueGenerator::Decimal => {
				// Generate a 4-fractional-digit decimal in [0, 1_000_000) so
				// the value fits comfortably in `NUMERIC(38, 10)` and similar.
				let v: i64 = RandGen::random_range(&mut *rng, 0..10_000_000_000i64);
				let d = Decimal::new(v, 4);
				BenchValue::Decimal(d)
			}
			ValueGenerator::Bytes(l) => {
				let buf = match l {
					Length::Range(r) => bytes_range(rng, r.clone()),
					Length::Fixed(l) => bytes(rng, *l),
				};
				BenchValue::Bytes(buf)
			}
			ValueGenerator::IntegerRange(r) => {
				let v: i32 = rng.random_range(r.start..r.end);
				BenchValue::Int(v as i64)
			}
			ValueGenerator::FloatRange(r) => {
				let v = rng.random_range(r.start..r.end);
				BenchValue::Float(v as f64)
			}
			ValueGenerator::DecimalRange(r) => {
				let v = rng.random_range(r.start..r.end);
				let d = Decimal::try_from(v).unwrap_or(Decimal::ZERO);
				BenchValue::Decimal(d)
			}
			ValueGenerator::StringEnum(a) => {
				let i = rng.random_range(0..a.len());
				BenchValue::String(a[i].to_string())
			}
			ValueGenerator::IntegerEnum(a) => {
				let i = rng.random_range(0..a.len());
				let n = &a[i];
				if let Some(i) = n.as_i64() {
					BenchValue::Int(i)
				} else if let Some(u) = n.as_u64() {
					BenchValue::UInt(u)
				} else {
					BenchValue::Float(n.as_f64().unwrap_or(0.0))
				}
			}
			ValueGenerator::FloatEnum(a) => {
				let i = rng.random_range(0..a.len());
				BenchValue::Float(a[i].as_f64().unwrap_or(0.0))
			}
			ValueGenerator::DecimalEnum(a) => {
				let i = rng.random_range(0..a.len());
				BenchValue::Decimal(a[i])
			}
			ValueGenerator::Array(a) => {
				let mut vec = Vec::with_capacity(a.len());
				for v in a {
					vec.push(v.generate(rng));
				}
				BenchValue::Array(vec)
			}
			ValueGenerator::Object(o) => {
				let mut vec = Vec::with_capacity(o.len());
				for (k, v) in o {
					vec.push((k.clone(), v.generate(rng)));
				}
				BenchValue::Object(vec)
			}
			ValueGenerator::Vector {
				dim,
				lo,
				hi,
			} => {
				let mut buf = Vec::with_capacity(*dim);
				for _ in 0..*dim {
					buf.push(rng.random_range(*lo..*hi));
				}
				BenchValue::FloatVector(buf)
			}
			ValueGenerator::ClusteredVector {
				dim,
				centroids,
				sigma,
			} => {
				let centroid = &centroids[rng.random_range(0..centroids.len())];
				// `sigma / sqrt(dim)` per component makes the noise vector's
				// length `sigma` overall, so the spread means the same thing at
				// any dimension.
				let scale = *sigma / (*dim as f32).sqrt();
				let mut buf: Vec<f32> =
					centroid.iter().map(|c| c + scale * standard_normal(rng)).collect();
				// Real embeddings are commonly unit-normalised, and it keeps
				// cosine and inner-product rankings consistent with each other.
				normalise(&mut buf);
				BenchValue::FloatVector(buf)
			}
		}
	}
}

#[derive(Clone, Debug)]
enum Length<Idx>
where
	Idx: FromStr,
{
	Range(Range<Idx>),
	Fixed(Idx),
}

impl<Idx> Length<Idx>
where
	Idx: FromStr,
{
	fn new(s: &str) -> Result<Self>
	where
		<Idx as FromStr>::Err: Display,
	{
		// Get the length config setting
		let parts: Vec<&str> = s.split("..").collect();
		// Check the length parameter
		let r = match parts.len() {
			2 => {
				let min = Idx::from_str(parts[0]).map_err(|e| anyhow!("{e}"))?;
				let max = Idx::from_str(parts[1]).map_err(|e| anyhow!("{e}"))?;
				Self::Range(min..max)
			}
			1 => Self::Fixed(Idx::from_str(parts[0]).map_err(|e| anyhow!("{e}"))?),
			v => {
				bail!("Invalid length generation value: {v}");
			}
		};
		Ok(r)
	}
}

/// The schema columns derived from the value template, used to build column
/// definitions (DDL) and to bind parameter values in column-oriented backends.
#[derive(Clone, Debug)]
pub(crate) struct Columns(pub(crate) Vec<(String, ColumnType)>);

impl Columns {
	fn new(value: &ValueGenerator) -> Result<Self> {
		if let ValueGenerator::Object(o) = value {
			let mut columns = Vec::with_capacity(o.len());
			for (f, g) in o {
				columns.push((f.to_string(), ColumnType::new(g)?));
			}
			Ok(Columns(columns))
		} else {
			bail!("An object was expected, but got: {value:?}");
		}
	}
}

/// The set of column types backends can target. Each variant maps directly to
/// at least one [`BenchValue`] variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnType {
	/// UTF-8 text column.
	String,
	/// 32-bit / 64-bit signed integer column.
	Integer,
	/// 32-bit floating-point column.
	Float,
	/// Arbitrary-precision decimal column.
	Decimal,
	/// UTC datetime column.
	DateTime,
	/// UUID column.
	Uuid,
	/// JSON object column.
	Object,
	/// JSON array column.
	Array,
	/// Boolean column.
	Bool,
	/// Opaque byte payload column.
	Bytes,
	/// Fixed-dimension f32 vector column for vector-search backends.
	FloatVector(usize),
}

impl ColumnType {
	fn new(v: &ValueGenerator) -> Result<Self> {
		let r = match v {
			ValueGenerator::Object(_) => ColumnType::Object,
			ValueGenerator::Array(_) => ColumnType::Array,
			ValueGenerator::StringEnum(_)
			| ValueGenerator::String(_)
			| ValueGenerator::Text(_)
			| ValueGenerator::Words(_, _) => ColumnType::String,
			ValueGenerator::Integer
			| ValueGenerator::IntegerRange(_)
			| ValueGenerator::IntegerEnum(_) => ColumnType::Integer,
			ValueGenerator::Float
			| ValueGenerator::FloatRange(_)
			| ValueGenerator::FloatEnum(_) => ColumnType::Float,
			ValueGenerator::Decimal
			| ValueGenerator::DecimalRange(_)
			| ValueGenerator::DecimalEnum(_) => ColumnType::Decimal,
			ValueGenerator::DateTime => ColumnType::DateTime,
			ValueGenerator::Bool => ColumnType::Bool,
			ValueGenerator::Uuid => ColumnType::Uuid,
			ValueGenerator::Bytes(_) => ColumnType::Bytes,
			ValueGenerator::Vector {
				dim,
				..
			}
			| ValueGenerator::ClusteredVector {
				dim,
				..
			} => ColumnType::FloatVector(*dim),
		};
		Ok(r)
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use tokio::task;

	fn vector_of(template: &str, seed: u64, sample: u32) -> Vec<f32> {
		ValueProvider::new(template)
			.unwrap()
			.with_seed(seed)
			.generate_value_for(ValueStream::Create, sample)
			.get_field("v")
			.and_then(|v| v.as_float_vector())
			.unwrap()
			.to_vec()
	}

	#[test]
	fn clustered_vectors_are_unit_length() {
		let v = vector_of(r#"{ "v": "vector:32:clustered:8" }"#, 1, 0);
		assert_eq!(v.len(), 32);
		let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
		assert!((norm - 1.0).abs() < 1e-4, "norm was {norm}");
	}

	/// The corpus has to be reconstructible for ground truth, which means the
	/// cluster draw must be part of that determinism, not an extra source of
	/// randomness on top of it.
	#[test]
	fn clustered_vectors_are_deterministic() {
		let tmpl = r#"{ "v": "vector:32:clustered:8" }"#;
		assert_eq!(vector_of(tmpl, 1, 7), vector_of(tmpl, 1, 7));
		assert_ne!(vector_of(tmpl, 1, 7), vector_of(tmpl, 1, 8));
	}

	/// Two corpus seeds should give two different datasets, cluster centres
	/// included — otherwise every seed reuses one structure.
	#[test]
	fn cluster_centres_follow_the_corpus_seed() {
		let tmpl = r#"{ "v": "vector:32:clustered:4" }"#;
		assert_ne!(vector_of(tmpl, 1, 0), vector_of(tmpl, 2, 0));
	}

	/// Tighter clusters must actually be tighter, or `sigma` means nothing.
	#[test]
	fn sigma_controls_cluster_spread() {
		fn spread(sigma: &str) -> f32 {
			let tmpl = format!(r#"{{ "v": "vector:64:clustered:4:{sigma}" }}"#);
			let mut vp = ValueProvider::new(&tmpl).unwrap().with_seed(9);
			let vs: Vec<Vec<f32>> = (0..200u32)
				.map(|i| {
					vp.generate_value_for(ValueStream::Create, i)
						.get_field("v")
						.and_then(|v| v.as_float_vector())
						.expect("template declares `v` as a vector column")
						.to_vec()
				})
				.collect();
			// Mean pairwise cosine distance over a fixed sample of pairs.
			let mut total = 0.0;
			let mut n = 0;
			for i in 0..vs.len() {
				for j in (i + 1)..vs.len() {
					total += 1.0 - vs[i].iter().zip(&vs[j]).map(|(a, b)| a * b).sum::<f32>();
					n += 1;
				}
			}
			total / n as f32
		}
		assert!(spread("0.15") < spread("0.80"), "tighter sigma should cluster more closely");
	}

	#[test]
	fn clustered_template_rejects_bad_parameters() {
		for bad in [
			r#"{ "v": "vector:8:clustered:0" }"#,
			r#"{ "v": "vector:8:clustered:4:0" }"#,
			r#"{ "v": "vector:8:clustered:4:-1" }"#,
			r#"{ "v": "vector:8:clustered:x" }"#,
		] {
			assert!(ValueProvider::new(bad).is_err(), "should have rejected {bad}");
		}
	}

	#[tokio::test]
	async fn check_all_values_are_unique() {
		let vp = ValueProvider::new(r#"{ "int": "int", "int_range": "int:1..99"}"#).unwrap();
		let mut v = vp.clone();
		let f1 = task::spawn(async move { (v.generate_value(), v.generate_value()) });
		let mut v = vp.clone();
		let f2 = task::spawn(async move { (v.generate_value(), v.generate_value()) });
		let (v1a, v1b) = f1.await.unwrap();
		let (v2a, v2b) = f2.await.unwrap();
		assert_ne!(v1a, v1b);
		assert_ne!(v2a, v2b);
		assert_ne!(v1a, v2a);
		assert_ne!(v1b, v2b);
	}
}
