use crate::value::BenchValue;
use anyhow::{Result, anyhow, bail};
use chrono::{TimeZone, Utc};
use log::debug;
use rand::RngExt as RandGen;
use rand::prelude::SmallRng;
use rust_decimal::Decimal;
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Range;
use std::str::FromStr;
use uuid::Uuid;

/// Generates synthetic [`BenchValue`] payloads from a JSON template authored in
/// `bench.toml`. The template is parsed once into a [`ValueGenerator`] tree and
/// each call to [`Self::generate_value`] produces a fresh randomised
/// [`BenchValue`] following the schema.
pub(crate) struct ValueProvider {
	generator: ValueGenerator,
	rng: SmallRng,
	columns: Columns,
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
		})
	}

	/// Returns the schema's columns in their declared order.
	pub(crate) fn columns(&self) -> Columns {
		self.columns.clone()
	}

	/// Produce a single randomised [`BenchValue`] payload.
	pub(crate) fn generate_value(&mut self) -> BenchValue {
		self.generator.generate(&mut self.rng)
	}
}

impl Clone for ValueProvider {
	fn clone(&self) -> Self {
		Self {
			generator: self.generator.clone(),
			rng: rand::make_rng(),
			columns: self.columns.clone(),
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
		};
		Ok(r)
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use tokio::task;

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
