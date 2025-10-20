use crate::dialect::Dialect;
use anyhow::{Result, anyhow, bail};
use log::debug;
use rand::prelude::SmallRng;
use rand::{Rng as RandGen, SeedableRng};
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Range;
use std::str::FromStr;
use uuid::Uuid;

pub(crate) struct ValueProvider {
	generator: ValueGenerator,
	rng: SmallRng,
	columns: Columns,
}

impl ValueProvider {
	pub(crate) fn new(json: &str) -> Result<Self> {
		// Decode the JSON string
		let val = serde_json::from_str(json)?;
		debug!("Value template: {val:#}");
		// Compile a value generator
		let generator = ValueGenerator::new(val)?;
		// Identifies the field in the top level (used for column oriented DB like Postgresql)
		let columns = Columns::new(&generator)?;
		Ok(Self {
			generator,
			columns,
			rng: SmallRng::from_os_rng(),
		})
	}

	pub(crate) fn columns(&self) -> Columns {
		self.columns.clone()
	}

	pub(crate) fn generate_value<D>(&mut self) -> Value
	where
		D: Dialect,
	{
		self.generator.generate::<D>(&mut self.rng)
	}
}

impl Clone for ValueProvider {
	fn clone(&self) -> Self {
		Self {
			generator: self.generator.clone(),
			rng: SmallRng::from_os_rng(),
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
	// We use i32 for better compatibility across DBs
	IntegerRange(Range<i32>),
	// We use f32 by default for better compatibility across DBs
	FloatRange(Range<f32>),
	StringEnum(Vec<String>),
	IntegerEnum(Vec<Number>),
	FloatEnum(Vec<Number>),
	Array(Vec<ValueGenerator>),
	Object(BTreeMap<String, ValueGenerator>),
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
		} else if s.eq("bool") {
			Self::Bool
		} else if s.eq("int") {
			Self::Integer
		} else if s.eq("float") {
			Self::Float
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
		let mut map = BTreeMap::new();
		for (k, v) in o {
			map.insert(k, Self::new(v)?);
		}
		Ok(Self::Object(map))
	}

	fn generate<D>(&self, rng: &mut SmallRng) -> Value
	where
		D: Dialect,
	{
		match self {
			ValueGenerator::Bool => {
				let v = RandGen::random_bool(&mut *rng, 0.5);
				Value::Bool(v)
			}
			ValueGenerator::String(l) => {
				let val = match l {
					Length::Range(r) => string_range(rng, r.clone()),
					Length::Fixed(l) => string(rng, *l),
				};
				Value::String(val)
			}
			ValueGenerator::Text(l) => {
				let val = match l {
					Length::Range(r) => text_range(rng, r.clone()),
					Length::Fixed(l) => text(rng, *l),
				};
				Value::String(val)
			}
			ValueGenerator::Words(l, dictionary) => {
				let val = match l {
					Length::Range(r) => words_range(rng, r.clone(), dictionary),
					Length::Fixed(l) => words(rng, *l, dictionary),
				};
				Value::String(val)
			}
			ValueGenerator::Integer => {
				let v = RandGen::random_range(&mut *rng, i32::MIN..i32::MAX);
				Value::Number(Number::from(v))
			}
			ValueGenerator::Float => {
				let v = RandGen::random_range(&mut *rng, f32::MIN..f32::MAX);
				Value::Number(Number::from_f64(v as f64).unwrap())
			}
			ValueGenerator::DateTime => {
				// Number of seconds from Epoch to 31/12/2030
				let s = RandGen::random_range(&mut *rng, 0..1_924_991_999);
				D::date_time(s)
			}
			ValueGenerator::Uuid => {
				let uuid = Uuid::new_v4();
				D::uuid(uuid)
			}
			ValueGenerator::IntegerRange(r) => {
				let v = rng.random_range(r.start..r.end);
				Value::Number(v.into())
			}
			ValueGenerator::FloatRange(r) => {
				let v = rng.random_range(r.start..r.end);
				Value::Number(Number::from_f64(v as f64).unwrap())
			}
			ValueGenerator::StringEnum(a) => {
				let i = rng.random_range(0..a.len());
				Value::String(a[i].to_string())
			}
			ValueGenerator::IntegerEnum(a) => {
				let i = rng.random_range(0..a.len());
				Value::Number(a[i].clone())
			}
			ValueGenerator::FloatEnum(a) => {
				let i = rng.random_range(0..a.len());
				Value::Number(a[i].clone())
			}
			ValueGenerator::Array(a) => {
				// Generate any array structure values
				let mut vec = Vec::with_capacity(a.len());
				for v in a {
					vec.push(v.generate::<D>(rng));
				}
				Value::Array(vec)
			}
			ValueGenerator::Object(o) => {
				// Generate any object structure values
				let mut map = Map::<String, Value>::new();
				for (k, v) in o {
					map.insert(k.clone(), v.generate::<D>(rng));
				}
				Value::Object(map)
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

#[derive(Clone, Debug)]
/// This structures defines the main columns use for create the schema
/// and insert generated data into a column-oriented database (PostreSQL).
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

#[derive(Clone, Debug)]
pub(crate) enum ColumnType {
	String,
	Integer,
	Float,
	DateTime,
	Uuid,
	Object,
	Bool,
}

impl ColumnType {
	fn new(v: &ValueGenerator) -> Result<Self> {
		let r = match v {
			ValueGenerator::Object(_) => ColumnType::Object,
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
			ValueGenerator::DateTime => ColumnType::DateTime,
			ValueGenerator::Bool => ColumnType::Bool,
			ValueGenerator::Uuid => ColumnType::Uuid,
			t => {
				bail!("Invalid data type: {t:?}");
			}
		};
		Ok(r)
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::dialect::AnsiSqlDialect;
	use tokio::task;

	#[tokio::test]
	async fn check_all_values_are_unique() {
		let vp = ValueProvider::new(r#"{ "int": "int", "int_range": "int:1..99"}"#).unwrap();
		let mut v = vp.clone();
		let f1 = task::spawn(async move {
			(v.generate_value::<AnsiSqlDialect>(), v.generate_value::<AnsiSqlDialect>())
		});
		let mut v = vp.clone();
		let f2 = task::spawn(async move {
			(v.generate_value::<AnsiSqlDialect>(), v.generate_value::<AnsiSqlDialect>())
		});
		let (v1a, v1b) = f1.await.unwrap();
		let (v2a, v2b) = f2.await.unwrap();
		assert_ne!(v1a, v1b);
		assert_ne!(v2a, v2b);
		assert_ne!(v1a, v2a);
		assert_ne!(v1b, v2b);
	}
}
