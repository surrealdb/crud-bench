use crate::dialect::Dialect;
use anyhow::{anyhow, bail, Result};
use fake::Fake;
use log::debug;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Range;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone)]
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
		// Identifies the field in the top level (used for column oriented DB lijke Postgresql)
		let columns = Columns::new(&generator)?;
		Ok(Self {
			generator,
			columns,
			rng: SmallRng::from_entropy(),
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

#[derive(Clone, Debug)]
enum ValueGenerator {
	Bool,
	String(Length<usize>),
	Text(Length<usize>),
	Integer,
	Float,
	DateTime,
	Uuid,
	IntegerRange(Range<i64>),
	FloatRange(Range<f64>),
	StringEnum(Vec<String>),
	Array(Vec<ValueGenerator>),
	Object(BTreeMap<String, ValueGenerator>),
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
		} else if let Some(i) = s.strip_prefix("int:") {
			if let Length::Range(r) = Length::new(i)? {
				Self::IntegerRange(r)
			} else {
				bail!("Expected a range but got: {i}");
			}
		} else if let Some(i) = s.strip_prefix("enum:") {
			let labels = i.split(",").map(|s| s.to_string()).collect();
			Self::StringEnum(labels)
		} else if s.eq("bool") {
			Self::Bool
		} else if s.eq("int") {
			Self::Integer
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
				let v = rng.gen::<bool>();
				Value::Bool(v)
			}
			ValueGenerator::String(l) => {
				let val = match l {
					Length::Range(r) => r.fake::<String>(),
					Length::Fixed(l) => l.fake::<String>(),
				};
				Value::String(val)
			}
			ValueGenerator::Text(l) => {
				let val = match l {
					Length::Range(r) => fake::faker::lorem::en::Paragraph(r.clone()).fake(),
					Length::Fixed(l) => fake::faker::lorem::en::Paragraph(*l..*l).fake(),
				};
				Value::String(val)
			}
			ValueGenerator::Integer => {
				let v = rng.gen::<i64>();
				Value::Number(Number::from(v))
			}
			ValueGenerator::Float => {
				let v = rng.gen::<f64>();
				Value::Number(Number::from_f64(v).unwrap())
			}
			ValueGenerator::DateTime => {
				// Number of seconds from Epoch to 31/12/2030
				let s = rng.gen_range(0..1_924_991_999);
				D::date_time(s)
			}
			ValueGenerator::Uuid => {
				let uuid = Uuid::new_v4();
				D::uuid(uuid)
			}
			ValueGenerator::IntegerRange(r) => {
				let v = rng.gen_range(r.start..r.end);
				Value::Number(v.into())
			}
			ValueGenerator::FloatRange(r) => {
				let v = rng.gen_range(r.start..r.end);
				Value::Number(Number::from_f64(v).unwrap())
			}
			ValueGenerator::StringEnum(a) => {
				let i = rng.gen_range(0..a.len());
				Value::String(a[i].to_string())
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
		let gen: Vec<&str> = s.split("..").collect();
		// Check the length parameter
		let r = match gen.len() {
			2 => {
				let min = Idx::from_str(gen[0]).map_err(|e| anyhow!("{e}"))?;
				let max = Idx::from_str(gen[1]).map_err(|e| anyhow!("{e}"))?;
				Self::Range(min..max)
			}
			1 => Self::Fixed(Idx::from_str(gen[0]).map_err(|e| anyhow!("{e}"))?),
			v => {
				bail!("Invalid length generation value: {v}");
			}
		};
		Ok(r)
	}
}

#[derive(Clone)]
/// This structures defines the main columns use for create the schema
/// and insert generated data into a column-oriented database (PostreSQL).
pub(crate) struct Columns(pub(crate) Vec<(String, ColumnType)>);

impl Columns {
	fn new(value: &ValueGenerator) -> Result<Self> {
		if let ValueGenerator::Object(o) = value {
			let mut columns = Vec::with_capacity(o.len());
			for (f, _) in o {
				columns.push((f.to_string(), ColumnType::Object));
			}
			Ok(Columns(columns))
		} else {
			bail!("An object was expected, but got: {value:?}");
		}
	}
}

#[derive(Clone)]
pub(crate) enum ColumnType {
	String,
	Integer,
	DateTime,
	Uuid,
	Object,
	Bool,
}

impl ColumnType {
	fn new(v: &ValueGenerator) -> Result<Self> {
		let r = match v {
			ValueGenerator::Object(_) => ColumnType::Object,
			ValueGenerator::StringEnum(_) | ValueGenerator::String(_) | ValueGenerator::Text(_) => {
				ColumnType::String
			}
			ValueGenerator::Integer | ValueGenerator::IntegerRange(_) => ColumnType::Integer,
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

impl Columns {
	pub(crate) fn insert_clauses<D>(&self, val: Value) -> Result<(String, String)>
	where
		D: Dialect,
	{
		let val = val.as_object().unwrap();
		let mut fields = Vec::with_capacity(self.0.len());
		let mut values = Vec::with_capacity(self.0.len());
		for (f, v) in val {
			fields.push(f.to_string());
			values.push(D::arg_string(v));
		}
		let fields = fields.join(",");
		let values = values.join(",");
		Ok((fields, values))
	}

	pub(crate) fn set_clause<D>(&self, val: Value) -> Result<String>
	where
		D: Dialect,
	{
		let mut updates = Vec::with_capacity(self.0.len());
		let val = val.as_object().unwrap();
		for (f, v) in val {
			let value = D::arg_string(v);
			updates.push(format!("{f}={value}"));
		}
		Ok(updates.join(","))
	}
}
