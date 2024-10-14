use anyhow::{bail, Result};
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use serde_json::{Map, Number, Value};
use std::str::FromStr;

#[derive(Clone)]
pub(crate) struct ValueProvider {
	val: Value,
	rng: SmallRng,
	columns: Columns,
}

#[derive(Clone)]
pub(crate) struct Columns(pub(crate) Vec<(String, ColumnType)>);

#[derive(Clone)]
pub(crate) enum ColumnType {
	String,
	Integer,
	Object,
}

impl ColumnType {
	fn to_sql_string(&self, val: &Value) -> String {
		match self {
			Self::String => format!("'{}'", val.as_str().unwrap()),
			Self::Object => {
				format!("'{}'", serde_json::to_string(val.as_object().unwrap()).unwrap())
			}
			Self::Integer => val.to_string(),
		}
	}
}

impl Columns {
	pub(crate) fn insert_clauses(&self, val: Value) -> Result<(String, String)> {
		let val = val.as_object().unwrap();
		let mut fields = Vec::with_capacity(self.0.len());
		let mut values = Vec::with_capacity(self.0.len());
		for (n, t) in &self.0 {
			fields.push(n.to_string());
			let value = t.to_sql_string(val.get(n).unwrap());
			values.push(value);
		}
		let fields = fields.join(",");
		let values = values.join(",");
		Ok((fields, values))
	}

	pub(crate) fn set_clause(&self, val: Value) -> Result<String> {
		let mut updates = Vec::with_capacity(self.0.len());
		let val = val.as_object().unwrap();
		for (n, t) in &self.0 {
			let value = t.to_sql_string(val.get(n).unwrap());
			updates.push(format!("{n}={value}"));
		}
		Ok(updates.join(","))
	}
}

const CHARSET: &[u8; 37] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789";

impl ValueProvider {
	pub(crate) fn new(json: &str) -> Result<Self> {
		let val = serde_json::from_str(json)?;
		let columns = Self::parse_columns(&val)?;
		Ok(Self {
			val,
			columns,
			rng: SmallRng::from_entropy(),
		})
	}

	fn parse_columns(val: &Value) -> Result<Columns> {
		let o = val.as_object().unwrap();
		let mut columns = Vec::with_capacity(o.len());
		for (f, t) in o {
			// Arrays
			if t.is_object() {
				columns.push((f.to_string(), ColumnType::Object));
			} else if let Some(str) = t.as_str() {
				let str = str.to_ascii_lowercase();
				if str.starts_with("string") {
					columns.push((f.to_string(), ColumnType::String));
				} else if str.eq("i32") {
					columns.push((f.to_string(), ColumnType::Integer));
				} else {
					bail!("Invalid JSON type: {str}");
				}
			} else {
				bail!("Unsupported JSON type: {t}");
			}
		}
		Ok(Columns(columns))
	}

	pub(crate) fn columns(&self) -> Columns {
		self.columns.clone()
	}

	pub(crate) fn generate_value(&mut self) -> Result<Value> {
		Self::parse_value(&mut self.rng, &self.val)
	}
	fn parse_value(rng: &mut SmallRng, value: &Value) -> Result<Value> {
		if let Some(object) = value.as_object() {
			let mut map = Map::<String, Value>::new();
			for (key, value) in object {
				map.insert(key.clone(), Self::parse_value(rng, value)?);
			}
			Ok(Value::Object(map))
		} else if let Some(array) = value.as_array() {
			let mut vec = Vec::with_capacity(array.len());
			for value in array {
				vec.push(Self::parse_value(rng, value)?);
			}
			Ok(Value::Array(vec))
		} else if let Some(str) = value.as_str() {
			let str = str.to_ascii_lowercase();
			if let Some(size) = str.strip_prefix("string") {
				let size = usize::from_str(size)?;
				let random_string: String = (0..size)
					.map(|_| {
						let idx = rng.gen_range(0..CHARSET.len());
						CHARSET[idx] as char
					})
					.collect();
				Ok(Value::String(random_string))
			} else if str.eq("i32") {
				let n: i16 = rng.gen();
				Ok(Value::Number(Number::from(n)))
			} else {
				bail!("Invalid JSON type: {str}");
			}
		} else {
			bail!("Unsupported JSON type: {value}");
		}
	}
}
