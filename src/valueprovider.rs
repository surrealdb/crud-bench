use anyhow::{bail, Result};
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::str::FromStr;

#[derive(Clone)]
pub(crate) struct ValueProvider {
	value: Value,
	rng: SmallRng,
}

const CHARSET: &[u8; 37] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789";

impl ValueProvider {
	pub(crate) fn new(json: &str) -> Result<Self> {
		let value = serde_json::from_str(json)?;
		Ok(Self {
			value,
			rng: SmallRng::from_entropy(),
		})
	}

	pub(crate) fn columns(&self) -> Option<&Map<String, Value>> {
		self.value.as_object()
	}

	pub(crate) fn generate_value(&mut self) -> Result<Value> {
		Self::explore_value(&mut self.rng, &self.value)
	}

	fn explore_value(rng: &mut SmallRng, value: &Value) -> Result<Value> {
		if let Some(object) = value.as_object() {
			let mut map = Map::<String, Value>::new();
			for (key, value) in object {
				map.insert(key.clone(), Self::explore_value(rng, value)?);
			}
			Ok(Value::Object(map))
		} else if let Some(array) = value.as_array() {
			let mut vec = Vec::with_capacity(array.len());
			for value in array {
				vec.push(Self::explore_value(rng, value)?);
			}
			Ok(Value::Array(vec))
		} else if let Some(str) = value.as_str() {
			if let Some(size) = str.strip_prefix("String") {
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
	pub(crate) fn sample(&mut self) -> Record {
		Record {
			integer: self.rng.gen(),
			text: (0..50)
				.map(|_| {
					let idx = self.rng.gen_range(0..CHARSET.len());
					CHARSET[idx] as char
				})
				.collect(),
		}
	}
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub(crate) struct Record {
	pub(crate) text: String,
	pub(crate) integer: i32,
}
