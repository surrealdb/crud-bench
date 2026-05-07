//! Row payloads encoded with Bincode. [`serde_json::Value`] cannot be decoded from Bincode
//! (`Serde(AnyNotSupported)`) because its `Deserialize` implementation uses `deserialize_any`.
//! We convert through [`BincodeVal`], a concrete tree type Bincode handles well.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

/// JSON-shaped value without `deserialize_any`; safe for `bincode::serde::decode_from_slice`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum BincodeVal {
	Null,
	Bool(bool),
	Num(JsonNum),
	String(String),
	Array(Vec<BincodeVal>),
	Object(Vec<(String, BincodeVal)>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum JsonNum {
	U(u64),
	I(i64),
	F(f64),
}

impl TryFrom<&Value> for BincodeVal {
	type Error = anyhow::Error;

	fn try_from(v: &Value) -> Result<Self> {
		Ok(match v {
			Value::Null => BincodeVal::Null,
			Value::Bool(b) => BincodeVal::Bool(*b),
			Value::Number(n) => BincodeVal::Num(JsonNum::try_from(n)?),
			Value::String(s) => BincodeVal::String(s.clone()),
			Value::Array(a) => {
				let mut out = Vec::with_capacity(a.len());
				for x in a {
					out.push(BincodeVal::try_from(x)?);
				}
				BincodeVal::Array(out)
			}
			Value::Object(o) => {
				let mut pairs = Vec::with_capacity(o.len());
				for (k, xv) in o {
					pairs.push((k.clone(), BincodeVal::try_from(xv)?));
				}
				BincodeVal::Object(pairs)
			}
		})
	}
}

impl TryFrom<&Number> for JsonNum {
	type Error = anyhow::Error;

	fn try_from(n: &Number) -> Result<Self> {
		if let Some(i) = n.as_i64() {
			return Ok(JsonNum::I(i));
		}
		if let Some(u) = n.as_u64() {
			return Ok(JsonNum::U(u));
		}
		let f = n.as_f64().context("JSON number is not representable as i64, u64, or f64")?;
		Ok(JsonNum::F(f))
	}
}

impl From<BincodeVal> for Value {
	fn from(v: BincodeVal) -> Self {
		match v {
			BincodeVal::Null => Value::Null,
			BincodeVal::Bool(b) => Value::Bool(b),
			BincodeVal::Num(n) => {
				let num = match n {
					JsonNum::U(u) => Number::from(u),
					JsonNum::I(i) => Number::from(i),
					JsonNum::F(f) => Number::from_f64(f).unwrap_or_else(|| Number::from(0i64)),
				};
				Value::Number(num)
			}
			BincodeVal::String(s) => Value::String(s),
			BincodeVal::Array(a) => Value::Array(a.into_iter().map(Value::from).collect()),
			BincodeVal::Object(pairs) => {
				Value::Object(pairs.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
			}
		}
	}
}

pub(crate) fn encode_value(val: &Value) -> Result<Vec<u8>> {
	let wire = BincodeVal::try_from(val).context("convert row to bincode wire value")?;
	bincode::serde::encode_to_vec(&wire, bincode::config::standard()).map_err(Into::into)
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<Value> {
	let (wire, _) =
		bincode::serde::decode_from_slice::<BincodeVal, _>(bytes, bincode::config::standard())
			.map_err(|e| anyhow::anyhow!(e))?;
	Ok(Value::from(wire))
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde_json::json;

	#[test]
	fn roundtrip_nested_array_like_tags() {
		let v = json!({
			"name": "x",
			"tags": ["alpha", "beta", "gamma"],
			"n": 42,
		});
		let bytes = encode_value(&v).unwrap();
		let out = decode_value(&bytes).unwrap();
		assert_eq!(v, out);
	}
}
