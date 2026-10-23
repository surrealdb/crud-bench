//! Typed runtime value used across crud-bench backends.
//!
//! `BenchValue` carries native types (`Uuid`, `DateTime`, `Decimal`, `Bytes`)
//! end-to-end through the benchmark, replacing the previous `serde_json::Value`
//! representation. `serde_json::Value` is still used at three boundaries:
//!
//! 1. Reading the JSON template from `bench.toml`.
//! 2. Emitting the representative `sample` field in `BenchmarkResult`.
//! 3. Cells inside JSONB / JSON-text columns in SQL backends, and array /
//!    object leaves in document stores that consume serde-serialised JSON.
//!
//! Backends convert `BenchValue` to their native parameter type with a single
//! match per variant, so per-row UUID/datetime parsing on the client side is
//! eliminated.

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value as JsonValue};
use std::str::FromStr;
use uuid::Uuid;

/// Typed value carried through the benchmark hot-path. Variants align with the
/// generator config tags exposed by `ValueProvider` and the column types
/// declared by `Columns`. Object preserves insertion order via a Vec so that
/// schemas defined in `bench.toml` produce deterministic column orderings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum BenchValue {
	/// Absent value.
	Null,
	/// Boolean scalar.
	Bool(bool),
	/// Signed integer scalar (i64-wide for SQL `BIGINT` and JSON compatibility).
	Int(i64),
	/// Unsigned integer scalar (u64-wide for keys and large counters).
	UInt(u64),
	/// IEEE-754 double scalar.
	Float(f64),
	/// Arbitrary-precision decimal scalar.
	Decimal(Decimal),
	/// UTF-8 string scalar.
	String(String),
	/// Opaque byte payload (`BYTEA`, `BLOB`, `Bson::Binary`, etc.).
	Bytes(Vec<u8>),
	/// 128-bit UUID, transported natively where supported.
	Uuid(Uuid),
	/// UTC timestamp with millisecond resolution at the wire boundary.
	DateTime(DateTime<Utc>),
	/// Ordered sequence of values.
	Array(Vec<BenchValue>),
	/// Ordered field/value map (insertion order from the template schema).
	Object(Vec<(String, BenchValue)>),
	/// Fixed-length packed vector of f32 components (vector search payloads).
	FloatVector(Vec<f32>),
	/// 2D geographic point `(longitude, latitude)` in WGS84 degrees.
	///
	/// Maps to native `geometry<point>` in SurrealDB and to a GeoJSON Point
	/// object `{"type": "Point", "coordinates": [lon, lat]}` in JSON/BSON
	/// engines (MongoDB `2dsphere`, ArangoDB `geo`, etc.).
	Point(f64, f64),
}

impl BenchValue {
	/// Encode the value to a compact bincode payload for KV-store backends.
	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		bincode::serde::encode_to_vec(self, bincode::config::standard())
			.map_err(|e| anyhow!("bench value encode: {e}"))
	}

	/// Decode a bincode payload previously produced by [`Self::encode`].
	pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
		let (value, _) =
			bincode::serde::decode_from_slice::<BenchValue, _>(bytes, bincode::config::standard())
				.map_err(|e| anyhow!("bench value decode: {e}"))?;
		Ok(value)
	}

	/// Convert to a `serde_json::Value` at the JSON boundary. Lossy variants
	/// (`Decimal`, `Uuid`, `DateTime`, `Bytes`) emit canonical string forms so
	/// the result `sample` field stays human-readable and interoperable.
	pub(crate) fn to_json(&self) -> JsonValue {
		match self {
			BenchValue::Null => JsonValue::Null,
			BenchValue::Bool(b) => JsonValue::Bool(*b),
			BenchValue::Int(i) => JsonValue::Number((*i).into()),
			BenchValue::UInt(u) => JsonValue::Number((*u).into()),
			BenchValue::Float(f) => match Number::from_f64(*f) {
				Some(n) => JsonValue::Number(n),
				None => JsonValue::Null,
			},
			BenchValue::Decimal(d) => JsonValue::String(d.to_string()),
			BenchValue::String(s) => JsonValue::String(s.clone()),
			BenchValue::Bytes(b) => {
				JsonValue::Array(b.iter().map(|byte| JsonValue::Number((*byte).into())).collect())
			}
			BenchValue::Uuid(u) => JsonValue::String(u.to_string()),
			BenchValue::DateTime(dt) => JsonValue::String(dt.to_rfc3339()),
			BenchValue::Array(a) => JsonValue::Array(a.iter().map(BenchValue::to_json).collect()),
			BenchValue::Object(o) => {
				let mut m = Map::with_capacity(o.len());
				for (k, v) in o {
					m.insert(k.clone(), v.to_json());
				}
				JsonValue::Object(m)
			}
			BenchValue::FloatVector(v) => JsonValue::Array(
				v.iter()
					.map(|f| match Number::from_f64(*f as f64) {
						Some(n) => JsonValue::Number(n),
						None => JsonValue::Null,
					})
					.collect(),
			),
			BenchValue::Point(lon, lat) => {
				let mut map = serde_json::Map::with_capacity(2);
				map.insert("type".to_string(), JsonValue::String("Point".to_string()));
				map.insert(
					"coordinates".to_string(),
					JsonValue::Array(vec![
						Number::from_f64(*lon).map_or(JsonValue::Null, JsonValue::Number),
						Number::from_f64(*lat).map_or(JsonValue::Null, JsonValue::Number),
					]),
				);
				JsonValue::Object(map)
			}
		}
	}

	/// Borrow the components of a packed float vector, if this value is one.
	pub(crate) fn as_float_vector(&self) -> Option<&[f32]> {
		match self {
			BenchValue::FloatVector(v) => Some(v.as_slice()),
			_ => None,
		}
	}

	/// Return the `(lon, lat)` coordinates of a geographic point, if this value is one.
	#[cfg(test)]
	pub(crate) fn as_point(&self) -> Option<(f64, f64)> {
		match self {
			BenchValue::Point(lon, lat) => Some((*lon, *lat)),
			_ => None,
		}
	}

	/// Field accessor for object-shaped values.
	pub(crate) fn get_field(&self, key: &str) -> Option<&BenchValue> {
		match self {
			BenchValue::Object(o) => o.iter().find(|(k, _)| k == key).map(|(_, v)| v),
			_ => None,
		}
	}

	/// Reject any leading top-level value that is not an object payload.
	pub(crate) fn into_object(self) -> Result<Vec<(String, BenchValue)>> {
		match self {
			BenchValue::Object(o) => Ok(o),
			_ => bail!("expected object payload for row"),
		}
	}
}

impl From<&JsonValue> for BenchValue {
	fn from(v: &JsonValue) -> Self {
		match v {
			JsonValue::Null => BenchValue::Null,
			JsonValue::Bool(b) => BenchValue::Bool(*b),
			JsonValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					BenchValue::Int(i)
				} else if let Some(u) = n.as_u64() {
					BenchValue::UInt(u)
				} else if let Some(f) = n.as_f64() {
					BenchValue::Float(f)
				} else {
					BenchValue::Null
				}
			}
			JsonValue::String(s) => BenchValue::String(s.clone()),
			JsonValue::Array(a) => BenchValue::Array(a.iter().map(BenchValue::from).collect()),
			JsonValue::Object(o) => {
				if o.len() == 2
					&& o.get("type").and_then(JsonValue::as_str) == Some("Point")
					&& let Some(JsonValue::Array(coords)) = o.get("coordinates")
					&& coords.len() == 2
					&& let (Some(lon), Some(lat)) = (coords[0].as_f64(), coords[1].as_f64())
				{
					return BenchValue::Point(lon, lat);
				}
				BenchValue::Object(
					o.iter().map(|(k, v)| (k.clone(), BenchValue::from(v))).collect(),
				)
			}
		}
	}
}

impl From<JsonValue> for BenchValue {
	fn from(v: JsonValue) -> Self {
		BenchValue::from(&v)
	}
}

impl From<&BenchValue> for JsonValue {
	fn from(v: &BenchValue) -> Self {
		v.to_json()
	}
}

impl From<BenchValue> for JsonValue {
	fn from(v: BenchValue) -> Self {
		v.to_json()
	}
}

/// Parse a decimal from its canonical string form.
pub(crate) fn parse_decimal(s: &str) -> Result<Decimal> {
	Decimal::from_str(s).with_context(|| format!("invalid decimal {s:?}"))
}

/// Parse a UUID from its canonical string form.
pub(crate) fn parse_uuid(s: &str) -> Result<Uuid> {
	Uuid::parse_str(s).with_context(|| format!("invalid uuid {s:?}"))
}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::TimeZone;
	use serde_json::json;

	#[test]
	fn roundtrip_nested_array_like_tags() {
		let v = BenchValue::Object(vec![
			("name".into(), BenchValue::String("x".into())),
			(
				"tags".into(),
				BenchValue::Array(vec![
					BenchValue::String("alpha".into()),
					BenchValue::String("beta".into()),
					BenchValue::String("gamma".into()),
				]),
			),
			("n".into(), BenchValue::Int(42)),
		]);
		let bytes = v.encode().unwrap();
		let out = BenchValue::decode(&bytes).unwrap();
		assert_eq!(v, out);
	}

	#[test]
	fn roundtrip_uuid_variant() {
		let u = Uuid::new_v4();
		let v = BenchValue::Uuid(u);
		let out = BenchValue::decode(&v.encode().unwrap()).unwrap();
		assert_eq!(v, out);
	}

	#[test]
	fn roundtrip_datetime_variant() {
		let dt = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
		let v = BenchValue::DateTime(dt);
		let out = BenchValue::decode(&v.encode().unwrap()).unwrap();
		assert_eq!(v, out);
	}

	#[test]
	fn roundtrip_decimal_variant() {
		let d = Decimal::from_str("12345.67890").unwrap();
		let v = BenchValue::Decimal(d);
		let out = BenchValue::decode(&v.encode().unwrap()).unwrap();
		assert_eq!(v, out);
	}

	#[test]
	fn roundtrip_bytes_variant() {
		let v = BenchValue::Bytes(vec![0u8, 1, 2, 3, 0xff, 0xfe]);
		let out = BenchValue::decode(&v.encode().unwrap()).unwrap();
		assert_eq!(v, out);
	}

	#[test]
	fn json_roundtrip_simple_shapes() {
		let original = json!({
			"name": "x",
			"tags": ["alpha", "beta"],
			"n": 42,
		});
		let bv = BenchValue::from(&original);
		let back = bv.to_json();
		assert_eq!(original, back);
	}

	#[test]
	fn json_emits_canonical_strings_for_native_types() {
		let u = Uuid::nil();
		let bv = BenchValue::Uuid(u);
		assert_eq!(bv.to_json(), JsonValue::String(u.to_string()));
		let dt = Utc.timestamp_opt(0, 0).unwrap();
		let bv = BenchValue::DateTime(dt);
		assert_eq!(bv.to_json(), JsonValue::String(dt.to_rfc3339()));
		let d = Decimal::from_str("3.14").unwrap();
		let bv = BenchValue::Decimal(d);
		assert_eq!(bv.to_json(), JsonValue::String("3.14".into()));

		let pt = BenchValue::Point(-0.1276, 51.5072);
		let pt_json = pt.to_json();
		assert_eq!(pt_json, json!({"type": "Point", "coordinates": [-0.1276, 51.5072]}));
		assert_eq!(BenchValue::from(&pt_json), pt);
		assert_eq!(BenchValue::decode(&pt.encode().unwrap()).unwrap(), pt);
	}
}
