//! Per-backend [`BenchValue`] → driver parameter conversions for SQL stores.
//!
//! Each helper matches on the [`BenchValue`] variant and produces the native
//! parameter type for that driver. Per-row UUID / datetime parsing on the
//! client side is eliminated — the variant *is* the type. JSON-typed columns
//! (`JSONB`, `JSON`, `TEXT JSON`) round-trip through [`BenchValue::to_json`]
//! at the boundary, which is the only place serde-JSON re-enters the picture.

#[allow(unused_imports)]
use crate::value::BenchValue;
#[allow(unused_imports)]
use crate::valueprovider::ColumnType;
#[allow(unused_imports)]
use anyhow::{Result, anyhow, bail};

/// Bind a [`BenchValue`] to a [`tokio_postgres::types::ToSql`] heap parameter,
/// validated against the destination [`ColumnType`].
#[cfg(feature = "postgres")]
pub(crate) fn bench_to_postgres_param(
	column_name: &str,
	column_type: &ColumnType,
	v: &BenchValue,
) -> Result<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> {
	use tokio_postgres::types::Json;
	match (column_type, v) {
		(ColumnType::Integer, BenchValue::Int(i)) => Ok(Box::new(*i as i32)),
		(ColumnType::Integer, BenchValue::UInt(u)) => Ok(Box::new(*u as i32)),
		(ColumnType::Float, BenchValue::Float(f)) => Ok(Box::new(*f as f32)),
		(ColumnType::Float, BenchValue::Int(i)) => Ok(Box::new(*i as f32)),
		(ColumnType::Bool, BenchValue::Bool(b)) => Ok(Box::new(*b)),
		(ColumnType::String, BenchValue::String(s)) => Ok(Box::new(s.clone())),
		(ColumnType::DateTime, BenchValue::DateTime(dt)) => Ok(Box::new(dt.naive_utc())),
		(ColumnType::Uuid, BenchValue::Uuid(u)) => Ok(Box::new(*u)),
		(ColumnType::Decimal, BenchValue::Decimal(d)) => Ok(Box::new(*d)),
		(ColumnType::Bytes, BenchValue::Bytes(b)) => Ok(Box::new(b.clone())),
		(ColumnType::Object, BenchValue::Object(_))
		| (ColumnType::Object, BenchValue::Array(_))
		| (ColumnType::Array, BenchValue::Object(_))
		| (ColumnType::Array, BenchValue::Array(_)) => Ok(Box::new(Json(v.to_json()))),
		(t, _) => Err(anyhow!("column {column_name}: BenchValue does not match column type {t:?}")),
	}
}

/// Bind a [`BenchValue`] to a [`tokio_rusqlite::types::ToSql`] heap parameter,
/// validated against the destination [`ColumnType`]. Decimals/UUIDs/datetimes
/// land as TEXT (SQLite has no native NUMERIC/UUID/TIMESTAMP).
#[cfg(feature = "sqlite")]
pub(crate) fn bench_to_sqlite_param(
	column_type: &ColumnType,
	v: &BenchValue,
) -> Result<Box<dyn tokio_rusqlite::types::ToSql + Send + Sync>> {
	match (column_type, v) {
		(ColumnType::Integer, BenchValue::Int(i)) => Ok(Box::new(*i)),
		(ColumnType::Integer, BenchValue::UInt(u)) => Ok(Box::new(*u as i64)),
		(ColumnType::Float, BenchValue::Float(f)) => Ok(Box::new(*f)),
		(ColumnType::Float, BenchValue::Int(i)) => Ok(Box::new(*i as f64)),
		(ColumnType::Bool, BenchValue::Bool(b)) => Ok(Box::new(*b)),
		// SQLite stores BOOL as INTEGER 0/1; rows read back as [`BenchValue::Int`].
		(ColumnType::Bool, BenchValue::Int(i)) => Ok(Box::new(*i != 0)),
		(ColumnType::Bool, BenchValue::UInt(u)) => Ok(Box::new(*u != 0)),
		(ColumnType::String, BenchValue::String(s)) => Ok(Box::new(s.clone())),
		(ColumnType::Object, _) | (ColumnType::Array, _) => {
			Ok(Box::new(serde_json::to_string(&v.to_json())?))
		}
		(ColumnType::DateTime, BenchValue::DateTime(dt)) => Ok(Box::new(dt.to_rfc3339())),
		// `TIMESTAMP` / UUID / `DECIMAL` columns are TEXT; reads can be [`BenchValue::String`].
		(ColumnType::DateTime, BenchValue::String(s)) => {
			let dt: chrono::DateTime<chrono::Utc> =
				s.parse().map_err(|e| anyhow!("datetime parameter {s:?}: {e}"))?;
			Ok(Box::new(dt.to_rfc3339()))
		}
		(ColumnType::Uuid, BenchValue::Uuid(u)) => Ok(Box::new(u.to_string())),
		(ColumnType::Uuid, BenchValue::String(s)) => {
			uuid::Uuid::parse_str(s).map_err(|e| anyhow!("uuid parameter {s:?}: {e}"))?;
			Ok(Box::new(s.clone()))
		}
		(ColumnType::Decimal, BenchValue::Decimal(d)) => Ok(Box::new(d.to_string())),
		(ColumnType::Decimal, BenchValue::String(s)) => {
			let d: rust_decimal::Decimal =
				s.parse().map_err(|e| anyhow!("decimal parameter {s:?}: {e}"))?;
			Ok(Box::new(d.to_string()))
		}
		(ColumnType::Bytes, BenchValue::Bytes(b)) => Ok(Box::new(b.clone())),
		(t, _) => Err(anyhow!("BenchValue does not match column type {t:?}")),
	}
}

/// Bind a [`BenchValue`] to a `mysql_async::Value`, validated against the
/// destination [`ColumnType`]. Decimals are passed as strings (MySQL/MariaDB
/// accept implicit cast to `DECIMAL`); UUIDs as their canonical 36-char form.
#[cfg(any(feature = "mysql", feature = "mariadb"))]
pub(crate) fn bench_to_mysql_value(
	column_type: &ColumnType,
	v: &BenchValue,
) -> Result<mysql_async::Value> {
	use mysql_async::Value as MyValue;
	match (column_type, v) {
		(ColumnType::Integer, BenchValue::Int(i)) => Ok(MyValue::Int(*i)),
		(ColumnType::Integer, BenchValue::UInt(u)) => Ok(MyValue::UInt(*u)),
		(ColumnType::Float, BenchValue::Float(f)) => Ok(MyValue::Double(*f)),
		(ColumnType::Float, BenchValue::Int(i)) => Ok(MyValue::Double(*i as f64)),
		(ColumnType::Bool, BenchValue::Bool(b)) => Ok(MyValue::Int(i64::from(*b))),
		(ColumnType::String, BenchValue::String(s)) => Ok(MyValue::Bytes(s.as_bytes().to_vec())),
		(ColumnType::DateTime, BenchValue::DateTime(dt)) => Ok(MyValue::from(dt.naive_utc())),
		(ColumnType::Uuid, BenchValue::Uuid(u)) => Ok(MyValue::Bytes(u.to_string().into_bytes())),
		(ColumnType::Decimal, BenchValue::Decimal(d)) => {
			Ok(MyValue::Bytes(d.to_string().into_bytes()))
		}
		(ColumnType::Bytes, BenchValue::Bytes(b)) => Ok(MyValue::Bytes(b.clone())),
		(ColumnType::Object, _) | (ColumnType::Array, _) => {
			Ok(MyValue::Bytes(serde_json::to_string(&v.to_json())?.into_bytes()))
		}
		(t, _) => bail!("BenchValue does not match column type {t:?}"),
	}
}
