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

/// Native PostgreSQL `POINT` wire-format wrapper (`(lon, lat)` as two big-endian `f64`s).
#[cfg(feature = "postgres")]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PgPoint(pub f64, pub f64);

#[cfg(feature = "postgres")]
impl tokio_postgres::types::ToSql for PgPoint {
	fn to_sql(
		&self,
		_ty: &tokio_postgres::types::Type,
		out: &mut tokio_postgres::types::private::BytesMut,
	) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
	{
		out.extend_from_slice(&self.0.to_be_bytes());
		out.extend_from_slice(&self.1.to_be_bytes());
		Ok(tokio_postgres::types::IsNull::No)
	}

	fn accepts(ty: &tokio_postgres::types::Type) -> bool {
		*ty == tokio_postgres::types::Type::POINT
	}

	tokio_postgres::types::to_sql_checked!();
}

#[cfg(feature = "postgres")]
impl<'a> tokio_postgres::types::FromSql<'a> for PgPoint {
	fn from_sql(
		_ty: &tokio_postgres::types::Type,
		raw: &'a [u8],
	) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		if raw.len() != 16 {
			return Err(format!("invalid PostgreSQL POINT byte length: {}", raw.len()).into());
		}
		let lon = f64::from_be_bytes(raw[0..8].try_into()?);
		let lat = f64::from_be_bytes(raw[8..16].try_into()?);
		Ok(PgPoint(lon, lat))
	}

	fn accepts(ty: &tokio_postgres::types::Type) -> bool {
		*ty == tokio_postgres::types::Type::POINT
	}
}

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
		(ColumnType::Point, BenchValue::Point(lon, lat)) => Ok(Box::new(PgPoint(*lon, *lat))),
		(ColumnType::Point, BenchValue::Object(_)) => {
			let (lon, lat) = v
				.as_point()
				.ok_or_else(|| anyhow!("column {column_name}: invalid GeoJSON Point object"))?;
			Ok(Box::new(PgPoint(lon, lat)))
		}
		(ColumnType::FloatVector(dim), BenchValue::FloatVector(vec)) => {
			if vec.len() != *dim {
				bail!("column {column_name}: vector dim mismatch ({} != {dim})", vec.len());
			}
			Ok(Box::new(pgvector::Vector::from(vec.clone())))
		}
		(ColumnType::FloatVector(dim), BenchValue::Bytes(b)) if b.len() == dim * 4 => {
			// Read-back round-trip: SQL fallbacks store the vector as
			// `cast_slice::<f32, u8>` bytes; the read path returns it as
			// `BenchValue::Bytes`. Reconstruct the `pgvector::Vector` so a
			// mixed read/write workload doesn't fail at the bind step.
			Ok(Box::new(pgvector::Vector::from(bytemuck::cast_slice::<u8, f32>(b).to_vec())))
		}
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
		(ColumnType::Object, _) | (ColumnType::Array, _) | (ColumnType::Point, _) => {
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
		(ColumnType::FloatVector(_), BenchValue::FloatVector(v)) => {
			Ok(Box::new(bytemuck::cast_slice::<f32, u8>(v).to_vec()))
		}
		// Read-back round-trip on SQL fallbacks: the vector lives as a BLOB
		// internally; mixed read/write workloads re-bind the read-back bytes
		// directly without an extra conversion step.
		(ColumnType::FloatVector(_), BenchValue::Bytes(b)) => Ok(Box::new(b.clone())),
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
		(ColumnType::Object, _) | (ColumnType::Array, _) | (ColumnType::Point, _) => {
			Ok(MyValue::Bytes(serde_json::to_string(&v.to_json())?.into_bytes()))
		}
		(ColumnType::FloatVector(_), BenchValue::FloatVector(v)) => {
			Ok(MyValue::Bytes(bytemuck::cast_slice::<f32, u8>(v).to_vec()))
		}
		// Read-back round-trip for mixed read/write workloads (see comment in
		// `bench_to_sqlite_param`).
		(ColumnType::FloatVector(_), BenchValue::Bytes(b)) => Ok(MyValue::Bytes(b.clone())),
		(t, _) => bail!("BenchValue does not match column type {t:?}"),
	}
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
	use super::PgPoint;
	use tokio_postgres::types::{FromSql, ToSql, Type, private::BytesMut};

	#[test]
	fn pg_point_wire_roundtrip() {
		let pt = PgPoint(2.3522, 48.8566);
		assert!(<PgPoint as ToSql>::accepts(&Type::POINT));
		assert!(<PgPoint as FromSql>::accepts(&Type::POINT));
		let mut buf = BytesMut::new();
		pt.to_sql(&Type::POINT, &mut buf).unwrap();
		assert_eq!(buf.len(), 16);
		let decoded = PgPoint::from_sql(&Type::POINT, &buf).unwrap();
		assert_eq!(decoded, pt);
		assert!(PgPoint::from_sql(&Type::POINT, &[0u8; 8]).is_err());
	}
}
