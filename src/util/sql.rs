//! Shared JSON → bound-parameter conversions for SQL backends.

use crate::valueprovider::ColumnType;
use anyhow::{Result, anyhow};
use chrono::NaiveDateTime;
use serde_json::Value;

/// `YYYY-MM-DD HH:MM:SS` (UTC wall clock) or RFC3339, for benchmark JSON datetime strings.
pub(crate) fn parse_json_datetime_str(s: &str) -> Result<NaiveDateTime> {
	use chrono::DateTime;
	NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
		.or_else(|_| DateTime::parse_from_rfc3339(s).map(|d| d.naive_utc()))
		.map_err(|e| anyhow!("invalid datetime {s:?}: {e}"))
}

/// UTC milliseconds since epoch (Scylla `TIMESTAMP`).
pub(crate) fn json_datetime_to_millis(s: &str) -> Result<i64> {
	use chrono::{TimeZone, Utc};
	let naive = parse_json_datetime_str(s)?;
	Ok(Utc.from_utc_datetime(&naive).timestamp_millis())
}

#[cfg(feature = "postgres")]
pub(crate) fn json_to_postgres_param(
	column_name: &str,
	column_type: &ColumnType,
	json_value: &Value,
) -> Result<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> {
	use tokio_postgres::types::Json;
	match column_type {
		ColumnType::Integer => {
			if let Some(int_val) = json_value.as_i64() {
				Ok(Box::new(int_val as i32))
			} else {
				Err(anyhow!("Expected integer for column {column_name}"))
			}
		}
		ColumnType::Float => {
			if let Some(float_val) = json_value.as_f64() {
				Ok(Box::new(float_val as f32))
			} else {
				Err(anyhow!("Expected float for column {column_name}"))
			}
		}
		ColumnType::Bool => {
			if let Some(bool_val) = json_value.as_bool() {
				Ok(Box::new(bool_val))
			} else {
				Err(anyhow!("Expected boolean for column {column_name}"))
			}
		}
		ColumnType::String => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow!("Expected string for column {column_name}"))
			}
		}
		ColumnType::DateTime => {
			if let Some(str_val) = json_value.as_str() {
				let naive = parse_json_datetime_str(str_val)?;
				Ok(Box::new(naive))
			} else {
				Err(anyhow!("Expected datetime string for column {column_name}"))
			}
		}
		ColumnType::Uuid => {
			if let Some(str_val) = json_value.as_str() {
				if let Ok(uuid) = uuid::Uuid::parse_str(str_val) {
					Ok(Box::new(uuid))
				} else {
					Err(anyhow!("Invalid UUID for column {column_name}"))
				}
			} else {
				Err(anyhow!("Expected UUID string for column {column_name}"))
			}
		}
		ColumnType::Object => Ok(Box::new(Json(json_value.clone()))),
		ColumnType::Array => Ok(Box::new(Json(json_value.clone()))),
	}
}

#[cfg(any(feature = "mysql", feature = "mariadb"))]
pub(crate) fn json_to_mysql_value(
	column_type: &ColumnType,
	json_value: &Value,
) -> Result<mysql_async::Value> {
	match column_type {
		ColumnType::Integer => {
			let i = json_value.as_i64().ok_or_else(|| anyhow!("Expected integer"))?;
			Ok(mysql_async::Value::Int(i))
		}
		ColumnType::Float => {
			let f = json_value.as_f64().ok_or_else(|| anyhow!("Expected float"))?;
			Ok(mysql_async::Value::Double(f))
		}
		ColumnType::Bool => {
			let b = json_value.as_bool().ok_or_else(|| anyhow!("Expected boolean"))?;
			Ok(mysql_async::Value::Int(i64::from(b)))
		}
		ColumnType::String => {
			let s = json_value.as_str().ok_or_else(|| anyhow!("Expected string"))?;
			Ok(mysql_async::Value::Bytes(s.as_bytes().to_vec()))
		}
		ColumnType::DateTime => {
			let s = json_value.as_str().ok_or_else(|| anyhow!("Expected datetime string"))?;
			Ok(mysql_async::Value::from(parse_json_datetime_str(s)?))
		}
		ColumnType::Uuid => {
			let s = json_value.as_str().ok_or_else(|| anyhow!("Expected UUID string"))?;
			Ok(mysql_async::Value::Bytes(s.as_bytes().to_vec()))
		}
		ColumnType::Object | ColumnType::Array => {
			Ok(mysql_async::Value::Bytes(serde_json::to_string(json_value)?.into_bytes()))
		}
	}
}

#[cfg(feature = "sqlite")]
pub(crate) fn json_to_sqlite_param(
	column_type: &ColumnType,
	json_value: &Value,
) -> Result<Box<dyn tokio_rusqlite::types::ToSql + Send + Sync>> {
	match column_type {
		ColumnType::Integer => {
			if let Some(int_val) = json_value.as_i64() {
				Ok(Box::new(int_val))
			} else {
				Err(anyhow!("Expected integer"))
			}
		}
		ColumnType::Float => {
			if let Some(float_val) = json_value.as_f64() {
				Ok(Box::new(float_val))
			} else {
				Err(anyhow!("Expected float"))
			}
		}
		ColumnType::Bool => {
			let b = if let Some(b) = json_value.as_bool() {
				b
			} else if let Some(i) = json_value.as_i64() {
				match i {
					0 => false,
					1 => true,
					_ => return Err(anyhow!("Expected boolean or integer 0/1 for bool column")),
				}
			} else {
				return Err(anyhow!("Expected boolean"));
			};
			Ok(Box::new(b))
		}
		ColumnType::String => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow!("Expected string"))
			}
		}
		ColumnType::Object | ColumnType::Array => {
			// Avoid double-encoding: TEXT JSON reads become `Value::String`; `Display` adds quotes.
			let text = match json_value {
				Value::Object(_) | Value::Array(_) => serde_json::to_string(json_value)?,
				Value::String(s) => s.clone(),
				_ => return Err(anyhow!("Expected JSON object, array, or JSON text")),
			};
			Ok(Box::new(text))
		}
		ColumnType::DateTime => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow!("Expected datetime string"))
			}
		}
		ColumnType::Uuid => {
			if let Some(str_val) = json_value.as_str() {
				Ok(Box::new(str_val.to_string()))
			} else {
				Err(anyhow!("Expected UUID string"))
			}
		}
	}
}
