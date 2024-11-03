use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use uuid::Uuid;

/// Help converting generated values to the right database representation
pub(crate) trait Dialect {
	fn uuid(u: Uuid) -> Value;
	fn date_time(secs_from_epoch: i64) -> Value;
	fn arg_string(val: &Value) -> String;
}

pub(crate) struct AnsiSqlDialect();

impl Dialect for AnsiSqlDialect {
	fn uuid(u: Uuid) -> Value {
		Value::String(u.to_string())
	}

	// The Default format is a String using ISO 8601
	fn date_time(secs_from_epoch: i64) -> Value {
		// Get the current UTC time
		let datetime: DateTime<Utc> = Utc.timestamp_opt(secs_from_epoch, 0).unwrap();
		// Format it to the SQL-friendly ISO 8601 format
		let formatted = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
		Value::String(formatted)
	}

	fn arg_string(val: &Value) -> String {
		match val {
			Value::Null => "null".to_string(),
			Value::Bool(b) => b.to_string(),
			Value::Number(n) => n.to_string(),
			Value::String(s) => format!("'{s}'"),
			Value::Array(a) => serde_json::to_string(&a).unwrap(),
			Value::Object(o) => format!("'{}'", serde_json::to_string(&o).unwrap()),
		}
	}
}

pub(crate) struct DefaultDialect();

impl Dialect for DefaultDialect {
	fn uuid(u: Uuid) -> Value {
		Value::String(u.to_string())
	}

	// The Default format is a String using ISO 8601
	fn date_time(secs_from_epoch: i64) -> Value {
		// Get the current UTC time
		let datetime: DateTime<Utc> = Utc.timestamp_opt(secs_from_epoch, 0).unwrap();
		// Format it to the SQL-friendly ISO 8601 format
		let formatted = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
		Value::String(formatted)
	}

	fn arg_string(val: &Value) -> String {
		val.to_string()
	}
}
