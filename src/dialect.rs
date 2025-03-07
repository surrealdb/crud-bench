use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::valueprovider::Columns;
use crate::Scan;
use anyhow::{bail, Result};
use chrono::{DateTime, TimeZone, Utc};
use flatten_json_object::ArrayFormatting;
use flatten_json_object::Flattener;
use mongodb::bson::{doc, to_document, Document};
use serde_json::Value;
use uuid::Uuid;

/// Help converting generated values to the right database representation
pub(crate) trait Dialect {
	fn uuid(u: Uuid) -> Value {
		Value::String(u.to_string())
	}
	fn date_time(secs_from_epoch: i64) -> Value {
		// Get the current UTC time
		let datetime: DateTime<Utc> = Utc.timestamp_opt(secs_from_epoch, 0).unwrap();
		// Format it to the SQL-friendly ISO 8601 format
		let formatted = datetime.to_rfc3339();
		Value::String(formatted)
	}
	fn escape_field(field: String) -> String {
		field
	}
	fn arg_string(val: Value) -> String {
		val.to_string()
	}
}

// --------------------------------------------------
// Default
// --------------------------------------------------

pub(crate) struct DefaultDialect();

impl Dialect for DefaultDialect {}

// --------------------------------------------------
// SQL
// --------------------------------------------------

pub(crate) struct AnsiSqlDialect();

impl Dialect for AnsiSqlDialect {
	fn escape_field(field: String) -> String {
		format!("\"{field}\"")
	}

	fn arg_string(val: Value) -> String {
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

impl AnsiSqlDialect {
	/// Constructs the field clauses for the [C]reate tests
	pub fn create_clause(cols: &Columns, val: Value) -> (String, String) {
		let mut fields = Vec::with_capacity(cols.0.len());
		let mut values = Vec::with_capacity(cols.0.len());
		if let Value::Object(map) = val {
			for (f, v) in map {
				fields.push(Self::escape_field(f));
				values.push(Self::arg_string(v));
			}
		}
		let fields = fields.join(", ");
		let values = values.join(", ");
		(fields, values)
	}
	/// Constructs the field clauses for the [U]pdate tests
	pub fn update_clause(cols: &Columns, val: Value) -> String {
		let mut updates = Vec::with_capacity(cols.0.len());
		if let Value::Object(map) = val {
			for (f, v) in map {
				let f = Self::escape_field(f);
				let v = Self::arg_string(v);
				updates.push(format!("{f} = {v}"));
			}
		}
		updates.join(", ")
	}
	/// Constructs the WHERE clause for [S]elect tests
	pub fn filter_clause(scan: &Scan) -> Result<String> {
		if let Some(ref c) = scan.condition {
			if let Some(ref c) = c.sql {
				return Ok(format!("WHERE {c}"));
			} else {
				bail!(NOT_SUPPORTED_ERROR);
			}
		}
		Ok(String::new())
	}
}

// --------------------------------------------------
// MySQL
// --------------------------------------------------

pub(crate) struct MySqlDialect();

impl Dialect for MySqlDialect {
	fn escape_field(field: String) -> String {
		format!("`{field}`")
	}

	fn arg_string(val: Value) -> String {
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

impl MySqlDialect {
	/// Constructs the field clauses for the [C]reate tests
	pub fn create_clause(cols: &Columns, val: Value) -> (String, String) {
		let mut fields = Vec::with_capacity(cols.0.len());
		let mut values = Vec::with_capacity(cols.0.len());
		if let Value::Object(map) = val {
			for (f, v) in map {
				fields.push(Self::escape_field(f));
				values.push(Self::arg_string(v));
			}
		}
		let fields = fields.join(", ");
		let values = values.join(", ");
		(fields, values)
	}
	/// Constructs the field clauses for the [U]pdate tests
	pub fn update_clause(cols: &Columns, val: Value) -> String {
		let mut updates = Vec::with_capacity(cols.0.len());
		if let Value::Object(map) = val {
			for (f, v) in map {
				let f = Self::escape_field(f);
				let v = Self::arg_string(v);
				updates.push(format!("{f} = {v}"));
			}
		}
		updates.join(", ")
	}
	/// Constructs the WHERE clause for [S]elect tests
	pub fn filter_clause(scan: &Scan) -> Result<String> {
		if let Some(ref c) = scan.condition {
			if let Some(ref c) = c.mysql {
				return Ok(format!("WHERE {c}"));
			} else {
				bail!(NOT_SUPPORTED_ERROR);
			}
		}
		Ok(String::new())
	}
}

// --------------------------------------------------
// Neo4j
// --------------------------------------------------

pub(crate) struct Neo4jDialect();

impl Dialect for Neo4jDialect {}

impl Neo4jDialect {
	/// Constructs the field clauses for the [C]reate tests
	pub fn create_clause(val: Value) -> Result<String> {
		let val = Flattener::new()
			.set_key_separator("_")
			.set_array_formatting(ArrayFormatting::Surrounded {
				start: "_".to_string(),
				end: "".to_string(),
			})
			.set_preserve_empty_arrays(false)
			.set_preserve_empty_objects(false)
			.flatten(&val)?;
		let obj = val.as_object().unwrap();
		let mut fields = Vec::with_capacity(obj.len());
		if let Value::Object(map) = val {
			for (f, v) in map {
				let f = Self::escape_field(f);
				let v = Self::arg_string(v);
				fields.push(format!("{f}: {v}"));
			}
		}
		Ok(fields.join(", "))
	}
	/// Constructs the field clauses for the [U]pdate tests
	pub fn update_clause(val: Value) -> Result<String> {
		let val = Flattener::new()
			.set_key_separator("_")
			.set_array_formatting(ArrayFormatting::Surrounded {
				start: "_".to_string(),
				end: "".to_string(),
			})
			.set_preserve_empty_arrays(false)
			.set_preserve_empty_objects(false)
			.flatten(&val)?;
		let obj = val.as_object().unwrap();
		let mut fields = Vec::with_capacity(obj.len());
		if let Value::Object(map) = val {
			for (f, v) in map {
				let f = Self::escape_field(f);
				let v = Self::arg_string(v);
				fields.push(format!("r.{f} = {v}"));
			}
		}
		Ok(fields.join(", "))
	}
	/// Constructs the WHERE clause for [S]elect tests
	pub fn filter_clause(scan: &Scan) -> Result<String> {
		if let Some(ref c) = scan.condition {
			if let Some(ref c) = c.neo4j {
				return Ok(format!("WHERE {c}"));
			} else {
				bail!(NOT_SUPPORTED_ERROR);
			}
		}
		Ok(String::new())
	}
}

// --------------------------------------------------
// SurrealDB
// --------------------------------------------------

pub(crate) struct SurrealDBDialect();

impl Dialect for SurrealDBDialect {}

impl SurrealDBDialect {
	/// Constructs the WHERE clause for [S]elect tests
	pub fn filter_clause(scan: &Scan) -> Result<String> {
		if let Some(ref c) = scan.condition {
			if let Some(ref c) = c.surrealdb {
				return Ok(format!("WHERE {c}"));
			} else {
				bail!(NOT_SUPPORTED_ERROR);
			}
		}
		Ok(String::new())
	}
}

// --------------------------------------------------
// ArangoDB
// --------------------------------------------------

pub(crate) struct ArangoDBDialect();

impl Dialect for ArangoDBDialect {}

impl ArangoDBDialect {
	/// Constructs the WHERE clause for [S]elect tests
	pub fn filter_clause(scan: &Scan) -> Result<String> {
		if let Some(ref c) = scan.condition {
			if let Some(ref c) = c.arangodb {
				return Ok(format!("FILTER {c}"));
			} else {
				bail!(NOT_SUPPORTED_ERROR);
			}
		}
		Ok(String::new())
	}
}

// --------------------------------------------------
// MongoDB
// --------------------------------------------------

pub(crate) struct MongoDBDialect();

impl Dialect for MongoDBDialect {}

impl MongoDBDialect {
	/// Constructs the filter document for [S]elect tests
	pub fn filter_clause(scan: &Scan) -> Result<Document> {
		if let Some(ref c) = scan.condition {
			if let Some(ref c) = c.mongodb {
				return Ok(to_document(c)?);
			} else {
				bail!(NOT_SUPPORTED_ERROR);
			}
		}
		Ok(doc! {})
	}
}
