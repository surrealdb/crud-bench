use crate::Scan;
use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::valueprovider::{ColumnType, Columns};
use anyhow::{Result, bail};
use chrono::{DateTime, TimeZone, Utc};
#[cfg(feature = "mongodb")]
use mongodb::bson::{Document, doc, to_document};
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
}

impl AnsiSqlDialect {
	/// Constructs the column list for an `INSERT` statement.
	pub(crate) fn insert_columns(columns: &Columns) -> String {
		columns
			.0
			.iter()
			.map(|(name, _)| Self::escape_field(name.clone()))
			.collect::<Vec<String>>()
			.join(", ")
	}

	/// Constructs the `(column list, placeholder list)` for a `INSERT` statement.
	pub(crate) fn create_clause(columns: &Columns) -> (String, String) {
		let column_list = Self::insert_columns(columns);
		let placeholders =
			(2..=1 + columns.0.len()).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ");
		(column_list, placeholders)
	}

	/// Constructs the `SET` clause for a `UPDATE` statement.
	pub(crate) fn update_clause(columns: &Columns) -> String {
		columns
			.0
			.iter()
			.enumerate()
			.map(|(i, (name, _))| format!("{} = ${}", Self::escape_field(name.clone()), i + 2))
			.collect::<Vec<_>>()
			.join(", ")
	}

	/// Constructs the WHERE clause for a [S]can test
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

	/// Constructs the ORDER BY clause for [S]can tests
	pub fn order_by_clause(scan: &Scan) -> Result<String> {
		match &scan.order_by {
			None => Ok(String::new()),
			Some(o) => match &o.sql {
				Some(s) if !s.is_empty() => Ok(format!("ORDER BY {s}")),
				_ => bail!(NOT_SUPPORTED_ERROR),
			},
		}
	}
}

// --------------------------------------------------
// PostgreSQL
// --------------------------------------------------

pub(crate) struct PostgresDialect();

impl PostgresDialect {
	/// Prefer `condition.postgres`, then generic `condition.sql`.
	pub fn filter_clause(scan: &Scan) -> Result<String> {
		if let Some(ref c) = scan.condition {
			if let Some(ref frag) = c.postgres {
				return Ok(format!("WHERE {frag}"));
			}
			if let Some(ref frag) = c.sql {
				return Ok(format!("WHERE {frag}"));
			}
			bail!(NOT_SUPPORTED_ERROR);
		}
		Ok(String::new())
	}

	/// Bench `tags.*` denotes a JSON array index; PostgreSQL has no Surreal-style path — index the JSONB column (same idea as SQLite).
	pub(crate) fn btree_index_key_list(columns: &Columns, spec: &crate::Index) -> String {
		spec.fields
			.iter()
			.map(|field| {
				if let Some(base) = field.strip_suffix(".*")
					&& let Some((_, col_type)) = columns.0.iter().find(|(n, _)| n == base)
					&& matches!(col_type, ColumnType::Array | ColumnType::Object)
				{
					return AnsiSqlDialect::escape_field(base.to_string());
				}
				AnsiSqlDialect::escape_field(field.clone())
			})
			.collect::<Vec<_>>()
			.join(", ")
	}
}

// --------------------------------------------------
// SQLite
// --------------------------------------------------

pub(crate) struct SqliteDialect();

impl SqliteDialect {
	/// Prefer `condition.sqlite`, then fall back to generic `condition.sql`.
	pub fn filter_clause(scan: &Scan) -> Result<String> {
		if let Some(ref c) = scan.condition {
			if let Some(ref frag) = c.sqlite {
				return Ok(format!("WHERE {frag}"));
			}
			if let Some(ref frag) = c.sql {
				return Ok(format!("WHERE {frag}"));
			}
			bail!(NOT_SUPPORTED_ERROR);
		}
		Ok(String::new())
	}

	/// Bench `tags.*` denotes a JSON array index; SQLite has no multi-valued btree — index the stored JSON column instead.
	pub(crate) fn btree_index_key_list(columns: &Columns, spec: &crate::Index) -> String {
		spec.fields
			.iter()
			.map(|field| {
				if let Some(base) = field.strip_suffix(".*")
					&& let Some((_, col_type)) = columns.0.iter().find(|(n, _)| n == base)
					&& matches!(col_type, ColumnType::Array | ColumnType::Object)
				{
					return AnsiSqlDialect::escape_field(base.to_string());
				}
				AnsiSqlDialect::escape_field(field.clone())
			})
			.collect::<Vec<_>>()
			.join(", ")
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
}

impl MySqlDialect {
	/// Escaped identifiers for each non-id column
	pub(crate) fn escaped_columns(columns: &Columns) -> Vec<String> {
		columns.0.iter().map(|(name, _)| Self::escape_field(name.clone())).collect()
	}

	/// Constructs the `(column list, placeholder list)` for a `INSERT` statement.
	pub(crate) fn create_clause(columns: &Columns) -> (String, String) {
		let column_list = columns
			.0
			.iter()
			.map(|(name, _)| Self::escape_field(name.clone()))
			.collect::<Vec<String>>()
			.join(", ");
		let placeholders = (0..columns.0.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
		(column_list, placeholders)
	}

	/// Constructs the `SET` clause for a `UPDATE` statement.
	pub(crate) fn update_clause(columns: &Columns) -> String {
		columns
			.0
			.iter()
			.map(|(name, _)| format!("{} = ?", Self::escape_field(name.clone())))
			.collect::<Vec<_>>()
			.join(", ")
	}

	/// Constructs the WHERE clause for [S]can tests
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

	/// Constructs the ORDER BY clause for [S]can tests
	pub fn order_by_clause(scan: &Scan) -> Result<String> {
		match &scan.order_by {
			None => Ok(String::new()),
			Some(o) => match &o.mysql {
				Some(s) if !s.is_empty() => Ok(format!("ORDER BY {s}")),
				_ => bail!(NOT_SUPPORTED_ERROR),
			},
		}
	}

	/// InnoDB btree indexes require a prefix length on [`ColumnType::String`] (`TEXT`) columns.
	///
	/// Bench specs may use Surreal-style `tags.*` on JSON array columns; map those to a MySQL 8
	/// [multi-valued index](https://dev.mysql.com/doc/refman/8.0/en/create-index.html#create-index-multi-valued)
	/// expression instead of a bogus column name.
	pub(crate) fn btree_index_key_list(columns: &Columns, spec: &crate::Index) -> String {
		spec.fields
			.iter()
			.map(|field| {
				if let Some(base) = field.strip_suffix(".*")
					&& let Some((_, col_type)) = columns.0.iter().find(|(n, _)| n == base)
					&& matches!(col_type, ColumnType::Array | ColumnType::Object)
				{
					let esc = Self::escape_field(base.to_string());
					return format!("((CAST({esc} AS CHAR(191) ARRAY)))");
				}

				let escaped = Self::escape_field(field.clone());
				let needs_prefix = columns
					.0
					.iter()
					.find(|(n, _)| n == field)
					.is_some_and(|(_, t)| matches!(t, ColumnType::String));
				if needs_prefix {
					format!("{escaped}(191)")
				} else {
					escaped
				}
			})
			.collect::<Vec<_>>()
			.join(", ")
	}
}

// --------------------------------------------------
// MariaDB
// --------------------------------------------------

pub(crate) struct MariaDBDialect();

impl Dialect for MariaDBDialect {
	fn escape_field(field: String) -> String {
		format!("`{field}`")
	}
}

impl MariaDBDialect {
	/// Escaped identifiers for each non-id column
	pub(crate) fn escaped_columns(columns: &Columns) -> Vec<String> {
		columns.0.iter().map(|(name, _)| Self::escape_field(name.clone())).collect()
	}

	/// Constructs the `(column list, placeholder list)` for a `INSERT` statement
	pub(crate) fn create_clause(columns: &Columns) -> (String, String) {
		let column_list = columns
			.0
			.iter()
			.map(|(name, _)| Self::escape_field(name.clone()))
			.collect::<Vec<String>>()
			.join(", ");
		let placeholders = (0..columns.0.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
		(column_list, placeholders)
	}

	/// Constructs the `SET` clause for a `UPDATE` statement.
	pub(crate) fn update_clause(columns: &Columns) -> String {
		columns
			.0
			.iter()
			.map(|(name, _)| format!("{} = ?", Self::escape_field(name.clone())))
			.collect::<Vec<_>>()
			.join(", ")
	}

	/// Constructs the WHERE clause for [S]can tests
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

	/// Constructs the ORDER BY clause for [S]can tests
	pub fn order_by_clause(scan: &Scan) -> Result<String> {
		match &scan.order_by {
			None => Ok(String::new()),
			Some(o) => match &o.mysql {
				Some(s) if !s.is_empty() => Ok(format!("ORDER BY {s}")),
				_ => bail!(NOT_SUPPORTED_ERROR),
			},
		}
	}

	/// B-tree index key list; unlike MySQL 8 `CAST(… AS … ARRAY)` multi-valued indexes, MariaDB uses a functional `CAST` to `CHAR` for `tags.*` JSON wildcard specs.
	pub(crate) fn btree_index_key_list(columns: &Columns, spec: &crate::Index) -> String {
		spec.fields
			.iter()
			.map(|field| {
				if let Some(base) = field.strip_suffix(".*")
					&& let Some((_, col_type)) = columns.0.iter().find(|(n, _)| n == base)
					&& matches!(col_type, ColumnType::Array | ColumnType::Object)
				{
					let esc = Self::escape_field(base.to_string());
					return format!("((CAST({esc} AS CHAR(191))))");
				}

				let escaped = Self::escape_field(field.clone());
				let needs_prefix = columns
					.0
					.iter()
					.find(|(n, _)| n == field)
					.is_some_and(|(_, t)| matches!(t, ColumnType::String));
				if needs_prefix {
					format!("{escaped}(191)")
				} else {
					escaped
				}
			})
			.collect::<Vec<_>>()
			.join(", ")
	}
}

// --------------------------------------------------
// Neo4j
// --------------------------------------------------

pub(crate) struct Neo4jDialect();

impl Dialect for Neo4jDialect {}

impl Neo4jDialect {
	/// Constructs the WHERE clause for [S]can tests
	pub fn filter_clause(scan: &Scan) -> Result<String> {
		if let Some(ref c) = scan.condition {
			if let Some(ref c) = c.neo4j {
				if let Some(index) = &scan.with_index
					&& let Some(kind) = &index.index_type
					&& kind == "fulltext"
				{
					return Ok(c.to_string());
				} else {
					return Ok(format!("WHERE {c}"));
				}
			} else {
				bail!(NOT_SUPPORTED_ERROR);
			}
		}
		Ok(String::new())
	}

	/// Constructs the ORDER BY clause for [S]can tests
	pub fn order_by_clause(scan: &Scan) -> Result<String> {
		match &scan.order_by {
			None => Ok(String::new()),
			Some(o) => match &o.neo4j {
				Some(s) if !s.is_empty() => Ok(format!("ORDER BY {s}")),
				_ => bail!(NOT_SUPPORTED_ERROR),
			},
		}
	}
}

// --------------------------------------------------
// SurrealDB
// --------------------------------------------------

pub(crate) struct SurrealDBDialect();

impl Dialect for SurrealDBDialect {}

impl SurrealDBDialect {
	/// Constructs the WHERE clause for [S]can tests
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

	/// Constructs the ORDER BY clause for [S]can tests
	pub fn order_by_clause(scan: &Scan) -> Result<String> {
		match &scan.order_by {
			None => Ok(String::new()),
			Some(o) => match &o.surrealdb {
				Some(s) if !s.is_empty() => Ok(format!("ORDER BY {s}")),
				_ => bail!(NOT_SUPPORTED_ERROR),
			},
		}
	}
}

// --------------------------------------------------
// ArangoDB
// --------------------------------------------------

pub(crate) struct ArangoDBDialect();

impl Dialect for ArangoDBDialect {}

impl ArangoDBDialect {
	/// Constructs the WHERE clause for [S]can tests
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

	/// Constructs the ORDER BY clause for [S]can tests
	pub fn sort_clause(scan: &Scan) -> Result<String> {
		match &scan.order_by {
			None => Ok(String::new()),
			Some(o) => match &o.arangodb {
				Some(s) if !s.is_empty() => Ok(format!("SORT {s}")),
				_ => bail!(NOT_SUPPORTED_ERROR),
			},
		}
	}
}

// --------------------------------------------------
// MongoDB
// --------------------------------------------------

pub(crate) struct MongoDBDialect();

#[cfg(feature = "mongodb")]
impl Dialect for MongoDBDialect {}

#[cfg(feature = "mongodb")]
impl MongoDBDialect {
	/// Constructs the filter document for [S]scan tests
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

	/// Constructs the ORDER BY clause for [S]can tests
	pub fn sort_document(scan: &Scan) -> Result<Option<Document>> {
		match &scan.order_by {
			None => Ok(None),
			Some(o) => match &o.mongodb {
				Some(v) => Ok(Some(to_document(v)?)),
				_ => bail!(NOT_SUPPORTED_ERROR),
			},
		}
	}
}
