//! Predicates that restrict which rows a KNN query may return.
//!
//! Filtered KNN is the shape most production vector search actually has —
//! "nearest neighbours *among this tenant's documents*" — and it is where
//! engines diverge most, because the strategies they reach for degrade in
//! completely different ways. A pre-filter narrows the candidate set and then
//! searches it exactly, so it stays accurate and gets slower as the predicate
//! widens. A post-filter searches the index for `k` and then discards the
//! non-matching hits, so it stays fast and returns *fewer than k* rows as the
//! predicate narrows. A filtered traversal walks the graph while skipping
//! non-matching nodes, so it degrades somewhere between the two. A latency
//! number cannot tell those apart; recall measured against a filter-aware
//! answer key can, which is why the predicate has to be something this harness
//! understands rather than an opaque per-dialect string.
//!
//! That is the constraint the grammar here is built around. A filter must be:
//!
//! 1. **Evaluable in-process**, against the [`BenchValue`] rows the corpus
//!    generator reproduces, so exact top-k *among matching rows* can be
//!    computed without asking any engine.
//! 2. **Renderable per dialect**, so every engine is asked the same question.
//!
//! [`Scan::condition`](crate::Scan) already carries raw per-dialect SQL, and it
//! satisfies (2) alone. It cannot satisfy (1): the harness would have no way to
//! know which rows the fragment selects, so there would be no answer key and
//! the filtered legs would report latency against an unknown accuracy — the
//! exact gap the recall work exists to close.

use crate::dialect::Dialect;
use crate::value::BenchValue;
use crate::valueprovider::{ColumnType, Columns};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Write as _;

/// Characters permitted in a text literal.
///
/// Every engine here quotes strings differently — SurrealQL escapes with
/// backslashes, ANSI SQL doubles the quote, RediSearch escapes inside a tag
/// with backslashes and additionally treats a long list of punctuation as
/// syntax. Rendering one literal three correct ways is a bug waiting to
/// happen, and a filter predicate is not the place to discover it: a
/// mis-escaped literal does not fail loudly, it silently selects a different
/// set of rows in one engine than in another, and the recall gap that follows
/// looks exactly like an index quality difference.
///
/// Restricting the charset side-steps all of it. The categorical columns a
/// filter is worth pointing at — `string_enum` labels, status names, region
/// codes — live comfortably inside it.
fn is_safe_literal_char(c: char) -> bool {
	c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '-' | '.' | '/' | '+' | ':' | '@')
}

/// One side of a comparison: a literal drawn from the benchmark TOML.
///
/// Untagged, so the config writes `2`, `1.5`, `true` or `"published"` directly
/// rather than tagging the type. Variant order is load-bearing — `Int` has to
/// precede `Float`, or a whole number would deserialise into a float and
/// compare against an integer column through an unnecessary cast.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum FilterScalar {
	Bool(bool),
	Int(i64),
	Float(f64),
	Text(String),
}

impl FilterScalar {
	/// Whether this literal is a number, and so usable with an ordering
	/// comparison.
	fn is_numeric(&self) -> bool {
		matches!(self, FilterScalar::Int(_) | FilterScalar::Float(_))
	}

	/// As an f64, for comparisons that cannot stay in integer arithmetic.
	fn as_f64(&self) -> Option<f64> {
		match self {
			FilterScalar::Int(i) => Some(*i as f64),
			FilterScalar::Float(f) => Some(*f),
			_ => None,
		}
	}

	/// Canonical rendering, used for the ground-truth cache fingerprint. It
	/// only has to be injective, not pretty.
	fn fingerprint(&self) -> String {
		match self {
			FilterScalar::Bool(b) => format!("b:{b}"),
			FilterScalar::Int(i) => format!("i:{i}"),
			// `{:?}` on f64 round-trips, where `{}` can collapse distinct
			// values to the same text.
			FilterScalar::Float(f) => format!("f:{f:?}"),
			FilterScalar::Text(t) => format!("t:{t}"),
		}
	}

	/// Reject anything the renderers cannot quote identically across dialects.
	fn validate(&self) -> Result<()> {
		match self {
			FilterScalar::Text(t) => {
				if t.is_empty() {
					bail!("a text filter value must not be empty");
				}
				if let Some(bad) = t.chars().find(|c| !is_safe_literal_char(*c)) {
					bail!(
						"text filter value {t:?} contains {bad:?}, which the per-dialect \
						 renderers cannot quote identically; use letters, digits, space or \
						 any of `_-./+:@`"
					);
				}
				Ok(())
			}
			FilterScalar::Float(f) => {
				if f.is_finite() {
					Ok(())
				} else {
					bail!("a numeric filter value must be finite, got {f}")
				}
			}
			_ => Ok(()),
		}
	}

	/// SQL / SurrealQL literal. Safe to inline because [`Self::validate`] has
	/// already restricted text to a charset that needs no escaping.
	fn to_sql_literal(&self) -> String {
		match self {
			FilterScalar::Bool(b) => b.to_string(),
			FilterScalar::Int(i) => i.to_string(),
			// Always render a decimal point, so a whole float stays a float
			// literal rather than becoming an integer the engine may type
			// differently.
			FilterScalar::Float(f) => format!("{f:?}"),
			FilterScalar::Text(t) => format!("'{t}'"),
		}
	}

	/// RediSearch tag literal: everything outside `[A-Za-z0-9_]` is escaped,
	/// which covers the whole permitted charset without having to track
	/// RediSearch's exact punctuation list.
	fn to_redis_tag(&self) -> String {
		let raw = match self {
			FilterScalar::Bool(b) => b.to_string(),
			FilterScalar::Int(i) => i.to_string(),
			FilterScalar::Float(f) => format!("{f:?}"),
			FilterScalar::Text(t) => t.clone(),
		};
		let mut out = String::with_capacity(raw.len());
		for c in raw.chars() {
			if !(c.is_ascii_alphanumeric() || c == '_') {
				out.push('\\');
			}
			out.push(c);
		}
		out
	}

	/// RediSearch numeric literal for a `@field:[min max]` range.
	fn to_redis_number(&self) -> Result<String> {
		match self {
			FilterScalar::Int(i) => Ok(i.to_string()),
			FilterScalar::Float(f) => Ok(format!("{f}")),
			other => bail!("redis: {other:?} is not a numeric filter value"),
		}
	}
}

/// Comparison applied between a column and a literal.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FilterOp {
	Eq,
	Ne,
	Lt,
	Lte,
	Gt,
	Gte,
	/// Membership in a list — the natural way to dial selectivity on a
	/// categorical column, where each additional label adds its own share.
	In,
}

impl FilterOp {
	/// Whether the operator orders its operands, and so needs numbers.
	fn is_ordering(self) -> bool {
		matches!(self, FilterOp::Lt | FilterOp::Lte | FilterOp::Gt | FilterOp::Gte)
	}

	/// SQL / SurrealQL spelling. `IN` is handled by the caller because its
	/// right-hand side is bracketed differently per dialect.
	fn to_sql(self) -> &'static str {
		match self {
			FilterOp::Eq => "=",
			FilterOp::Ne => "!=",
			FilterOp::Lt => "<",
			FilterOp::Lte => "<=",
			FilterOp::Gt => ">",
			FilterOp::Gte => ">=",
			FilterOp::In => "IN",
		}
	}
}

/// The right-hand side: one literal, or a list for [`FilterOp::In`].
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum FilterValue {
	One(FilterScalar),
	Many(Vec<FilterScalar>),
}

impl FilterValue {
	/// The literals, however many there are, so callers that treat both forms
	/// alike do not have to match.
	fn scalars(&self) -> &[FilterScalar] {
		match self {
			FilterValue::One(v) => std::slice::from_ref(v),
			FilterValue::Many(v) => v.as_slice(),
		}
	}
}

/// How a filter column is declared in a RediSearch index.
///
/// RediSearch has no general scalar type: a column is either `NUMERIC`, and
/// queried with a range, or `TAG`, and queried by exact label. The choice is
/// fixed at `FT.CREATE` time, before any scan runs, so it is derived from the
/// schema's column type rather than from the predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterFieldKind {
	Numeric,
	Tag,
}

/// A filter column, resolved against the schema.
///
/// Engines that index filter columns separately from the primary record —
/// Redis dual-writes them into the `vec:{key}` HASH the vector index is built
/// over — need the full set before the first row is written, which is well
/// before any scan is configured. This is how that set reaches them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilterField {
	pub(crate) name: String,
	pub(crate) kind: FilterFieldKind,
}

/// One predicate restricting a KNN scan, named so its leg can be told apart in
/// the results.
///
/// Selectivity is deliberately *not* configured. It is measured: the
/// ground-truth sweep regenerates every row to compute exact top-k anyway, so
/// it counts matches while it is there and reports the share that actually
/// matched. A configured selectivity would be a claim about the data; the
/// measured one is a fact about it, and the two diverge the moment anyone
/// edits the value template.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorFilter {
	/// Short label for this predicate, used in leg headings and result rows.
	pub(crate) name: String,
	/// Column the predicate tests.
	pub(crate) field: String,
	pub(crate) op: FilterOp,
	pub(crate) value: FilterValue,
}

impl VectorFilter {
	/// Stable digest of the predicate, for the ground-truth cache key. Two
	/// filters selecting different rows must never share one.
	pub(crate) fn fingerprint(&self) -> String {
		let values = self
			.value
			.scalars()
			.iter()
			.map(FilterScalar::fingerprint)
			.collect::<Vec<_>>()
			.join(",");
		// The name is excluded on purpose: renaming a predicate does not change
		// which rows it selects, so it must not invalidate a cached answer key.
		format!("{}\u{1e}{:?}\u{1e}[{values}]", self.field, self.op)
	}

	/// Check the predicate against the schema, and against what the renderers
	/// can express.
	///
	/// Called once at startup rather than per row: a filter that cannot be
	/// evaluated should stop the run before an engine is started, not midway
	/// through a ground-truth sweep over a million rows.
	pub(crate) fn validate(&self, columns: &Columns) -> Result<()> {
		if self.name.trim().is_empty() {
			bail!("a vector filter must have a non-empty `name`");
		}
		if self.field.trim().is_empty() {
			bail!("vector filter `{}`: `field` must be non-empty", self.name);
		}
		let Some((_, column)) = columns.0.iter().find(|(n, _)| n == &self.field) else {
			bail!(
				"vector filter `{}`: field `{}` is not a column in the value template",
				self.name,
				self.field
			);
		};
		// Only the types every engine can index and compare identically. A
		// datetime or decimal filter would need per-engine literal formats and
		// per-engine collation rules, and getting either subtly wrong produces
		// two engines answering different questions.
		let kind = match column {
			ColumnType::Integer | ColumnType::Float => FilterFieldKind::Numeric,
			ColumnType::String | ColumnType::Bool => FilterFieldKind::Tag,
			other => bail!(
				"vector filter `{}`: field `{}` is a {other:?} column; filters are supported on \
				 integer, float, string and boolean columns",
				self.name,
				self.field
			),
		};
		let values = self.value.scalars();
		if values.is_empty() {
			bail!("vector filter `{}`: the value list must not be empty", self.name);
		}
		for v in values {
			v.validate().map_err(|e| anyhow::anyhow!("vector filter `{}`: {e}", self.name))?;
		}
		match self.op {
			FilterOp::In => {
				if matches!(self.value, FilterValue::One(_)) {
					bail!("vector filter `{}`: `in` takes a list, e.g. value = [0, 1]", self.name);
				}
			}
			op if op.is_ordering() => {
				if matches!(self.value, FilterValue::Many(_)) {
					bail!(
						"vector filter `{}`: `{}` takes a single value, not a list",
						self.name,
						format!("{:?}", op).to_lowercase()
					);
				}
				if !values[0].is_numeric() {
					bail!(
						"vector filter `{}`: `{}` orders its operands, so the value must be a \
						 number",
						self.name,
						format!("{:?}", op).to_lowercase()
					);
				}
			}
			_ => {
				if matches!(self.value, FilterValue::Many(_)) {
					bail!(
						"vector filter `{}`: `{}` takes a single value; use `in` for a list",
						self.name,
						format!("{:?}", self.op).to_lowercase()
					);
				}
			}
		}
		// A numeric column compared against a label (or the reverse) would
		// evaluate to a constant `false` in-process while the engine might
		// reject the query outright — two different wrong answers.
		let numeric_column = kind == FilterFieldKind::Numeric;
		for v in values {
			match (numeric_column, v) {
				(true, v) if !v.is_numeric() => bail!(
					"vector filter `{}`: field `{}` is numeric, but the value {v:?} is not",
					self.name,
					self.field
				),
				(false, v) if v.is_numeric() => bail!(
					"vector filter `{}`: field `{}` is not numeric, but the value {v:?} is",
					self.name,
					self.field
				),
				_ => {}
			}
		}
		Ok(())
	}

	/// The Redis field kind this predicate's column maps to. Only meaningful
	/// after [`Self::validate`] has accepted the column.
	pub(crate) fn field_kind(&self, columns: &Columns) -> Option<FilterFieldKind> {
		columns.0.iter().find(|(n, _)| n == &self.field).and_then(|(_, t)| match t {
			ColumnType::Integer | ColumnType::Float => Some(FilterFieldKind::Numeric),
			ColumnType::String | ColumnType::Bool => Some(FilterFieldKind::Tag),
			_ => None,
		})
	}

	/// Whether a generated row satisfies the predicate.
	///
	/// This is the definition the answer key is built from, so it is also the
	/// definition every engine is measured against: an engine whose predicate
	/// semantics differ from this one shows up as a recall gap, which is the
	/// intended behaviour rather than a flaw in the measurement.
	pub(crate) fn matches(&self, row: &BenchValue) -> Result<bool> {
		let Some(cell) = row.get_field(&self.field) else {
			bail!("vector filter `{}`: row has no field `{}`", self.name, self.field);
		};
		let values = self.value.scalars();
		Ok(match self.op {
			FilterOp::Eq => equals(cell, &values[0]),
			FilterOp::Ne => !equals(cell, &values[0]),
			FilterOp::In => values.iter().any(|v| equals(cell, v)),
			op => match compare(cell, &values[0]) {
				// A row whose cell is not comparable with the literal cannot
				// satisfy an ordering predicate. `validate` has already ruled
				// out a type mismatch coming from the config, so this is the
				// residual case of a NULL-ish cell.
				None => false,
				Some(ord) => match op {
					FilterOp::Lt => ord == Ordering::Less,
					FilterOp::Lte => ord != Ordering::Greater,
					FilterOp::Gt => ord == Ordering::Greater,
					FilterOp::Gte => ord != Ordering::Less,
					_ => unreachable!("eq/ne/in handled above"),
				},
			},
		})
	}

	/// Render as a SQL / SurrealQL boolean expression, with identifiers escaped
	/// by the caller's dialect.
	///
	/// The two dialects agree on every operator here; they differ only in how
	/// an `IN` list is bracketed, which [`ListSyntax`] carries.
	pub(crate) fn to_sql<D: Dialect>(&self, list: ListSyntax) -> String {
		let field = D::escape_field(self.field.clone());
		match self.op {
			FilterOp::In => {
				let items = self
					.value
					.scalars()
					.iter()
					.map(FilterScalar::to_sql_literal)
					.collect::<Vec<_>>()
					.join(", ");
				let (open, close) = list.brackets();
				format!("{field} IN {open}{items}{close}")
			}
			op => {
				format!("{field} {} {}", op.to_sql(), self.value.scalars()[0].to_sql_literal())
			}
		}
	}

	/// Render as a RediSearch query expression, over the HASH field a vector
	/// row's filter columns are mirrored into.
	pub(crate) fn to_redis(&self, kind: FilterFieldKind) -> Result<String> {
		let field = redis_field_name(&self.field);
		let values = self.value.scalars();
		Ok(match kind {
			FilterFieldKind::Tag => match self.op {
				FilterOp::Eq => format!("@{field}:{{{}}}", values[0].to_redis_tag()),
				FilterOp::Ne => format!("-@{field}:{{{}}}", values[0].to_redis_tag()),
				FilterOp::In => {
					// RediSearch unions tags inside one brace group with `|`.
					let items =
						values.iter().map(FilterScalar::to_redis_tag).collect::<Vec<_>>().join("|");
					format!("@{field}:{{{items}}}")
				}
				op => bail!("redis: `{}` is not supported on a TAG field", op.to_sql()),
			},
			FilterFieldKind::Numeric => {
				let v = values[0].to_redis_number();
				match self.op {
					FilterOp::Eq => {
						let v = v?;
						format!("@{field}:[{v} {v}]")
					}
					FilterOp::Ne => {
						let v = v?;
						format!("-@{field}:[{v} {v}]")
					}
					// `(` marks an exclusive bound in a RediSearch range.
					FilterOp::Lt => format!("@{field}:[-inf ({}]", v?),
					FilterOp::Lte => format!("@{field}:[-inf {}]", v?),
					FilterOp::Gt => format!("@{field}:[({} +inf]", v?),
					FilterOp::Gte => format!("@{field}:[{} +inf]", v?),
					FilterOp::In => {
						let mut out = String::from("(");
						for (i, value) in values.iter().enumerate() {
							if i > 0 {
								out.push_str(" | ");
							}
							let n = value.to_redis_number()?;
							let _ = write!(out, "@{field}:[{n} {n}]");
						}
						out.push(')');
						out
					}
				}
			}
		})
	}
}

/// How a dialect brackets an `IN` list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListSyntax {
	/// `IN (a, b)` — ANSI SQL.
	Parens,
	/// `IN [a, b]` — SurrealQL, where a list is an array literal.
	Brackets,
}

impl ListSyntax {
	fn brackets(self) -> (char, char) {
		match self {
			ListSyntax::Parens => ('(', ')'),
			ListSyntax::Brackets => ('[', ']'),
		}
	}
}

/// HASH field a filter column is mirrored into for the Redis vector index.
///
/// Prefixed so a column named `v` cannot collide with the embedding the vector
/// index is defined over.
pub(crate) fn redis_field_name(column: &str) -> String {
	format!("f_{column}")
}

/// Equality between a generated cell and a config literal.
fn equals(cell: &BenchValue, value: &FilterScalar) -> bool {
	match (cell, value) {
		(BenchValue::Bool(a), FilterScalar::Bool(b)) => a == b,
		(BenchValue::String(a), FilterScalar::Text(b)) => a == b,
		// `int_enum` yields whichever integer variant fits, so both are
		// compared through the same ordering path rather than by variant.
		(BenchValue::Int(_) | BenchValue::UInt(_) | BenchValue::Float(_), v) if v.is_numeric() => {
			compare(cell, value) == Some(Ordering::Equal)
		}
		_ => false,
	}
}

/// Ordering between a generated cell and a config literal, when both are
/// numbers. `None` for anything else, which callers read as "cannot satisfy an
/// ordering predicate".
fn compare(cell: &BenchValue, value: &FilterScalar) -> Option<Ordering> {
	match (cell, value) {
		// Integer against integer stays in integer arithmetic: an i64 beyond
		// 2^53 does not survive the trip through f64, and a benchmark is
		// entitled to use one as a key-like column.
		(BenchValue::Int(a), FilterScalar::Int(b)) => Some(a.cmp(b)),
		(BenchValue::UInt(a), FilterScalar::Int(b)) => {
			if *b < 0 {
				Some(Ordering::Greater)
			} else {
				Some(a.cmp(&(*b as u64)))
			}
		}
		_ => {
			let a = match cell {
				BenchValue::Int(i) => *i as f64,
				BenchValue::UInt(u) => *u as f64,
				BenchValue::Float(f) => *f,
				_ => return None,
			};
			let b = value.as_f64()?;
			a.partial_cmp(&b)
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::dialect::{AnsiSqlDialect, SurrealDBDialect};
	use crate::valueprovider::ValueProvider;

	const TEMPLATE: &str = r#"{
		"number": "int:1..5000",
		"score": "float:0.0..100.0",
		"status": "string_enum:draft,published,archived",
		"city": "string_enum:London,New York",
		"active": "bool",
		"created_at": "datetime",
		"embedding": "vector:8"
	}"#;

	fn columns() -> Columns {
		ValueProvider::new(TEMPLATE).unwrap().columns()
	}

	fn filter(json: &str) -> VectorFilter {
		serde_json::from_str(json).unwrap()
	}

	fn row(pairs: Vec<(&str, BenchValue)>) -> BenchValue {
		BenchValue::Object(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
	}

	#[test]
	fn parses_each_scalar_shape() {
		let f = filter(r#"{"name":"n","field":"number","op":"lte","value":50}"#);
		assert_eq!(f.value, FilterValue::One(FilterScalar::Int(50)));
		let f = filter(r#"{"name":"n","field":"score","op":"gt","value":12.5}"#);
		assert_eq!(f.value, FilterValue::One(FilterScalar::Float(12.5)));
		let f = filter(r#"{"name":"n","field":"active","op":"eq","value":true}"#);
		assert_eq!(f.value, FilterValue::One(FilterScalar::Bool(true)));
		let f = filter(r#"{"name":"n","field":"status","op":"eq","value":"draft"}"#);
		assert_eq!(f.value, FilterValue::One(FilterScalar::Text("draft".into())));
		let f = filter(r#"{"name":"n","field":"status","op":"in","value":["draft","archived"]}"#);
		assert_eq!(f.value.scalars().len(), 2);
	}

	#[test]
	fn matches_numeric_and_categorical_rows() {
		let cols = columns();
		let f = filter(r#"{"name":"n","field":"number","op":"lte","value":50}"#);
		f.validate(&cols).unwrap();
		assert!(f.matches(&row(vec![("number", BenchValue::Int(50))])).unwrap());
		assert!(!f.matches(&row(vec![("number", BenchValue::Int(51))])).unwrap());

		let f = filter(r#"{"name":"n","field":"status","op":"in","value":["draft","archived"]}"#);
		f.validate(&cols).unwrap();
		assert!(f.matches(&row(vec![("status", BenchValue::String("archived".into()))])).unwrap());
		assert!(
			!f.matches(&row(vec![("status", BenchValue::String("published".into()))])).unwrap()
		);

		let f = filter(r#"{"name":"n","field":"active","op":"eq","value":true}"#);
		f.validate(&cols).unwrap();
		assert!(f.matches(&row(vec![("active", BenchValue::Bool(true))])).unwrap());
		assert!(!f.matches(&row(vec![("active", BenchValue::Bool(false))])).unwrap());

		let f = filter(r#"{"name":"n","field":"number","op":"ne","value":7}"#);
		assert!(f.matches(&row(vec![("number", BenchValue::Int(8))])).unwrap());
		assert!(!f.matches(&row(vec![("number", BenchValue::UInt(7))])).unwrap());
	}

	#[test]
	fn ordering_survives_integers_beyond_f64_precision() {
		// 2^53 and its successor collapse to the same f64, so an i64 path is
		// the only one that can tell them apart.
		let f = filter(r#"{"name":"n","field":"number","op":"gt","value":9007199254740992}"#);
		assert!(!f.matches(&row(vec![("number", BenchValue::Int(9007199254740992))])).unwrap());
		assert!(f.matches(&row(vec![("number", BenchValue::Int(9007199254740993))])).unwrap());
	}

	#[test]
	fn missing_field_is_an_error_not_a_silent_miss() {
		let f = filter(r#"{"name":"n","field":"number","op":"lte","value":50}"#);
		assert!(f.matches(&row(vec![("other", BenchValue::Int(1))])).is_err());
	}

	#[test]
	fn rejects_predicates_the_harness_cannot_honour() {
		let cols = columns();
		// Unknown column.
		let f = filter(r#"{"name":"n","field":"nope","op":"eq","value":1}"#);
		assert!(f.validate(&cols).is_err());
		// Column type with no agreed cross-engine comparison.
		let f = filter(r#"{"name":"n","field":"created_at","op":"eq","value":"x"}"#);
		assert!(f.validate(&cols).is_err());
		// Ordering against a label.
		let f = filter(r#"{"name":"n","field":"status","op":"gt","value":"draft"}"#);
		assert!(f.validate(&cols).is_err());
		// Numeric column against a label.
		let f = filter(r#"{"name":"n","field":"number","op":"eq","value":"draft"}"#);
		assert!(f.validate(&cols).is_err());
		// Label column against a number.
		let f = filter(r#"{"name":"n","field":"status","op":"eq","value":3}"#);
		assert!(f.validate(&cols).is_err());
		// `in` with a single value, and a scalar op with a list.
		let f = filter(r#"{"name":"n","field":"number","op":"in","value":1}"#);
		assert!(f.validate(&cols).is_err());
		let f = filter(r#"{"name":"n","field":"number","op":"eq","value":[1,2]}"#);
		assert!(f.validate(&cols).is_err());
		// Empty list.
		let f = filter(r#"{"name":"n","field":"number","op":"in","value":[]}"#);
		assert!(f.validate(&cols).is_err());
		// Unnamed.
		let f = filter(r#"{"name":"  ","field":"number","op":"eq","value":1}"#);
		assert!(f.validate(&cols).is_err());
		// A literal the renderers cannot quote identically everywhere.
		let f = filter(r#"{"name":"n","field":"status","op":"eq","value":"o'brien"}"#);
		assert!(f.validate(&cols).is_err());
	}

	#[test]
	fn renders_sql_per_dialect() {
		let f = filter(r#"{"name":"n","field":"number","op":"lte","value":50}"#);
		assert_eq!(f.to_sql::<AnsiSqlDialect>(ListSyntax::Parens), r#""number" <= 50"#);
		assert_eq!(f.to_sql::<SurrealDBDialect>(ListSyntax::Brackets), "number <= 50");

		let f = filter(r#"{"name":"n","field":"status","op":"in","value":["draft","archived"]}"#);
		assert_eq!(
			f.to_sql::<AnsiSqlDialect>(ListSyntax::Parens),
			r#""status" IN ('draft', 'archived')"#
		);
		assert_eq!(
			f.to_sql::<SurrealDBDialect>(ListSyntax::Brackets),
			"status IN ['draft', 'archived']"
		);

		let f = filter(r#"{"name":"n","field":"active","op":"eq","value":false}"#);
		assert_eq!(f.to_sql::<SurrealDBDialect>(ListSyntax::Brackets), "active = false");

		// A whole float keeps its decimal point rather than becoming an int.
		let f = filter(r#"{"name":"n","field":"score","op":"gte","value":2.0}"#);
		assert_eq!(f.to_sql::<SurrealDBDialect>(ListSyntax::Brackets), "score >= 2.0");
	}

	#[test]
	fn renders_redis_query_expressions() {
		let f = filter(r#"{"name":"n","field":"number","op":"lte","value":50}"#);
		assert_eq!(f.to_redis(FilterFieldKind::Numeric).unwrap(), "@f_number:[-inf 50]");
		let f = filter(r#"{"name":"n","field":"number","op":"gt","value":50}"#);
		assert_eq!(f.to_redis(FilterFieldKind::Numeric).unwrap(), "@f_number:[(50 +inf]");
		let f = filter(r#"{"name":"n","field":"number","op":"eq","value":7}"#);
		assert_eq!(f.to_redis(FilterFieldKind::Numeric).unwrap(), "@f_number:[7 7]");
		let f = filter(r#"{"name":"n","field":"number","op":"in","value":[1,2]}"#);
		assert_eq!(
			f.to_redis(FilterFieldKind::Numeric).unwrap(),
			"(@f_number:[1 1] | @f_number:[2 2])"
		);

		let f = filter(r#"{"name":"n","field":"status","op":"eq","value":"draft"}"#);
		assert_eq!(f.to_redis(FilterFieldKind::Tag).unwrap(), "@f_status:{draft}");
		let f = filter(r#"{"name":"n","field":"status","op":"ne","value":"draft"}"#);
		assert_eq!(f.to_redis(FilterFieldKind::Tag).unwrap(), "-@f_status:{draft}");
		let f = filter(r#"{"name":"n","field":"status","op":"in","value":["draft","archived"]}"#);
		assert_eq!(f.to_redis(FilterFieldKind::Tag).unwrap(), "@f_status:{draft|archived}");
		// A space inside a tag is escaped rather than splitting the term.
		let f = filter(r#"{"name":"n","field":"city","op":"eq","value":"New York"}"#);
		assert_eq!(f.to_redis(FilterFieldKind::Tag).unwrap(), r"@f_city:{New\ York}");
		// Ordering has no meaning on a TAG field, and is refused rather than
		// silently rendered as something else.
		let f = filter(r#"{"name":"n","field":"status","op":"gt","value":"draft"}"#);
		assert!(f.to_redis(FilterFieldKind::Tag).is_err());
	}

	#[test]
	fn fingerprint_separates_predicates_and_ignores_the_name() {
		let a = filter(r#"{"name":"a","field":"number","op":"lte","value":50}"#);
		let renamed = filter(r#"{"name":"b","field":"number","op":"lte","value":50}"#);
		// Renaming does not change which rows are selected, so it must not
		// invalidate a cached answer key.
		assert_eq!(a.fingerprint(), renamed.fingerprint());

		let widened = filter(r#"{"name":"a","field":"number","op":"lte","value":500}"#);
		assert_ne!(a.fingerprint(), widened.fingerprint());
		let other_op = filter(r#"{"name":"a","field":"number","op":"lt","value":50}"#);
		assert_ne!(a.fingerprint(), other_op.fingerprint());
		let other_field = filter(r#"{"name":"a","field":"score","op":"lte","value":50}"#);
		assert_ne!(a.fingerprint(), other_field.fingerprint());
		// A list and the concatenation of its parts must not collide.
		let list = filter(r#"{"name":"a","field":"status","op":"in","value":["a","b"]}"#);
		let single = filter(r#"{"name":"a","field":"status","op":"in","value":["a,b"]}"#);
		assert_ne!(list.fingerprint(), single.fingerprint());
	}

	#[test]
	fn field_kind_follows_the_column_type() {
		let cols = columns();
		let f = filter(r#"{"name":"n","field":"number","op":"lte","value":50}"#);
		assert_eq!(f.field_kind(&cols), Some(FilterFieldKind::Numeric));
		let f = filter(r#"{"name":"n","field":"score","op":"lte","value":50.0}"#);
		assert_eq!(f.field_kind(&cols), Some(FilterFieldKind::Numeric));
		let f = filter(r#"{"name":"n","field":"status","op":"eq","value":"draft"}"#);
		assert_eq!(f.field_kind(&cols), Some(FilterFieldKind::Tag));
		let f = filter(r#"{"name":"n","field":"active","op":"eq","value":true}"#);
		assert_eq!(f.field_kind(&cols), Some(FilterFieldKind::Tag));
	}
}
