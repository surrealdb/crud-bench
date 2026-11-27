//! Mixed scan/write legs: after each timed scan sample, optionally run compensating `UPDATE`s that
//! swap indexed column values between two rows so multiset statistics for equality predicates stay
//! stable while indexes are invalidated.

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, ScanContext};
use crate::keyprovider::{IntegerKeyProvider, KeyProvider, StringKeyProvider};
use crate::value::BenchValue;
use crate::{Scan, ScanWithWrites, ScanWritesOperation};
use anyhow::Result;

/// Deterministic subset of samples that run writes after the scan (spread via ratio).
pub(crate) fn sample_includes_writes(sample: u32, ratio: f64) -> bool {
	if ratio <= 0.0 {
		return false;
	}
	if ratio >= 1.0 {
		return true;
	}
	let threshold = (ratio.clamp(0.0, 1.0) * 1000.0).round() as u32;
	(sample.wrapping_mul(2654435761) % 1000) < threshold
}

const DEFAULT_SWAP_FIELDS: &[&str] = &["number"];

fn indexed_field_names(scan: &Scan) -> Vec<&str> {
	match scan.with_index.as_ref() {
		Some(i) if !i.fields.is_empty() => i.fields.iter().map(|s| s.as_str()).collect(),
		_ => DEFAULT_SWAP_FIELDS.to_vec(),
	}
}

/// Run one mixed sample: full scan (including `expect` assertion), then optional compensating swaps.
pub(crate) async fn run_scan_with_writes<C: BenchmarkClient>(
	client: &C,
	scan: &Scan,
	ctx: ScanContext,
	spec: &ScanWithWrites,
	sample: u32,
	samples: u32,
	kp: &mut KeyProvider,
) -> Result<()> {
	client.scan(scan, kp, ctx).await?;

	if spec.operation != ScanWritesOperation::Update {
		return Ok(());
	}
	if !sample_includes_writes(sample, spec.ratio) {
		return Ok(());
	}

	match kp {
		KeyProvider::OrderedInteger(p) => {
			let ka = p.key(sample.wrapping_mul(17) % samples);
			let kb = p.key(sample.wrapping_mul(31).wrapping_add(samples / 2) % samples);
			compensating_swap_u32(client, ka, kb, scan).await
		}
		KeyProvider::UnorderedInteger(p) => {
			let ka = p.key(sample.wrapping_mul(17) % samples);
			let kb = p.key(sample.wrapping_mul(31).wrapping_add(samples / 2) % samples);
			compensating_swap_u32(client, ka, kb, scan).await
		}
		KeyProvider::OrderedString(p) => {
			let ka = p.key(sample.wrapping_mul(17) % samples);
			let kb = p.key(sample.wrapping_mul(31).wrapping_add(samples / 2) % samples);
			compensating_swap_string(client, ka, kb, scan).await
		}
		KeyProvider::UnorderedString(p) => {
			let ka = p.key(sample.wrapping_mul(17) % samples);
			let kb = p.key(sample.wrapping_mul(31).wrapping_add(samples / 2) % samples);
			compensating_swap_string(client, ka, kb, scan).await
		}
	}
}

/// Replace the named field on an object-shaped [`BenchValue`].
fn replace_field(value: &mut BenchValue, field: &str, new_val: BenchValue) {
	if let BenchValue::Object(o) = value {
		for (k, v) in o.iter_mut() {
			if k == field {
				*v = new_val;
				return;
			}
		}
		o.push((field.to_string(), new_val));
	}
}

async fn compensating_swap_u32<C: BenchmarkClient>(
	client: &C,
	ka: u32,
	kb: u32,
	scan: &Scan,
) -> Result<()> {
	if ka == kb {
		return Ok(());
	}
	let fields = indexed_field_names(scan);

	let va_res = client.read_u32(ka).await;
	let vb_res = client.read_u32(kb).await;
	let (mut va, mut vb): (BenchValue, BenchValue) = match (va_res, vb_res) {
		(Err(ea), _) | (_, Err(ea)) if ea.to_string().contains(NOT_SUPPORTED_ERROR) => {
			return Ok(());
		}
		(Err(e), _) => return Err(e),
		(_, Err(e)) => return Err(e),
		(Ok(va), Ok(vb)) => (va.into(), vb.into()),
	};

	let mut swapped = false;
	for field in fields {
		let a_val = va.get_field(field).cloned();
		let b_val = vb.get_field(field).cloned();
		if let (Some(a), Some(b)) = (a_val, b_val) {
			replace_field(&mut va, field, b);
			replace_field(&mut vb, field, a);
			swapped = true;
		}
	}
	if swapped {
		client.update_u32(ka, va).await?;
		client.update_u32(kb, vb).await?;
	}
	Ok(())
}

async fn compensating_swap_string<C: BenchmarkClient>(
	client: &C,
	ka: String,
	kb: String,
	scan: &Scan,
) -> Result<()> {
	if ka == kb {
		return Ok(());
	}
	let fields = indexed_field_names(scan);

	let va_res = client.read_string(ka.clone()).await;
	let vb_res = client.read_string(kb.clone()).await;
	let (mut va, mut vb): (BenchValue, BenchValue) = match (va_res, vb_res) {
		(Err(ea), _) | (_, Err(ea)) if ea.to_string().contains(NOT_SUPPORTED_ERROR) => {
			return Ok(());
		}
		(Err(e), _) => return Err(e),
		(_, Err(e)) => return Err(e),
		(Ok(va), Ok(vb)) => (va.into(), vb.into()),
	};

	let mut swapped = false;
	for field in fields {
		let a_val = va.get_field(field).cloned();
		let b_val = vb.get_field(field).cloned();
		if let (Some(a), Some(b)) = (a_val, b_val) {
			replace_field(&mut va, field, b);
			replace_field(&mut vb, field, a);
			swapped = true;
		}
	}
	if swapped {
		client.update_string(ka, va).await?;
		client.update_string(kb, vb).await?;
	}
	Ok(())
}
