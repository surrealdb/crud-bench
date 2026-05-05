use anyhow::{Result, anyhow};
use redis::aio::MultiplexedConnection;
use serde_json::Value;
use std::hint::black_box;
use tokio::sync::Mutex;

pub(crate) async fn batch_create_u32(
	conn: &Mutex<MultiplexedConnection>,
	key_vals: Vec<(u32, Value)>,
) -> Result<()> {
	if key_vals.is_empty() {
		return Ok(());
	}
	let mut c = conn.lock().await;
	let mut pipe = redis::pipe();
	for (k, v) in key_vals {
		let val = bincode::serde::encode_to_vec(&v, bincode::config::standard())?;
		pipe.cmd("SET").arg(k).arg(val).ignore();
	}
	pipe.exec_async(&mut *c).await?;
	Ok(())
}

pub(crate) async fn batch_create_string(
	conn: &Mutex<MultiplexedConnection>,
	key_vals: Vec<(String, Value)>,
) -> Result<()> {
	if key_vals.is_empty() {
		return Ok(());
	}
	let mut c = conn.lock().await;
	let mut pipe = redis::pipe();
	for (k, v) in key_vals {
		let val = bincode::serde::encode_to_vec(&v, bincode::config::standard())?;
		pipe.cmd("SET").arg(k).arg(val).ignore();
	}
	pipe.exec_async(&mut *c).await?;
	Ok(())
}

pub(crate) async fn batch_read_u32(
	conn: &Mutex<MultiplexedConnection>,
	keys: Vec<u32>,
) -> Result<()> {
	if keys.is_empty() {
		return Ok(());
	}
	let mut c = conn.lock().await;
	let mut pipe = redis::pipe();
	for k in &keys {
		pipe.cmd("GET").arg(k);
	}
	let vals: Vec<Option<Vec<u8>>> = pipe.query_async(&mut *c).await?;
	assert_eq!(vals.len(), keys.len());
	for v in vals {
		let v = v.ok_or_else(|| anyhow!("missing key"))?;
		assert!(!v.is_empty());
		black_box(v);
	}
	Ok(())
}

pub(crate) async fn batch_read_string(
	conn: &Mutex<MultiplexedConnection>,
	keys: Vec<String>,
) -> Result<()> {
	if keys.is_empty() {
		return Ok(());
	}
	let mut c = conn.lock().await;
	let mut pipe = redis::pipe();
	for k in &keys {
		pipe.cmd("GET").arg(k);
	}
	let vals: Vec<Option<Vec<u8>>> = pipe.query_async(&mut *c).await?;
	assert_eq!(vals.len(), keys.len());
	for v in vals {
		let v = v.ok_or_else(|| anyhow!("missing key"))?;
		assert!(!v.is_empty());
		black_box(v);
	}
	Ok(())
}

pub(crate) async fn batch_update_u32(
	conn: &Mutex<MultiplexedConnection>,
	key_vals: Vec<(u32, Value)>,
) -> Result<()> {
	batch_create_u32(conn, key_vals).await
}

pub(crate) async fn batch_update_string(
	conn: &Mutex<MultiplexedConnection>,
	key_vals: Vec<(String, Value)>,
) -> Result<()> {
	batch_create_string(conn, key_vals).await
}

pub(crate) async fn batch_delete_u32(
	conn: &Mutex<MultiplexedConnection>,
	keys: Vec<u32>,
) -> Result<()> {
	if keys.is_empty() {
		return Ok(());
	}
	let mut c = conn.lock().await;
	let mut pipe = redis::pipe();
	for k in keys {
		pipe.cmd("DEL").arg(k).ignore();
	}
	pipe.exec_async(&mut *c).await?;
	Ok(())
}

pub(crate) async fn batch_delete_string(
	conn: &Mutex<MultiplexedConnection>,
	keys: Vec<String>,
) -> Result<()> {
	if keys.is_empty() {
		return Ok(());
	}
	let mut c = conn.lock().await;
	let mut pipe = redis::pipe();
	for k in keys {
		pipe.cmd("DEL").arg(k).ignore();
	}
	pipe.exec_async(&mut *c).await?;
	Ok(())
}
