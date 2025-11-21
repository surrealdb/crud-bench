#![cfg(feature = "surrealdb")]

use crate::engine::BenchmarkEngine;
use crate::surrealdb::{SurrealDBClient, initialise_db};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType};
use anyhow::{Result, bail};
use std::sync::atomic::{AtomicUsize, Ordering};
use surrealdb::opt::auth::Root;

const DEFAULT: &str = "ws://127.0.0.1:8001;ws://127.0.0.1:8002;ws://127.0.0.1:8003";

pub(crate) struct SurrealDBClientsProvider {
	round_robin: AtomicUsize,
	endpoints: Vec<String>,
	root: Root,
}

impl BenchmarkEngine<SurrealDBClient> for SurrealDBClientsProvider {
	/// Initiates a new datastore benchmarking engine
	async fn setup(_: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Get the custom endpoint if specified
		let endpoint = options.endpoint.as_deref().unwrap_or(DEFAULT);
		// Define root user details
		let root = Root {
			username: String::from("root"),
			password: String::from("root"),
		};

		let mut endpoints = Vec::new();
		for e in endpoint.split(';') {
			match endpoint.split_once(':').unwrap().0 {
				"ws" | "wss" | "http" | "https" => endpoints.push(e.to_string()),
				_ => bail!("A remote connection is expected: {e}"),
			};
		}
		if endpoints.is_empty() {
			bail!("Invalid endpoint: {endpoint}")
		}
		Ok(Self {
			round_robin: AtomicUsize::new(0),
			endpoints,
			root,
		})
	}
	/// Creates a new client for this benchmarking engine
	async fn create_client(&self) -> Result<SurrealDBClient> {
		let next = self.round_robin.fetch_add(1, Ordering::Relaxed);
		let endpoint = &self.endpoints[next % self.endpoints.len()];
		let client = initialise_db(endpoint, self.root.clone()).await?;
		Ok(SurrealDBClient::new(client))
	}
}
