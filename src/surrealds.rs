#![cfg(feature = "surrealdb")]

//! # SurrealDS - Multi-Instance SurrealDB Client Provider
//!
//! This module provides the `SurrealDBClientsProvider`, which enables benchmarking against
//! multiple SurrealDB instances simultaneously. This is particularly useful for testing
//! distributed SurrealDB deployments, load balancing scenarios, and assessing the performance
//! characteristics of multi-node SurrealDB clusters.
//!
//! ## Features
//!
//! - **Multi-Instance Support**: Connect to multiple SurrealDB instances using a single endpoint configuration
//! - **Round-Robin Load Balancing**: Automatically distributes client connections across available instances
//! - **Networked Connections Only**: Designed for remote SurrealDB instances (ws://, wss://, http://, https://)
//!
//! ## Usage
//!
//! The SurrealDS feature is enabled through the `surrealdb` feature flag and requires specifying
//! multiple endpoints in the endpoint configuration, separated by semicolons:
//!
//! ```bash
//! cargo run -r -- -d surrealdb -e "ws://127.0.0.1:8001;ws://127.0.0.1:8002;ws://127.0.0.1:8003" -s 100000
//! ```
//!
//! ## Architecture
//!
//! The `SurrealDBClientsProvider` implements the `BenchmarkEngine` trait and creates
//! `SurrealDBClient` instances on demand. Each client creation request uses the round-robin
//! algorithm to select the next available endpoint, ensuring even distribution of connections
//! across all configured SurrealDB instances.

use crate::engine::BenchmarkEngine;
use crate::surrealdb::{SurrealDBClient, initialise_db};
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType};
use anyhow::{Result, bail};
use std::sync::atomic::{AtomicUsize, Ordering};
use surrealdb::opt::auth::Root;

/// Default endpoints for SurrealDS when no custom endpoint is specified.
///
/// This configuration assumes three local SurrealDB instances running on ports 8001, 8002, and 8003.
/// In production scenarios, these would typically point to different hosts or distributed nodes.
const DEFAULT: &str = "ws://127.0.0.1:8001;ws://127.0.0.1:8002;ws://127.0.0.1:8003";

/// A benchmark engine provider that manages connections to multiple SurrealDB instances.
///
/// `SurrealDBClientsProvider` enables distributed benchmarking by maintaining a pool of
/// endpoints and creating client connections using a round-robin selection algorithm.
/// This ensures that benchmark workloads are evenly distributed across all configured
/// SurrealDB instances.
///
/// ## Connection Strategy
///
/// - Connections are created on-demand for each concurrent client
/// - Round-robin selection ensures even distribution of load
/// - Each endpoint must be a remote connection (ws://, wss://, http://, https://)
/// - All instances must be accessible and properly configured with the same credentials
///
/// ## Example Configuration
///
/// ```text
/// endpoints: ["ws://node1:8000", "ws://node2:8000", "ws://node3:8000"]
/// ```
///
/// When creating clients, the provider will cycle through endpoints in order:
/// - Client 0 → node1
/// - Client 1 → node2
/// - Client 2 → node3
/// - Client 3 → node1 (wraps around)
pub(crate) struct SurrealDBClientsProvider {
	/// Atomic counter for round-robin endpoint selection.
	///
	/// This counter is incremented atomically on each client creation to determine
	/// the next endpoint to use. The value is taken modulo the number of endpoints
	/// to cycle through the available instances.
	round_robin: AtomicUsize,

	/// List of SurrealDB endpoints to distribute connections across.
	///
	/// Each endpoint should be a valid remote connection string (e.g., "ws://host:port").
	/// The endpoints are parsed from the configuration string by splitting on semicolons.
	endpoints: Vec<String>,

	/// Root user authentication credentials.
	///
	/// These credentials are used to authenticate with all configured SurrealDB instances.
	/// All instances in the cluster must accept the same root credentials.
	root: Root,
}

impl BenchmarkEngine<SurrealDBClient> for SurrealDBClientsProvider {
	/// Initializes a new multi-instance SurrealDB benchmarking engine.
	///
	/// This method sets up the provider by parsing the endpoint configuration string,
	/// validating that all endpoints are remote connections, and preparing the
	/// round-robin counter for distributing client connections.
	///
	/// # Arguments
	///
	/// * `_` - Key type (unused, inherited from trait)
	/// * `_columns` - Column configuration (unused, inherited from trait)
	/// * `options` - Benchmark configuration containing the endpoint string
	///
	/// # Endpoint Format
	///
	/// The endpoint string should contain one or more SurrealDB endpoints separated
	/// by semicolons. Each endpoint must use a remote protocol (ws, wss, http, or https).
	///
	/// Example: `"ws://127.0.0.1:8001;ws://127.0.0.1:8002;ws://127.0.0.1:8003"`
	///
	/// # Returns
	///
	/// Returns a configured `SurrealDBClientsProvider` ready to create client connections,
	/// or an error if:
	/// - The endpoint configuration is empty
	/// - Any endpoint uses an unsupported protocol (e.g., embedded databases)
	///
	/// # Errors
	///
	/// This function will return an error if:
	/// - The endpoint string is empty or contains no valid endpoints
	/// - Any endpoint uses a non-remote protocol (mem://, file://, rocksdb://, surrealkv://)
	async fn setup(_: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		// Get the custom endpoint configuration from options, or use the default
		// three-instance local setup if no endpoint is specified
		let endpoint = options.endpoint.as_deref().unwrap_or(DEFAULT);

		// Define root user authentication credentials
		// All configured SurrealDB instances must accept these credentials
		let root = Root {
			username: String::from("root"),
			password: String::from("root"),
		};

		// Parse and validate endpoints from the semicolon-separated string
		let mut endpoints = Vec::new();
		for e in endpoint.split(';') {
			// Skip empty segments that result from trailing or double semicolons
			if e.is_empty() {
				continue;
			}

			// Validate that each endpoint uses a remote connection protocol
			// SurrealDS only supports networked instances, not embedded databases
			let scheme = e.split_once(':').map(|(scheme, _)| scheme).unwrap_or("");

			match scheme {
				"ws" | "wss" | "http" | "https" => endpoints.push(e.to_string()),
				_ => bail!("A remote connection is expected: {e}"),
			};
		}

		// Ensure at least one valid endpoint was provided
		if endpoints.is_empty() {
			bail!("Invalid endpoint: {endpoint}")
		}

		// Create the provider with initialized round-robin counter starting at 0
		Ok(Self {
			round_robin: AtomicUsize::new(0),
			endpoints,
			root,
		})
	}

	/// Creates a new SurrealDB client connected to one of the configured instances.
	///
	/// This method implements the round-robin load balancing strategy by:
	/// 1. Atomically incrementing the round-robin counter
	/// 2. Using modulo arithmetic to select the next endpoint
	/// 3. Establishing a connection to the selected endpoint
	/// 4. Returning a fully initialized and authenticated client
	///
	/// # Load Distribution
	///
	/// With 3 endpoints and 6 concurrent clients, the distribution would be:
	/// - Client 0 → endpoint 0
	/// - Client 1 → endpoint 1
	/// - Client 2 → endpoint 2
	/// - Client 3 → endpoint 0
	/// - Client 4 → endpoint 1
	/// - Client 5 → endpoint 2
	///
	/// # Returns
	///
	/// Returns a `SurrealDBClient` connected and authenticated to one of the
	/// configured SurrealDB instances, or an error if the connection fails.
	///
	/// # Errors
	///
	/// This function will return an error if:
	/// - The connection to the selected endpoint fails
	/// - Authentication with the root credentials fails
	/// - The namespace or database selection fails
	async fn create_client(&self) -> Result<SurrealDBClient> {
		// Atomically fetch the current counter value and increment it for the next call
		// Using Relaxed ordering is sufficient here as we don't need synchronization
		// beyond the atomic increment itself
		let next = self.round_robin.fetch_add(1, Ordering::Relaxed);

		// Select the endpoint using modulo to cycle through the available instances
		// This ensures even distribution: counter % endpoint_count
		let endpoint = &self.endpoints[next % self.endpoints.len()];

		// Initialize a new connection to the selected endpoint with root credentials
		// This function handles connection, authentication, and namespace/database selection
		let client = initialise_db(endpoint, self.root.clone()).await?;

		// Wrap the connected client in a SurrealDBClient for the benchmark interface
		Ok(SurrealDBClient::new(client))
	}
}
