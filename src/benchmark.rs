use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io::Write;
use std::io::{stdout, IsTerminal, Stdout};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::Args;
use anyhow::{bail, Result};
use futures::future::try_join_all;
use log::{error, info};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::task;

const TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct Benchmark {
	/// The server endpoint to connect to
	endpoint: Option<String>,
	/// The timeout for connecting to the server
	timeout: Duration,
	/// The number of clients to spawn
	clients: u32,
	/// The number of threads to spawn
	threads: u32,
	/// The number of samples to run
	samples: i32,
}

impl Benchmark {
	pub(crate) fn new(args: &Args) -> Self {
		Self {
			endpoint: args.endpoint.to_owned(),
			timeout: Duration::from_secs(60),
			clients: args.clients,
			threads: args.threads,
			samples: args.samples,
		}
	}
	/// Run the benchmark for the desired benchmark engine
	pub(crate) async fn run<C, P>(&self, engine: P) -> Result<BenchmarkResult>
	where
		C: BenchmarkClient + Send + Sync,
		P: BenchmarkEngine<C> + Send + Sync,
	{
		// Setup the datastore
		self.wait_for_client(&engine).await?.startup().await?;
		// Setup the clients
		let clients = self.setup_clients(&engine).await?;
		// Run the "creates" benchmark
		let creates = self.run_operation::<C, P>(&clients, BenchmarkOperation::Create).await?;
		// Run the "reads" benchmark
		let reads = self.run_operation::<C, P>(&clients, BenchmarkOperation::Read).await?;
		// Run the "reads" benchmark
		let updates = self.run_operation::<C, P>(&clients, BenchmarkOperation::Update).await?;
		// Run the "deletes" benchmark
		let deletes = self.run_operation::<C, P>(&clients, BenchmarkOperation::Delete).await?;
		// Setup the datastore
		self.wait_for_client(&engine).await?.shutdown().await?;
		// Return the benchmark results
		Ok(BenchmarkResult {
			creates,
			reads,
			updates,
			deletes,
		})
	}

	async fn wait_for_client<C, P>(&self, engine: &P) -> Result<C>
	where
		C: BenchmarkClient + Send + Sync,
		P: BenchmarkEngine<C> + Send + Sync,
	{
		// Wait for a small amount of time
		tokio::time::sleep(TIMEOUT).await;
		// Get the current system time
		let time = SystemTime::now();
		// Check the elapsed time
		while time.elapsed()? < self.timeout {
			// Get the specified endpoint
			let endpoint = self.endpoint.to_owned();
			// Attempt to create a client connection
			if let Ok(v) = engine.create_client(endpoint).await {
				return Ok(v);
			}
			// Wait for a small amount of time
			tokio::time::sleep(TIMEOUT).await;
		}
		bail!("Can't create the client")
	}

	async fn setup_clients<C, P>(&self, engine: &P) -> Result<Vec<Arc<C>>>
	where
		C: BenchmarkClient + Send + Sync,
		P: BenchmarkEngine<C> + Send + Sync,
	{
		// Create a set of client connections
		let mut clients = Vec::with_capacity(self.clients as usize);
		// Create the desired number of connections
		for i in 0..self.clients {
			// Log some information
			info!("Creating client {}", i + 1);
			// Get the specified endpoint
			let endpoint = self.endpoint.to_owned();
			// Create a new client connection
			clients.push(engine.create_client(endpoint));
		}
		// Wait for all the clients to connect
		Ok(try_join_all(clients).await?.into_iter().map(Arc::new).collect())
	}

	async fn run_operation<C, P>(
		&self,
		clients: &[Arc<C>],
		operation: BenchmarkOperation,
	) -> Result<Duration>
	where
		C: BenchmarkClient + Send + Sync,
		P: BenchmarkEngine<C> + Send + Sync,
	{
		// Get the total concurrent futures
		let total = (self.clients * self.threads) as usize;
		// Whether we have experienced an error
		let error = Arc::new(AtomicBool::new(false));
		// The total records processed so far
		let current = Arc::new(AtomicI32::new(0));
		// Store the futures in a vector
		let mut futures = Vec::with_capacity(total);
		// Print out the first stage
		let mut out = TerminalOut::default();
		out.map(|| format!("\r{operation} 0%"))?;
		// Measure the starting time
		let time = Instant::now();
		// Loop over the clients
		for (client, c) in clients.iter().cloned().zip(1..) {
			// Loop over the threads
			for t in 0..self.threads {
				let error = error.clone();
				let current = current.clone();
				let client = client.clone();
				let out = out.clone();
				let samples = self.samples;
				futures.push(task::spawn(async move {
					info!("Task #{c}/{t}/{operation} starting");
					if let Err(e) =
						Self::operation_loop(client, samples, &error, &current, operation, out)
							.await
					{
						error!("{e}");
						error.store(true, Ordering::Relaxed);
					}
					info!("Task #{c}/{t}/{operation} finished");
				}));
			}
		}
		// Wait for all of the threads to complete
		if let Err(e) = try_join_all(futures).await {
			error.store(true, Ordering::Relaxed);
			error!("{e}");
		}
		// Calculate the elapsed time
		let elapsed = time.elapsed();
		// Print out the last stage
		out.map_ln(|| format!("\r{operation} 100%"))?;
		// Everything ok
		Ok(elapsed)
	}

	async fn operation_loop<C>(
		client: Arc<C>,
		samples: i32,
		error: &AtomicBool,
		current: &AtomicI32,
		operation: BenchmarkOperation,
		mut out: TerminalOut,
	) -> Result<()>
	where
		C: BenchmarkClient,
	{
		// Check if we have encountered an error
		while !error.load(Ordering::Relaxed) {
			let sample = current.fetch_add(1, Ordering::Relaxed);
			if sample >= samples {
				// We are done
				break;
			}
			// Calculate the completion percent
			out.map(|| {
				let percent = if sample == 0 {
					0u8
				} else {
					(sample * 20 / samples) as u8
				};
				format!("\r{operation} {}%", percent * 5)
			})?;
			// Perform the benchmark operation
			match operation {
				BenchmarkOperation::Read => {
					client.read(sample).await?;
				}
				BenchmarkOperation::Create => {
					let mut provider = RecordProvider::default();
					let record = provider.sample();
					client.create(sample, record).await?;
				}
				BenchmarkOperation::Update => {
					let mut provider = RecordProvider::default();
					let record = provider.sample();
					client.update(sample, record).await?;
				}
				BenchmarkOperation::Delete => {
					client.delete(sample).await?;
				}
			}
		}
		Ok(())
	}
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BenchmarkOperation {
	Create,
	Read,
	Update,
	Delete,
}

impl Display for BenchmarkOperation {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Create => write!(f, "Create"),
			Self::Read => write!(f, "Read"),
			Self::Update => write!(f, "Update"),
			Self::Delete => write!(f, "Delete"),
		}
	}
}

pub(crate) struct BenchmarkResult {
	creates: Duration,
	reads: Duration,
	updates: Duration,
	deletes: Duration,
}

impl Display for BenchmarkResult {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		writeln!(f, "[C]reates: {:?}", self.creates)?;
		writeln!(f, "[R]eads: {:?}", self.reads)?;
		writeln!(f, "[U]pdates: {:?}", self.updates)?;
		write!(f, "[D]eletes: {:?}", self.deletes)
	}
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub(crate) struct Record {
	pub(crate) text: String,
	pub(crate) integer: i32,
}

struct RecordProvider {
	rng: SmallRng,
	record: Record,
}

impl Default for RecordProvider {
	fn default() -> Self {
		Self {
			rng: SmallRng::from_entropy(),
			record: Default::default(),
		}
	}
}

const CHARSET: &[u8; 37] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789";

impl RecordProvider {
	fn sample(&mut self) -> &Record {
		self.record.text = (0..50)
			.map(|_| {
				let idx = self.rng.gen_range(0..CHARSET.len());
				CHARSET[idx] as char
			})
			.collect();
		&self.record
	}
}

pub(crate) trait BenchmarkEngine<C>: Send + Sync
where
	C: BenchmarkClient,
{
	fn create_client(&self, endpoint: Option<String>) -> impl Future<Output = Result<C>> + Send;
}

pub(crate) trait BenchmarkClient: Send + Sync + 'static {
	/// Initialise the store at startup
	async fn startup(&self) -> Result<()> {
		Ok(())
	}
	/// Cleanup the store at shutdown
	fn shutdown(&self) -> impl Future<Output = Result<()>> + Send {
		async { Ok(()) }
	}
	/// Create a record at a key
	fn create(&self, key: i32, record: &Record) -> impl Future<Output = Result<()>> + Send;
	/// Read a record at a key
	fn read(&self, key: i32) -> impl Future<Output = Result<()>> + Send;
	/// Update a record at a key
	fn update(&self, key: i32, record: &Record) -> impl Future<Output = Result<()>> + Send;
	/// Delete a record at a key
	fn delete(&self, key: i32) -> impl Future<Output = Result<()>> + Send;
}

pub(crate) struct TerminalOut(Option<Stdout>);

impl Default for TerminalOut {
	fn default() -> Self {
		let stdout = stdout();
		if stdout.is_terminal() {
			Self(Some(stdout))
		} else {
			Self(None)
		}
	}
}

impl Clone for TerminalOut {
	fn clone(&self) -> Self {
		Self(self.0.as_ref().map(|_| stdout()))
	}
}
impl TerminalOut {
	pub(crate) fn map_ln<F, S>(&mut self, f: F) -> Result<()>
	where
		F: Fn() -> S,
		S: Display,
	{
		if let Some(ref mut o) = self.0 {
			writeln!(o, "{}", f())?;
			o.flush()?;
		}
		Ok(())
	}

	pub(crate) fn map<F, S>(&mut self, f: F) -> Result<()>
	where
		F: Fn() -> S,
		S: Display,
	{
		if let Some(ref mut o) = self.0 {
			write!(o, "{}", f())?;
			o.flush()?;
		}
		Ok(())
	}
}
