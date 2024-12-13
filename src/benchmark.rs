use crate::dialect::Dialect;
use crate::keyprovider::{IntegerKeyProvider, KeyProvider, StringKeyProvider};
use crate::result::{OperationMetric, OperationResult};
use crate::valueprovider::{Columns, ValueProvider};
use crate::{Args, KeyType, Scan, Scans};
use anyhow::{bail, Result};
use futures::future::try_join_all;
use log::info;
use serde_json::Value;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io::Write;
use std::io::{stdout, IsTerminal, Stdout};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::task;

const TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) const NOT_SUPPORTED_ERROR: &str = "NotSupported";

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
	samples: u32,
	/// Pid to monitor
	pid: Option<u32>,
}
impl Benchmark {
	pub(crate) fn new(args: &Args) -> Self {
		Self {
			endpoint: args.endpoint.to_owned(),
			timeout: Duration::from_secs(60),
			clients: args.clients,
			threads: args.threads,
			samples: args.samples,
			pid: args.pid,
		}
	}
	/// Run the benchmark for the desired benchmark engine
	pub(crate) async fn run<C, D, E>(
		&self,
		engine: E,
		kp: KeyProvider,
		mut vp: ValueProvider,
		scans: Scans,
	) -> Result<BenchmarkResult>
	where
		C: BenchmarkClient + Send + Sync,
		D: Dialect,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		// Generate a value sample for the report
		let sample = vp.generate_value::<D>();
		// Setup the datastore
		self.wait_for_client(&engine).await?.startup().await?;
		// Setup the clients
		let clients = self.setup_clients(&engine).await?;
		// Run the "creates" benchmark
		let creates = self
			.run_operation::<C, D>(
				&clients,
				BenchmarkOperation::Create,
				kp,
				vp.clone(),
				self.samples,
			)
			.await?;
		// Run the "reads" benchmark
		let reads = self
			.run_operation::<C, D>(&clients, BenchmarkOperation::Read, kp, vp.clone(), self.samples)
			.await?;
		// Run the "reads" benchmark
		let updates = self
			.run_operation::<C, D>(
				&clients,
				BenchmarkOperation::Update,
				kp,
				vp.clone(),
				self.samples,
			)
			.await?;
		// Run the "scan" benchmarks
		let mut scan_results = Vec::with_capacity(scans.len());
		for scan in scans {
			let name = scan.name.clone();
			let samples = scan.samples.map(|s| s as u32).unwrap_or(self.samples);
			let duration = self
				.run_operation::<C, D>(
					&clients,
					BenchmarkOperation::Scan(scan),
					kp,
					vp.clone(),
					samples,
				)
				.await?;
			scan_results.push((name, duration));
		}
		// Run the "deletes" benchmark
		let deletes = self
			.run_operation::<C, D>(&clients, BenchmarkOperation::Delete, kp, vp, self.samples)
			.await?;
		// Setup the datastore
		self.wait_for_client(&engine).await?.shutdown().await?;
		// Return the benchmark results
		Ok(BenchmarkResult {
			creates,
			reads,
			updates,
			scans: scan_results,
			deletes,
			sample,
		})
	}

	async fn wait_for_client<C, E>(&self, engine: &E) -> Result<C>
	where
		C: BenchmarkClient + Send + Sync,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		// Get the current system time
		let time = SystemTime::now();
		// Check the elapsed time
		while time.elapsed()? < self.timeout {
			// Wait for a small amount of time
			tokio::time::sleep(TIMEOUT).await;
			// Get the specified endpoint
			let endpoint = self.endpoint.to_owned();
			// Attempt to create a client connection
			if let Ok(v) = engine.create_client(endpoint).await {
				return Ok(v);
			}
		}
		bail!("Can't create the client")
	}

	async fn setup_clients<C, E>(&self, engine: &E) -> Result<Vec<Arc<C>>>
	where
		C: BenchmarkClient + Send + Sync,
		E: BenchmarkEngine<C> + Send + Sync,
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

	async fn run_operation<C, D>(
		&self,
		clients: &[Arc<C>],
		operation: BenchmarkOperation,
		kp: KeyProvider,
		vp: ValueProvider,
		samples: u32,
	) -> Result<Option<OperationResult>>
	where
		C: BenchmarkClient + Send + Sync,
		D: Dialect,
	{
		// Get the total concurrent futures
		let total = (self.clients * self.threads) as usize;
		// Whether we have experienced an error
		let error = Arc::new(AtomicBool::new(false));
		// Wether the test should be skipped
		let skip = Arc::new(AtomicBool::new(false));
		// The total records processed so far
		let current = Arc::new(AtomicU32::new(0));
		// Store the futures in a vector
		let mut futures = Vec::with_capacity(total);
		// Print out the first stage
		let mut out = TerminalOut::default();
		out.map(|| Some(format!("\r{operation} 0%")))?;
		// Measure the starting time
		let metric = OperationMetric::new(self.pid);
		// Loop over the clients
		for (client, _) in clients.iter().cloned().zip(1..) {
			// Loop over the threads
			for _ in 0..self.threads {
				let error = error.clone();
				let skip = skip.clone();
				let current = current.clone();
				let client = client.clone();
				let out = out.clone();
				let vp = vp.clone();
				let operation = operation.clone();
				futures.push(task::spawn(async move {
					match Self::operation_loop::<C, D>(
						client,
						samples,
						&error,
						&current,
						operation,
						(kp, vp, out),
					)
					.await
					{
						Err(e) if e.to_string().eq(NOT_SUPPORTED_ERROR) => {
							skip.store(true, Ordering::Relaxed);
							Ok(())
						}
						Err(e) => {
							eprintln!("{e}");
							error.store(true, Ordering::Relaxed);
							Err(e)
						}
						Ok(_) => Ok(()),
					}
				}));
			}
		}
		// Wait for all the threads to complete
		if let Err(e) = try_join_all(futures).await {
			error.store(true, Ordering::Relaxed);
			Err(e)?;
		}
		if error.load(Ordering::Relaxed) {
			bail!("Task failure");
		}
		// Calculate runtime information
		let result = OperationResult::new(metric);
		// Print out the last stage
		out.map_ln(|| Some(format!("\r{operation} 100%")))?;
		// Shall we skip the operation? (operation not supported)
		if skip.load(Ordering::Relaxed) {
			return Ok(None);
		}
		// Everything ok
		Ok(Some(result))
	}

	async fn operation_loop<C, D>(
		client: Arc<C>,
		samples: u32,
		error: &AtomicBool,
		current: &AtomicU32,
		operation: BenchmarkOperation,
		(mut kp, mut vp, mut out): (KeyProvider, ValueProvider, TerminalOut),
	) -> Result<()>
	where
		C: BenchmarkClient,
		D: Dialect,
	{
		let mut old_percent = 0;
		// Check if we have encountered an error
		while !error.load(Ordering::Relaxed) {
			let sample = current.fetch_add(1, Ordering::Relaxed);
			if sample >= samples {
				// We are done
				break;
			}
			// Calculate the completion percent
			out.map(|| {
				let new_percent = if sample == 0 {
					0u8
				} else {
					(sample * 20 / samples) as u8
				};
				if new_percent != old_percent {
					old_percent = new_percent;
					Some(format!("\r{operation} {}%", new_percent * 5))
				} else {
					None
				}
			})?;
			// Perform the benchmark operation
			match &operation {
				BenchmarkOperation::Create => {
					let value = vp.generate_value::<D>();
					client.create(sample, value, &mut kp).await?
				}
				BenchmarkOperation::Read => client.read(sample, &mut kp).await?,
				BenchmarkOperation::Update => {
					let value = vp.generate_value::<D>();
					client.update(sample, value, &mut kp).await?
				}
				BenchmarkOperation::Scan(s) => client.scan(s, &kp).await?,
				BenchmarkOperation::Delete => client.delete(sample, &mut kp).await?,
			};
		}

		Ok(())
	}
}

#[derive(Clone, Debug)]
pub(crate) enum BenchmarkOperation {
	Create,
	Read,
	Update,
	Scan(Scan),
	Delete,
}

impl Display for BenchmarkOperation {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Create => write!(f, "Create"),
			Self::Read => write!(f, "Read"),
			Self::Scan(s) => write!(f, "Scan::{}", s.name),
			Self::Update => write!(f, "Update"),
			Self::Delete => write!(f, "Delete"),
		}
	}
}

pub(crate) struct BenchmarkResult {
	creates: Option<OperationResult>,
	reads: Option<OperationResult>,
	updates: Option<OperationResult>,
	scans: Vec<(String, Option<OperationResult>)>,
	deletes: Option<OperationResult>,
	pub(super) sample: Value,
}

impl Display for BenchmarkResult {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		if let Some(r) = &self.creates {
			writeln!(f, "[C]reates: {}", r)?;
		}
		if let Some(r) = &self.reads {
			writeln!(f, "[R]eads: {}", r)?;
		}
		if let Some(r) = &self.updates {
			writeln!(f, "[U]pdates: {}", r)?;
		}
		for (name, result) in &self.scans {
			if let Some(r) = &result {
				writeln!(f, "[S]can::{name}: {r}")?;
			} else {
				writeln!(f, "[S]can::{name}: <Skipped - Not supported>")?;
			}
		}
		if let Some(r) = &self.deletes {
			writeln!(f, "[D]eletes: {}", r)?;
		}
		Ok(())
	}
}

pub(crate) trait BenchmarkEngine<C>: Sized
where
	C: BenchmarkClient + Send,
{
	async fn setup(kt: KeyType, columns: Columns) -> Result<Self>;
	async fn create_client(&self, endpoint: Option<String>) -> Result<C>;
}

pub(crate) trait BenchmarkClient: Sync + Send + 'static {
	/// Initialise the store at startup
	async fn startup(&self) -> Result<()> {
		Ok(())
	}
	/// Cleanup the store at shutdown
	async fn shutdown(&self) -> Result<()> {
		Ok(())
	}
	fn create(
		&self,
		n: u32,
		val: Value,
		kp: &mut KeyProvider,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => self.create_u32(p.key(n), val).await,
				KeyProvider::UnorderedInteger(p) => self.create_u32(p.key(n), val).await,
				KeyProvider::OrderedString(p) => self.create_string(p.key(n), val).await,
				KeyProvider::UnorderedString(p) => self.create_string(p.key(n), val).await,
			}
		}
	}

	fn read(&self, n: u32, kp: &mut KeyProvider) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => self.read_u32(p.key(n)).await,
				KeyProvider::UnorderedInteger(p) => self.read_u32(p.key(n)).await,
				KeyProvider::OrderedString(p) => self.read_string(p.key(n)).await,
				KeyProvider::UnorderedString(p) => self.read_string(p.key(n)).await,
			}
		}
	}
	fn update(
		&self,
		n: u32,
		val: Value,
		kp: &mut KeyProvider,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => self.update_u32(p.key(n), val).await,
				KeyProvider::UnorderedInteger(p) => self.update_u32(p.key(n), val).await,
				KeyProvider::OrderedString(p) => self.update_string(p.key(n), val).await,
				KeyProvider::UnorderedString(p) => self.update_string(p.key(n), val).await,
			}
		}
	}

	fn delete(&self, n: u32, kp: &mut KeyProvider) -> impl Future<Output = Result<()>> + Send {
		async move {
			match kp {
				KeyProvider::OrderedInteger(p) => self.delete_u32(p.key(n)).await,
				KeyProvider::UnorderedInteger(p) => self.delete_u32(p.key(n)).await,
				KeyProvider::OrderedString(p) => self.delete_string(p.key(n)).await,
				KeyProvider::UnorderedString(p) => self.delete_string(p.key(n)).await,
			}
		}
	}

	fn scan(&self, scan: &Scan, kp: &KeyProvider) -> impl Future<Output = Result<()>> + Send {
		async move {
			let result = match kp {
				KeyProvider::OrderedInteger(_) | KeyProvider::UnorderedInteger(_) => {
					self.scan_u32(scan).await?
				}
				KeyProvider::OrderedString(_) | KeyProvider::UnorderedString(_) => {
					self.scan_string(scan).await?
				}
			};
			if let Some(expect) = scan.expect {
				assert_eq!(expect, result);
			}
			Ok(())
		}
	}

	fn scan_u32(&self, _scan: &Scan) -> impl Future<Output = Result<usize>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	fn scan_string(&self, _scan: &Scan) -> impl Future<Output = Result<usize>> + Send {
		async move { bail!(NOT_SUPPORTED_ERROR) }
	}

	/// Create a record at a key
	fn create_u32(&self, key: u32, val: Value) -> impl Future<Output = Result<()>> + Send;

	fn create_string(&self, key: String, val: Value) -> impl Future<Output = Result<()>> + Send;

	/// Read a record at a key
	fn read_u32(&self, key: u32) -> impl Future<Output = Result<()>> + Send;

	fn read_string(&self, key: String) -> impl Future<Output = Result<()>> + Send;

	/// Update a record at a key
	fn update_u32(&self, key: u32, val: Value) -> impl Future<Output = Result<()>> + Send;

	fn update_string(&self, key: String, val: Value) -> impl Future<Output = Result<()>> + Send;

	/// Delete a record at a key
	fn delete_u32(&self, key: u32) -> impl Future<Output = Result<()>> + Send;

	fn delete_string(&self, key: String) -> impl Future<Output = Result<()>> + Send;
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
	pub(crate) fn map_ln<F, S>(&mut self, mut f: F) -> Result<()>
	where
		F: FnMut() -> Option<S>,
		S: Display,
	{
		if let Some(ref mut o) = self.0 {
			if let Some(s) = f() {
				writeln!(o, "{}", s)?;
				o.flush()?;
			}
		}
		Ok(())
	}

	pub(crate) fn map<F, S>(&mut self, mut f: F) -> Result<()>
	where
		F: FnMut() -> Option<S>,
		S: Display,
	{
		if let Some(ref mut o) = self.0 {
			if let Some(s) = f() {
				write!(o, "{}", s)?;
				o.flush()?;
			}
		}
		Ok(())
	}
}
