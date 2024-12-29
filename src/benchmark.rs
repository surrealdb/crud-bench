use crate::dialect::Dialect;
use crate::engine::{BenchmarkClient, BenchmarkEngine};
use crate::keyprovider::KeyProvider;
use crate::result::{BenchmarkResult, OperationMetric, OperationResult};
use crate::terminal::Terminal;
use crate::valueprovider::ValueProvider;
use crate::{Args, Scan, Scans};
use anyhow::{bail, Result};
use futures::future::try_join_all;
use hdrhistogram::Histogram;
use log::info;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::task;
use tokio::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) const NOT_SUPPORTED_ERROR: &str = "NotSupported";

pub(crate) struct Benchmark {
	/// The server endpoint to connect to
	pub(crate) endpoint: Option<String>,
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
		// Compact the datastore
		if std::env::var("COMPACTION").is_ok() {
			self.wait_for_client(&engine).await?.compact().await?;
		}
		// Run the "reads" benchmark
		let reads = self
			.run_operation::<C, D>(&clients, BenchmarkOperation::Read, kp, vp.clone(), self.samples)
			.await?;
		// Compact the datastore
		if std::env::var("COMPACTION").is_ok() {
			self.wait_for_client(&engine).await?.compact().await?;
		}
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
		// Compact the datastore
		if std::env::var("COMPACTION").is_ok() {
			self.wait_for_client(&engine).await?.compact().await?;
		}
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
			scan_results.push((name, samples, duration));
		}
		// Compact the datastore
		if std::env::var("COMPACTION").is_ok() {
			self.wait_for_client(&engine).await?.compact().await?;
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
			// Attempt to create a client connection
			if let Ok(v) = engine.create_client().await {
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
			// Create a new client connection
			clients.push(engine.create_client());
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
		// Create a new terminal display
		let mut out = Terminal::default();
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
		out.write(|| Some(format!("\r{operation} 0%")))?;
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
							Ok(None)
						}
						Err(e) => {
							eprintln!("{e}");
							error.store(true, Ordering::Relaxed);
							Err(e)
						}
						Ok(h) => Ok(Some(h)),
					}
				}));
			}
		}
		// Wait for all the threads to complete
		let mut global_histogram = Histogram::new(3)?;
		match try_join_all(futures).await {
			Ok(results) => {
				for res in results {
					if let Some(histogram) = res? {
						global_histogram.add(histogram)?;
					}
				}
			}
			Err(e) => {
				error.store(true, Ordering::Relaxed);
				Err(e)?;
			}
		};
		if error.load(Ordering::Relaxed) {
			bail!("Task failure");
		}
		// Calculate runtime information
		let result = OperationResult::new(metric, global_histogram);
		// Print out the last stage
		out.writeln(|| Some(format!("\r{operation} 100%")))?;
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
		(mut kp, mut vp, mut out): (KeyProvider, ValueProvider, Terminal),
	) -> Result<Histogram<u64>>
	where
		C: BenchmarkClient,
		D: Dialect,
	{
		let mut histogram = Histogram::new(3)?;
		let mut old_percent = 0;
		// Check if we have encountered an error
		while !error.load(Ordering::Relaxed) {
			let sample = current.fetch_add(1, Ordering::Relaxed);
			if sample >= samples {
				// We are done
				break;
			}
			// Perform the benchmark operation
			let time = Instant::now();
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
			// Output the percentage completion
			out.write(|| {
				// Calculate the percentage completion
				let new_percent = match sample {
					0 => 0u8,
					_ => (sample * 20 / samples) as u8,
				};
				// Display the percent if multiple of 5
				if new_percent != old_percent {
					old_percent = new_percent;
					Some(format!("\r{operation} {}%", new_percent * 5))
				} else {
					None
				}
			})?;
			//
			histogram.record(time.elapsed().as_micros() as u64)?;
		}
		Ok(histogram)
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
