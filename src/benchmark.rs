use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::Args;
use anyhow::{bail, Result};
use log::{error, info, warn};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::task;
use tokio::time::sleep;

pub(crate) struct Benchmark {
	threads: usize,
	pool: usize,
	samples: i32,
	timeout: Duration,
	endpoint: Option<String>,
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
		writeln!(f, "[D]eletes: {:?}", self.deletes)
	}
}

impl Benchmark {
	pub(crate) fn new(args: &Args) -> Self {
		Self {
			threads: args.threads,
			samples: args.samples,
			timeout: Duration::from_secs(60),
			endpoint: args.endpoint.to_owned(),
			pool: args.pool.unwrap_or(1),
		}
	}

	pub(crate) fn pool(&self) -> usize {
		self.pool
	}

	pub(crate) async fn wait_for_client<C, P>(&self, engine: &P) -> Result<C>
	where
		C: BenchmarkClient,
		P: BenchmarkEngine<C>,
	{
		sleep(Duration::from_secs(2)).await;
		let start = SystemTime::now();
		while start.elapsed()? < self.timeout {
			info!("Create client connection");
			if let Ok(client) = engine.create_client(self.endpoint.to_owned()).await {
				return Ok(client);
			}
			warn!("DB not yet responding");
			sleep(Duration::from_secs(5)).await;
		}
		bail!("Can't create the client")
	}

	pub(crate) async fn run<C, P>(&self, engine: P) -> Result<BenchmarkResult>
	where
		C: BenchmarkClient,
		P: BenchmarkEngine<C>,
	{
		// Start the client
		self.wait_for_client(&engine).await?.startup().await?;

		// Run the "creates" benchmark
		info!("Start creates benchmark");
		let creates = self.run_operation(&engine, BenchmarkOperation::Create).await?;
		info!("Creates benchmark done");

		// Run the "reads" benchmark
		info!("Start reads benchmark");
		let reads = self.run_operation(&engine, BenchmarkOperation::Read).await?;
		info!("Reads benchmark done");

		// Run the "reads" benchmark
		info!("Start updates benchmark");
		let updates = self.run_operation(&engine, BenchmarkOperation::Update).await?;
		info!("Reads benchmark done");

		// Run the "deletes" benchmark
		info!("Start deletes benchmark");
		let deletes = self.run_operation(&engine, BenchmarkOperation::Delete).await?;
		info!("Deletes benchmark done");

		Ok(BenchmarkResult {
			creates,
			reads,
			updates,
			deletes,
		})
	}

	async fn run_operation<C, P>(
		&self,
		engine: &P,
		operation: BenchmarkOperation,
	) -> Result<Duration>
	where
		C: BenchmarkClient + Send + Sync,
		P: BenchmarkEngine<C> + Send + Sync,
	{
		let error = Arc::new(AtomicBool::new(false));
		let time = Instant::now();
		let percent = Arc::new(AtomicU8::new(0));
		print!("\r{operation:?} 0%");

		let current = Arc::new(AtomicI32::new(0));

		let mut futures = Vec::with_capacity(self.threads);

		// start the threads
		for thread_number in 0..self.threads {
			let client = Arc::new(engine.create_client(self.endpoint.clone()).await?);
			for _ in 0..self.pool {
				let current = current.clone();
				let error = error.clone();
				let percent = percent.clone();
				let samples = self.samples;
				let client = client.clone();
				let f = task::spawn(async move {
					info!("Thread #{thread_number}/{operation:?} starts");
					if let Err(e) =
						Self::operation_loop(client, samples, &error, &current, &percent, operation)
							.await
					{
						error!("{e}");
						error.store(true, Ordering::Relaxed);
					}
					info!("Thread #{thread_number}/{operation:?} ends");
				});
				futures.push(f);
			}
		}

		// Wait for threads to be done
		for f in futures {
			if let Err(e) = f.await {
				{
					error!("{e}");
					error.store(true, Ordering::Relaxed);
				}
			}
		}

		if error.load(Ordering::Relaxed) {
			bail!("Benchmark error");
		}
		println!("\r{operation:?} 100%");
		io::stdout().flush()?;
		Ok(time.elapsed())
	}

	async fn operation_loop<C>(
		client: Arc<C>,
		samples: i32,
		error: &AtomicBool,
		current: &AtomicI32,
		percent: &AtomicU8,
		operation: BenchmarkOperation,
	) -> Result<()>
	where
		C: BenchmarkClient,
	{
		let mut record_provider = RecordProvider::default();
		while !error.load(Ordering::Relaxed) {
			let sample = current.fetch_add(1, Ordering::Relaxed);
			if sample >= samples {
				// We are done
				break;
			}
			// Calculate and print out the percents
			{
				let new_percent = if sample == 0 {
					0u8
				} else {
					(sample * 20 / samples) as u8
				};
				let old_percent = percent.load(Ordering::Relaxed);
				if new_percent > old_percent {
					percent.store(new_percent, Ordering::Relaxed);
					print!("\r{operation:?} {}%", new_percent * 5);
					io::stdout().flush()?;
				}
			}
			match operation {
				BenchmarkOperation::Read => client.read(sample).await?,
				BenchmarkOperation::Create => {
					let record = record_provider.sample();
					client.create(sample, record).await?;
				}
				BenchmarkOperation::Update => {
					let record = record_provider.sample();
					client.update(sample, record).await?;
				}
				BenchmarkOperation::Delete => client.delete(sample).await?,
			}
		}
		client.shutdown().await?;
		Ok::<(), anyhow::Error>(())
	}
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BenchmarkOperation {
	Create,
	Read,
	Update,
	Delete,
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
