use std::fmt::{Display, Formatter};
use std::io;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{bail, Result};
use log::{error, info, warn};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rayon::scope;
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};
use tokio::time::sleep;

use crate::Args;

pub(crate) struct Benchmark {
	threads: usize,
	samples: i32,
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
		}
	}

	pub async fn wait_for_client<C, P>(client_provider: &P, time_out: Duration) -> Result<C>
	where
		C: BenchmarkClient + Send,
		P: BenchmarkClientProvider<C> + Send + Sync,
	{
		sleep(Duration::from_secs(2)).await;
		let start = SystemTime::now();
		while start.elapsed()? < time_out {
			sleep(Duration::from_secs(2)).await;
			info!("Create client connection");
			if let Ok(client) = client_provider.create_client().await {
				return Ok(client);
			}
			warn!("DB not yet responding");
		}
		bail!("Can't create the client")
	}

	pub(crate) fn run<C, P>(&self, client_provider: P) -> Result<BenchmarkResult>
	where
		C: BenchmarkClient + Send,
		P: BenchmarkClientProvider<C> + Send + Sync,
	{
		{
			// Prepare
			let runtime = Runtime::new().expect("Failed to create a runtime");
			runtime.block_on(async {
				let mut client =
					Self::wait_for_client(&client_provider, Duration::from_secs(60)).await?;
				client.startup().await?;
				Ok::<(), anyhow::Error>(())
			})?;
		}

		// Run the "creates" benchmark
		info!("Start creates benchmark");
		let creates = self.run_operation(&client_provider, BenchmarkOperation::Create)?;
		info!("Creates benchmark done");

		// Run the "reads" benchmark
		info!("Start reads benchmark");
		let reads = self.run_operation(&client_provider, BenchmarkOperation::Read)?;
		info!("Reads benchmark done");

		// Run the "reads" benchmark
		info!("Start updates benchmark");
		let updates = self.run_operation(&client_provider, BenchmarkOperation::Update)?;
		info!("Reads benchmark done");

		// Run the "deletes" benchmark
		info!("Start deletes benchmark");
		let deletes = self.run_operation(&client_provider, BenchmarkOperation::Delete)?;
		info!("Deletes benchmark done");

		Ok(BenchmarkResult {
			creates,
			reads,
			updates,
			deletes,
		})
	}

	fn run_operation<C, P>(
		&self,
		client_provider: &P,
		operation: BenchmarkOperation,
	) -> Result<Duration>
	where
		C: BenchmarkClient + Send,
		P: BenchmarkClientProvider<C> + Send + Sync,
	{
		let error = Arc::new(AtomicBool::new(false));
		let time = Instant::now();
		let percent = Arc::new(AtomicU8::new(0));
		print!("\r{operation:?} 0%");
		scope(|s| {
			let current = Arc::new(AtomicI32::new(0));
			for thread_number in 0..self.threads {
				let current = current.clone();
				let error = error.clone();
				let percent = percent.clone();
				s.spawn(move |_| {
					let mut record_provider = RecordProvider::default();
					let runtime = Builder::new_multi_thread()
						.worker_threads(4) // Set the number of worker threads
						.enable_all() // Enables all runtime features, including I/O and time
						.build()
						.expect("Failed to create a runtime");
					if let Err(e) = runtime.block_on(async {
						info!("Thread #{thread_number}/{operation:?} starts");
						let mut client = client_provider.create_client().await?;
						while !error.load(Ordering::Relaxed) {
							let sample = current.fetch_add(1, Ordering::Relaxed);
							if sample >= self.samples {
								break;
							}
							// Calculate and print out the percents
							{
								let new_percent = if sample == 0 {
									0u8
								} else {
									(sample * 20 / self.samples) as u8
								};
								let old_percent = percent.load(Ordering::Relaxed);
								if new_percent > old_percent {
									percent.store(new_percent, Ordering::Relaxed);
									print!("\r{operation:?} {}%", new_percent * 5);
									io::stdout().flush()?;
								}
							}
							match operation {
								BenchmarkOperation::Create => {
									let record = record_provider.sample();
									client.create(sample, record).await?;
								}
								BenchmarkOperation::Read => client.read(sample).await?,
								BenchmarkOperation::Update => {
									let record = record_provider.sample();
									client.update(sample, record).await?;
								}
								BenchmarkOperation::Delete => client.delete(sample).await?,
							}
						}
						client.shutdown().await?;
						info!("Thread #{thread_number}/{operation:?} ends");
						Ok::<(), anyhow::Error>(())
					}) {
						error!("{}", e);
						error.store(true, Ordering::Relaxed);
					}
				});
			}
		});
		println!("\r{operation:?} 100%");
		io::stdout().flush()?;
		if error.load(Ordering::Relaxed) {
			bail!("Benchmark error");
		}
		Ok(time.elapsed())
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

pub(crate) trait BenchmarkClientProvider<C>
where
	C: BenchmarkClient,
{
	async fn create_client(&self) -> Result<C>;
}

pub(crate) trait BenchmarkClient {
	/// Initialise the store at startup
	async fn startup(&mut self) -> Result<()> {
		Ok(())
	}
	/// Cleanup the store at shutdown
	async fn shutdown(&mut self) -> Result<()> {
		Ok(())
	}
	/// Create a record at a key
	async fn create(&mut self, key: i32, record: &Record) -> Result<()>;
	/// Read a record at a key
	async fn read(&mut self, key: i32) -> Result<()>;
	/// Update a record at a key
	async fn update(&mut self, key: i32, record: &Record) -> Result<()>;
	/// Delete a record at a key
	async fn delete(&mut self, key: i32) -> Result<()>;
}
