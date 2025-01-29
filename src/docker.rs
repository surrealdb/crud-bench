use crate::benchmark::Benchmark;
use log::{debug, error, info};
use std::fmt;
use std::process::{exit, Command};
use std::time::Duration;

const RETRIES: i32 = 10;

const TIMEOUT: Duration = Duration::from_secs(6);

pub(crate) struct DockerParams {
	pub(crate) image: &'static str,
	pub(crate) pre_args: &'static str,
	pub(crate) post_args: &'static str,
}

pub(crate) struct Container {
	image: String,
}

impl Drop for Container {
	fn drop(&mut self) {
		let _ = Self::stop();
	}
}

impl Container {
	/// Get the name of the Docker image
	pub(crate) fn image(&self) -> &str {
		&self.image
	}

	/// Start the Docker container
	pub(crate) fn start(image: String, pre: &str, post: &str, options: &Benchmark) -> Self {
		// Output debug information to the logs
		info!("Starting Docker image '{image}'");
		// Attempt to start Docker 10 times
		for i in 1..=RETRIES {
			// Configure the Docker command arguments
			let mut arguments = Arguments::new(["run"]);
			// Configure the default pre arguments
			arguments.append(pre);
			// Configure any custom pre arguments
			if let Ok(v) = std::env::var("DOCKER_PRE_ARGS") {
				arguments.append(&v);
			}
			// Configure container options
			match options.sync {
				true => {
					if image.as_str() == "surrealdb/surrealdb:nightly" {
						arguments.append("-e SURREAL_ROCKSDB_SYNC_DATA=true");
						arguments.append("-e SURREAL_SURREALKV_SYNC_DATA=true");
					}
				}
				false => {
					if image.as_str() == "surrealdb/surrealdb:nightly" {
						arguments.append("-e SURREAL_ROCKSDB_SYNC_DATA=false");
						arguments.append("-e SURREAL_SURREALKV_SYNC_DATA=false");
					}
				}
			}
			// Run in privileged mode if specified
			if options.privileged {
				arguments.add(["--privileged"]);
			}
			// Configure the Docker container options
			arguments.add(["--rm"]);
			arguments.add(["--quiet"]);
			arguments.add(["--pull", "always"]);
			arguments.add(["--name", "crud-bench"]);
			arguments.add(["--net", "host"]);
			arguments.add(["-d", &image]);
			// Configure the default post arguments
			arguments.append(post);
			// Configure container options
			match options.sync {
				true => {
					if image.as_str() == "postgres" {
						arguments.append("-c fsync=on")
					}
				}
				false => {
					if image.as_str() == "postgres" {
						arguments.append("-c fsync=off")
					}
					if image.as_str() == "mysql" {
						arguments.append("--innodb-flush-log-at-trx-commit=0");
					}
				}
			}
			// Configure any custom post arguments
			if let Ok(v) = std::env::var("DOCKER_POST_ARGS") {
				arguments.append(&v);
			}
			// Execute the Docker run command
			match Self::execute(arguments.clone()) {
				// The command executed successfully
				Ok(_) => break,
				// There was an error with the command
				Err(e) => match i {
					// This is the last attempt so exit fully
					RETRIES => {
						error!("Docker command failure: `docker {arguments}`");
						error!("{e}");
						exit(1);
					}
					// Let's log the output and retry the command
					_ => {
						debug!("Docker command failure: `docker {arguments}`");
						debug!("{e}");
						std::thread::sleep(TIMEOUT);
					}
				},
			}
		}
		// Return the container name
		Self {
			image,
		}
	}

	/// Stop the Docker container
	pub(crate) fn stop() -> Result<String, String> {
		info!("Stopping Docker container 'crud-bench'");
		let args = ["container", "stop", "--time", "300", "crud-bench"];
		Self::execute(Arguments::new(args))
	}

	/// Output the container logs
	pub(crate) fn logs() -> Result<String, String> {
		info!("Logging Docker container 'crud-bench'");
		let args = ["container", "logs", "crud-bench"];
		Self::execute(Arguments::new(args))
	}

	fn execute(args: Arguments) -> Result<String, String> {
		// Output debug information to the logs
		info!("Running command `docker {args}`");
		// Create a new process command
		let mut command = Command::new("docker");
		// Set the arguments on the command
		let command = command.args(args.0.clone());
		// Catch all output from the command
		let output = command.output().expect("Failed to execute process");
		// Output command failure if errored
		match output.status.success() {
			// Get the stderr out from the command
			false => Err(String::from_utf8(output.stderr).unwrap().trim().to_string()),
			// Get the stdout out from the command
			true => Ok(String::from_utf8(output.stdout).unwrap().trim().to_string()),
		}
	}
}

#[derive(Clone)]
pub(crate) struct Arguments(Vec<String>);

impl fmt::Display for Arguments {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.join(" "))
	}
}

impl Arguments {
	fn new<I, S>(args: I) -> Self
	where
		I: IntoIterator<Item = S>,
		S: Into<String>,
	{
		let mut a = Self(vec![]);
		a.add(args);
		a
	}

	fn add<I, S>(&mut self, args: I)
	where
		I: IntoIterator<Item = S>,
		S: Into<String>,
	{
		for arg in args {
			self.0.push(arg.into());
		}
	}

	fn append(&mut self, args: &str) {
		let split: Vec<&str> = args.split(' ').filter(|a| !a.is_empty()).collect();
		self.add(split);
	}
}
