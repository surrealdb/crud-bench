use crate::benchmark::Benchmark;
use log::{debug, error, info};
use std::fmt;
use std::io::Write;
use std::process::{Command, Stdio, exit};
use std::time::Duration;

const RETRIES: i32 = 10;

const TIMEOUT: Duration = Duration::from_secs(6);

pub(crate) struct DockerParams {
	pub(crate) image: &'static str,
	pub(crate) pre_args: String,
	pub(crate) post_args: String,
	/// Where the image keeps its data. Mounted from a named volume when a run
	/// splits loading from querying, so the dataset outlives the container.
	pub(crate) data_path: Option<&'static str>,
	/// A Dockerfile for `image`, when crud-bench builds the image locally
	/// rather than pulling it. It must need no build context.
	pub(crate) dockerfile: Option<&'static str>,
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
	pub(crate) fn start(
		image: String,
		pre: &str,
		post: &str,
		data_path: Option<&str>,
		dockerfile: Option<&str>,
		options: &Benchmark,
	) -> Self {
		// Build the image first if it is not pulled from a registry
		if let Some(dockerfile) = dockerfile
			&& let Err(e) = Self::build(&image, dockerfile)
		{
			error!("Docker build failure for image '{image}'");
			error!("{e}");
			exit(1);
		}
		// Output debug information to the logs
		info!("Starting Docker image '{image}'");
		// A load run starts from an empty volume; a query run mounts the one a
		// load run filled.
		let volume = options.volume.as_deref().zip(data_path);
		if let Some((volume, _)) = volume
			&& options.mode == crate::reuse::RunMode::Load
		{
			let _ = Self::execute(Arguments::new(["volume", "rm", "--force", volume]));
		}
		// Attempt to start Docker 10 times
		for i in 1..=RETRIES {
			// Configure the Docker command arguments
			let mut args = Arguments::new(["run"]);
			// Configure the default pre arguments
			args.append(pre);
			// Configure any custom pre arguments
			if let Ok(v) = std::env::var("DOCKER_PRE_ARGS") {
				args.append(&v);
			}
			// Run in privileged mode if specified
			if options.privileged {
				args.add(["--privileged"]);
			}
			// Persist the data directory in a named volume
			if let Some((volume, path)) = volume {
				args.add(["-v", &format!("{volume}:{path}")]);
			}
			// Configure the Docker container options
			args.add(["--rm"]);
			args.add(["--quiet"]);
			// A locally built image has no registry to pull from
			args.add([
				"--pull",
				if dockerfile.is_some() {
					"never"
				} else {
					"always"
				},
			]);
			args.add(["--name", "crud-bench"]);
			args.add(["--net", "host"]);
			args.add(["-d", &image]);
			// Configure the default post arguments
			args.append(post);
			// Configure any custom post arguments
			if let Ok(v) = std::env::var("DOCKER_POST_ARGS") {
				args.append(&v);
			}
			// Execute the Docker run command
			match Self::execute(args.clone()) {
				// The command executed successfully
				Ok(_) => break,
				// There was an error with the command
				Err(e) => match i {
					// This is the last attempt so exit fully
					RETRIES => {
						error!("Docker command failure: `docker {args}`");
						error!("{e}");
						exit(1);
					}
					// Let's log the output and retry the command
					_ => {
						debug!("Docker command failure: `docker {args}`");
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

	/// Build the Docker image `image` from `dockerfile`, passed on standard input
	fn build(image: &str, dockerfile: &str) -> Result<String, String> {
		// Output debug information to the logs
		info!("Building Docker image '{image}'");
		println!("Running command: `docker build --quiet --tag {image} -`");
		// Start the build with the Dockerfile on standard input
		let mut child = Command::new("docker")
			.args(["build", "--quiet", "--tag", image, "-"])
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.spawn()
			.map_err(|e| e.to_string())?;
		// Closing standard input after writing ends the Dockerfile
		let mut stdin = child.stdin.take().expect("standard input is piped");
		stdin.write_all(dockerfile.as_bytes()).map_err(|e| e.to_string())?;
		drop(stdin);
		// Wait for the build to finish
		let output = child.wait_with_output().map_err(|e| e.to_string())?;
		match output.status.success() {
			false => Err(String::from_utf8_lossy(&output.stderr).trim().to_string()),
			true => Ok(String::from_utf8_lossy(&output.stdout).trim().to_string()),
		}
	}

	/// Host PID of the container's main process
	pub(crate) fn pid(&self) -> Result<u32, String> {
		let args = ["inspect", "--format", "{{.State.Pid}}", "crud-bench"];
		let pid = Self::execute(Arguments::new(args))?;
		match pid.parse() {
			// A stopped container reports PID 0
			Ok(0) => Err("the container is not running".to_string()),
			Ok(pid) => Ok(pid),
			Err(e) => Err(format!("unexpected PID '{pid}': {e}")),
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
		println!("Running command: `docker {args}`");
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
