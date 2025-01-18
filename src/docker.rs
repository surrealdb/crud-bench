use log::{error, info};
use std::fmt;
use std::process::{exit, Command};

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
		Self::stop();
	}
}

impl Container {
	/// Get the name of the Docker image
	pub(crate) fn image(&self) -> &str {
		&self.image
	}

	/// Start the Docker container
	pub(crate) fn start(image: String, pre: &str, post: &str) -> Self {
		// Output debug information to the logs
		info!("Starting Docker image '{image}'");
		// Configure the Docker command arguments
		let mut arguments = Arguments::new(["run"]);
		arguments.append(pre);
		arguments.add(["--rm"]);
		arguments.add(["--quiet"]);
		arguments.add(["--name", "crud-bench"]);
		arguments.add(["--net", "host"]);
		arguments.add(["-d", &image]);
		arguments.append(post);
		// Execute the Docker run command
		Self::docker(arguments);
		// Return the container name
		Self {
			image,
		}
	}

	/// Stop the Docker container
	pub(crate) fn stop() {
		info!("Stopping Docker container 'crud-bench'");
		Self::docker(Arguments::new(["container", "stop", "--time", "300", "crud-bench"]));
	}

	/// Output the container logs
	pub(crate) fn logs() {
		info!("Logging Docker container 'crud-bench'");
		let logs = Self::docker(Arguments::new(["logs", "crud-bench"]));
		println!("{logs}");
	}

	fn docker(args: Arguments) -> String {
		// Create a new process command
		let mut command = Command::new("docker");
		// Set the arguments on the command
		let command = command.args(args.0.clone());
		// Catch all output from the command
		let output = command.output().expect("Failed to execute process");
		// Get the stdout out from the command
		let stdout = String::from_utf8(output.stdout).unwrap().trim().to_string();
		// Output command failure if errored
		if let Some(i) = output.status.code() {
			if i != 0 {
				let stderr = String::from_utf8(output.stderr).unwrap().trim().to_string();
				error!("Docker command failure: `docker {args}`");
				eprintln!("{stderr}");
				eprintln!("--------------------------------------------------");
				Container::logs();
				eprintln!("--------------------------------------------------");
				exit(1);
			}
		}
		stdout
	}
}

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
