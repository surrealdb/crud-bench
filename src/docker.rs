use std::process::Command;

use log::{error, info};

pub(crate) struct DockerContainer {
	id: String,
	image: String,
	running: bool,
}

pub(crate) struct DockerParams {
	pub(crate) image: &'static str,
	pub(crate) pre_args: &'static str,
	pub(crate) post_args: &'static str,
}

impl DockerContainer {
	pub(crate) fn start(image: String, pre: &str, post: &str) -> Self {
		info!("Start Docker image {}", image);
		let mut arguments = Arguments::new(["run"]);
		arguments.append(pre);
		arguments.add(["-d", &image]);
		arguments.append(post);
		let id = Self::docker(arguments);
		Self {
			id,
			image,
			running: true,
		}
	}

	pub(crate) fn image(&self) -> &str {
		&self.image
	}

	pub(crate) fn logs(&self) {
		info!("Logging Docker container {}", self.id);
		let stdout = Self::docker(Arguments::new(["logs", &self.id]));
		println!("{}", stdout);
	}

	pub(crate) fn stop(&mut self) {
		if self.running {
			info!("Stopping Docker container {}", self.id);
			Self::docker(Arguments::new(["stop", &self.id]));
			self.running = false;
		}
	}
	fn docker(args: Arguments) -> String {
		let mut command = Command::new("docker");
		let command = command.args(args.0);
		info!("{:?}", command);
		let output = command.output().unwrap();
		let std_out = String::from_utf8(output.stdout).unwrap().trim().to_string();
		if !output.stderr.is_empty() {
			error!("{}", String::from_utf8(output.stderr).unwrap());
			error!("{command:?}");
		}
		assert_eq!(output.status.code(), Some(0), "Docker command failure: {command:?}");
		std_out
	}
}

impl Drop for DockerContainer {
	fn drop(&mut self) {
		// Be sure the container is stopped
		self.stop();
		// Delete the container
		info!("Delete Docker container {}", self.id);
		Self::docker(Arguments::new(["rm", &self.id]));
	}
}

pub(crate) struct Arguments(Vec<String>);

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
