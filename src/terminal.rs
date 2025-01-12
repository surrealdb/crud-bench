use anyhow::Result;
use std::fmt::Display;
use std::io::Write;
use std::io::{stdout, IsTerminal, Stdout};

pub(crate) struct Terminal(Option<Stdout>);

impl Default for Terminal {
	fn default() -> Self {
		let stdout = stdout();
		if stdout.is_terminal() {
			Self(Some(stdout))
		} else {
			Self(None)
		}
	}
}

impl Clone for Terminal {
	fn clone(&self) -> Self {
		Self(self.0.as_ref().map(|_| stdout()))
	}
}

impl Terminal {
	/// Write a line to this Terminal via a callback
	pub(crate) fn write<F, S>(&mut self, mut f: F) -> Result<()>
	where
		F: FnMut() -> Option<S>,
		S: Display,
	{
		if let Some(ref mut o) = self.0 {
			if let Some(s) = f() {
				write!(o, "{s}")?;
				o.flush()?;
			}
		}
		Ok(())
	}
}
