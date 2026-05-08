//! Live benchmark terminal output: colours (grep-safe phase lines) and progress bars.

use clap::ValueEnum;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{IsTerminal, stdout};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub(crate) enum ColorChoice {
	/// Colour when stdout is a TTY and `NO_COLOR` is unset.
	#[default]
	Auto,
	Always,
	Never,
}

impl ColorChoice {
	pub(crate) fn resolve(self) -> bool {
		if std::env::var_os("NO_COLOR").is_some() {
			return false;
		}
		match self {
			ColorChoice::Never => false,
			ColorChoice::Always => true,
			ColorChoice::Auto => stdout().is_terminal(),
		}
	}
}

#[derive(Clone, Copy)]
pub(crate) struct BenchUi {
	use_color: bool,
}

impl BenchUi {
	pub(crate) fn new(color: ColorChoice) -> Self {
		Self {
			use_color: color.resolve(),
		}
	}

	/// Whether to draw an indicatif progress bar (TTY stdout only).
	pub(crate) fn use_progress_bar(self) -> bool {
		stdout().is_terminal()
	}

	/// Plain line (phase markers, dev.sh grep).
	pub(crate) fn println_plain(self, line: &str) {
		println!("{line}");
	}

	/// `head` is the label before ` took ` (e.g. `Create`, `BuildIndex`). When colour is on,
	/// ` took ` is dim cyan and the duration is bold vivid blue; otherwise plain ASCII.
	pub(crate) fn println_took_head(self, head: &str, duration: &str) {
		const DIM_TOOK: &str = "\x1b[2;36m";
		// Bold vivid blue (256-colour); distinct from dim cyan `took`.
		const TIME: &str = "\x1b[1;38;5;33m";
		const RESET: &str = "\x1b[0m";
		if self.use_color {
			println!("{head}{DIM_TOOK} took {RESET}{TIME}{duration}{RESET}");
		} else {
			println!("{head} took {duration}");
		}
	}

	/// Scan phase completion: `Scan :: {ctx}` plus optional dim `, combined workload (ratio N%)`.
	pub(crate) fn println_took_scan(self, ctx: &str, ratio_pct: Option<u32>, duration: &str) {
		const DIM_WORKLOAD: &str = "\x1b[2;38;5;244m";
		const DIM_TOOK: &str = "\x1b[2;36m";
		// Bold vivid blue (256-colour); distinct from dim cyan `took`.
		const TIME: &str = "\x1b[1;38;5;33m";
		const RESET: &str = "\x1b[0m";
		let head = format!("Scan :: {ctx}");
		if self.use_color {
			match ratio_pct {
				None => println!("{head}{DIM_TOOK} took {RESET}{TIME}{duration}{RESET}"),
				Some(p) => println!(
					"{head}{DIM_WORKLOAD}, combined workload (ratio {p}%){RESET}{DIM_TOOK} took {RESET}{TIME}{duration}{RESET}"
				),
			}
		} else {
			match ratio_pct {
				None => println!("{head} took {duration}"),
				Some(p) => println!("{head}, combined workload (ratio {p}%) took {duration}"),
			}
		}
	}

	/// Run title under a scan group (multi-`runs` specs).
	pub(crate) fn println_scan_run(self, run_name: &str) {
		if self.use_color {
			println!("\x1b[2m  ▸ {}\x1b[0m", run_name);
		} else {
			println!("  ▸ {run_name}");
		}
	}

	/// Decorative section separator (scan groups, Delete, Batches, …). Not used by dev.sh.
	pub(crate) fn section_header(self, title: &str) {
		let width = 52usize.min(title.len() + 8).max(44);
		let line: String = "═".repeat(width);
		if self.use_color {
			println!("\x1b[2m{line}\x1b[0m\n\x1b[1;36m  {}\x1b[0m\n\x1b[2m{line}\x1b[0m", title);
		} else {
			println!("{line}\n  {title}\n{line}");
		}
	}

	/// Optional dim line (setup hints; still plain substring for tooling).
	pub(crate) fn println_muted(self, line: &str) {
		if self.use_color {
			println!("\x1b[2m{line}\x1b[0m");
		} else {
			println!("{line}");
		}
	}

	/// Progress bar for `total` global iterations; message is a short label.
	pub(crate) fn progress_bar(self, total: u64, short_label: &str) -> Option<Arc<ProgressBar>> {
		if !self.use_progress_bar() || total == 0 {
			return None;
		}
		let pb = ProgressBar::new(total);
		let tmpl = if self.use_color {
			"{spinner:.green} [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({percent:>3}%) {eta_precise} {wide_msg}"
		} else {
			"{spinner} [{wide_bar}] {pos:>7}/{len:7} ({percent:>3}%) {eta_precise} {wide_msg}"
		};
		let style = ProgressStyle::with_template(tmpl)
			.or_else(|_| ProgressStyle::with_template("{wide_bar} {pos}/{len}"))
			.unwrap()
			.progress_chars("=>-");
		pb.set_style(style);
		pb.set_message(short_label.to_string());
		pb.reset_eta();
		pb.enable_steady_tick(Duration::from_millis(200));
		Some(Arc::new(pb))
	}
}
