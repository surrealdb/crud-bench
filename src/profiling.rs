use pprof::protos::Message;
use pprof::ProfilerGuard;
use pprof::ProfilerGuardBuilder;
use std::io::Write;
use std::sync::OnceLock;

static PROFILER: OnceLock<ProfilerGuard<'static>> = OnceLock::new();

pub(crate) fn initialise() {
	PROFILER.get_or_init(|| {
		ProfilerGuardBuilder::default()
			.frequency(1000)
			.blocklist(&["libc", "libgcc", "pthread", "vdso"])
			.build()
			.unwrap()
	});
}

pub(crate) fn process() {
	if let Some(guard) = PROFILER.get() {
		if let Ok(report) = guard.report().build() {
			// Output a flamegraph
			let file = std::fs::File::create("flamegraph.svg").unwrap();
			report.flamegraph(file).unwrap();
			// Output a pprof
			let mut file = std::fs::File::create("profile.pb").unwrap();
			let profile = report.pprof().unwrap();
			let mut content = Vec::new();
			profile.encode(&mut content).unwrap();
			file.write_all(&content).unwrap();
		}
	}
}
