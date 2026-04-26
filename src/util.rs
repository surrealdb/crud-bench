use std::time::Duration;

pub(crate) fn format_duration(duration: Duration) -> String {
	let secs = duration.as_secs();
	if secs >= 86400 {
		let days = secs / 86400;
		let hours = (secs % 86400) / 3600;
		format!("{days}d {hours}h")
	} else if secs >= 3600 {
		let hours = secs / 3600;
		let minutes = (secs % 3600) / 60;
		format!("{hours}h {minutes}m")
	} else if secs >= 60 {
		let minutes = secs / 60;
		let seconds = secs % 60;
		format!("{minutes}m {seconds}s")
	} else if secs > 0 {
		let seconds = secs;
		let millis = duration.subsec_millis();
		format!("{seconds}s {millis}ms")
	} else if duration.subsec_millis() > 0 {
		let millis = duration.subsec_millis();
		let micros = duration.subsec_micros() % 1000;
		format!("{millis}ms {micros}µs")
	} else if duration.subsec_micros() > 0 {
		let micros = duration.subsec_micros();
		let nanos = duration.subsec_nanos() % 1000;
		format!("{micros}µs {nanos}ns")
	} else {
		format!("{}ns", duration.subsec_nanos())
	}
}
