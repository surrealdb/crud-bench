use crate::result::{BenchmarkResult, OperationResult};
use serde_json::{Map, Value, json};

/// Generate an HTML file with interactive charts for a single benchmark result
pub(crate) fn generate_html(result: &BenchmarkResult, database_name: &str) -> String {
	let system_info_html = generate_system_info(result);

	format!(
		r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CRUD Benchmark Results - {database_name}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:opsz,wght@14..32,100..900&family=JetBrains+Mono:wght@100..800&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/apexcharts@3.45.0/dist/apexcharts.min.js"></script>
    <style>
        :root {{
            --sd-bg: #0e0c14;
            --sd-surface: #16141f;
            --sd-surface-2: #1a1825;
            --sd-surface-border: rgba(111, 121, 136, 0.2);
            --sd-text: #e8e4f0;
            --sd-text-muted: #9990ab;
            --sd-text-dim: #6f7988;
            --sd-accent: #7c5cfc;
            --sd-write: #c471f5;
            --sd-energy: #d255fe;
            --sd-passion: #651ddd;
            --sd-line: #3d3650;
            --sd-success: #34d399;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: "Inter", system-ui, sans-serif;
            font-size: 16px;
            line-height: 1.6;
            color: var(--sd-text);
            background-color: var(--sd-bg);
            background-image:
                radial-gradient(ellipse 80% 60% at 50% -10%, rgba(124, 92, 252, 0.15), transparent 60%),
                radial-gradient(ellipse 60% 40% at 100% 50%, rgba(210, 85, 254, 0.08), transparent 70%),
                radial-gradient(ellipse 60% 40% at 0% 80%, rgba(101, 29, 221, 0.1), transparent 70%);
            -webkit-font-smoothing: antialiased;
            min-height: 100vh;
            padding: 24px 16px 64px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 40px 32px 48px;
        }}
        @media (max-width: 640px) {{
            .container {{ padding: 24px 16px 32px; }}
        }}
        .eyebrow {{
            font-family: "JetBrains Mono", ui-monospace, monospace;
            font-size: 11px;
            letter-spacing: 0.2em;
            text-transform: uppercase;
            color: var(--sd-text-muted);
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .eyebrow::before {{
            content: "";
            width: 24px;
            height: 1px;
            background: var(--sd-accent);
            display: inline-block;
        }}
        h1 {{
            font-size: clamp(2rem, 4vw, 3.25rem);
            font-weight: 700;
            letter-spacing: -0.03em;
            line-height: 1.05;
            margin-bottom: 16px;
            color: #ffffff;
        }}
        h1 .grad {{
            background: linear-gradient(135deg, var(--sd-energy), var(--sd-passion));
            -webkit-background-clip: text;
            background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .subtitle {{
            color: var(--sd-text-muted);
            font-size: 1.125rem;
            line-height: 1.55;
            font-style: italic;
            margin-bottom: 40px;
            max-width: 720px;
        }}
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
            gap: 28px;
            margin-bottom: 28px;
        }}
        @media (max-width: 700px) {{
            .chart-grid {{ grid-template-columns: 1fr; }}
        }}
        .chart-container {{
            background:
                linear-gradient(var(--sd-surface), var(--sd-surface)) padding-box,
                linear-gradient(135deg, rgba(124, 92, 252, 0.18), rgba(124, 92, 252, 0.04)) border-box;
            border: 1px solid transparent;
            border-radius: 12px;
            padding: 22px 26px;
        }}
        .chart-title {{
            font-size: 1.375rem;
            font-weight: 600;
            letter-spacing: -0.02em;
            margin-bottom: 14px;
            color: #ffffff;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 36px;
        }}
        .stat-card {{
            background:
                linear-gradient(var(--sd-surface), var(--sd-surface)) padding-box,
                linear-gradient(135deg, rgba(124, 92, 252, 0.22), rgba(124, 92, 252, 0.05)) border-box;
            border: 1px solid transparent;
            border-radius: 12px;
            padding: 20px 18px;
            text-align: center;
            border-top: 3px solid var(--sd-accent);
            color: var(--sd-text);
        }}
        .stat-card.blue {{ border-top-color: #7c5cfc; }}
        .stat-card.green {{ border-top-color: #34d399; }}
        .stat-card.orange {{ border-top-color: #d255fe; }}
        .stat-card.red {{ border-top-color: #c471f5; }}
        .stat-label {{
            font-family: "JetBrains Mono", ui-monospace, monospace;
            font-size: 10px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: var(--sd-text-dim);
            margin-bottom: 8px;
        }}
        .stat-value {{
            font-size: 1.85rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            color: #ffffff;
        }}
        .stat-unit {{
            font-size: 0.65rem;
            font-weight: 500;
            color: var(--sd-text-muted);
            margin-left: 2px;
        }}
        .chart {{
            max-height: 400px;
        }}
        .full-width {{
            grid-column: 1 / -1;
        }}
        .system-info {{
            background:
                linear-gradient(var(--sd-surface), var(--sd-surface)) padding-box,
                linear-gradient(135deg, rgba(124, 92, 252, 0.18), rgba(124, 92, 252, 0.04)) border-box;
            border: 1px solid transparent;
            border-radius: 12px;
            padding: 24px 26px;
            margin-bottom: 36px;
        }}
        .system-info-title {{
            font-size: 1.25rem;
            font-weight: 600;
            letter-spacing: -0.02em;
            margin-bottom: 18px;
            color: #ffffff;
        }}
        .system-info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 12px;
        }}
        .system-info-item {{
            display: flex;
            align-items: center;
            padding: 12px 14px;
            background: var(--sd-surface-2);
            border-radius: 8px;
            border: 1px solid var(--sd-surface-border);
        }}
        .system-info-label {{
            font-weight: 600;
            color: var(--sd-text-muted);
            margin-right: 8px;
            min-width: 120px;
            font-size: 0.9rem;
        }}
        .system-info-value {{
            color: var(--sd-text);
            font-family: "JetBrains Mono", ui-monospace, monospace;
            font-size: 0.875rem;
        }}
        .apexcharts-tooltip-box {{
            padding: 10px 12px;
            background: var(--sd-surface) !important;
            border: 1px solid var(--sd-surface-border) !important;
            border-radius: 8px;
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
            color: var(--sd-text);
        }}
        .apexcharts-tooltip-box > div {{
            padding: 3px 0;
            color: var(--sd-text-muted);
        }}
        .apexcharts-tooltip-box strong {{
            color: var(--sd-text);
        }}
        .chart-subtitle {{
            color: var(--sd-text-muted);
            font-size: 0.95em;
            line-height: 1.45;
            margin: -6px 0 14px 0;
            max-width: 920px;
        }}
        .scan-percentile-table-wrap {{
            overflow-x: auto;
            max-width: 100%;
            margin-top: 4px;
        }}
        .scan-percentile-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
            color: var(--sd-text-muted);
        }}
        .scan-percentile-table caption {{
            caption-side: top;
            text-align: left;
            font-size: 0.85em;
            color: var(--sd-text-muted);
            padding-bottom: 8px;
        }}
        .scan-percentile-table th {{
            font-weight: 600;
            background: var(--sd-surface-2);
            color: var(--sd-text);
            white-space: nowrap;
            font-family: "JetBrains Mono", ui-monospace, monospace;
            font-size: 10px;
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }}
        .scan-percentile-table th, .scan-percentile-table td {{
            border: 1px solid var(--sd-surface-border);
            padding: 8px 10px;
        }}
        .scan-percentile-table td.num {{
            font-variant-numeric: tabular-nums;
            text-align: right;
            font-family: "JetBrains Mono", ui-monospace, monospace;
            color: var(--sd-text);
        }}
        .scan-percentile-table td.scan-name {{
            max-width: 28em;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            text-align: left;
            color: var(--sd-text);
        }}
        .scan-percentile-table td.spark {{
            text-align: center;
            vertical-align: middle;
            min-width: 130px;
            background: rgba(22, 20, 31, 0.5);
        }}
        .scan-percentile-table th.scan-dist-col,
        .scan-percentile-table td.scan-dist {{
            border-left: 1px solid var(--sd-line);
            text-align: center;
            vertical-align: middle;
            min-width: 96px;
            background: rgba(22, 20, 31, 0.35);
        }}
        .scan-percentile-table tbody tr:nth-child(even) {{
            background: rgba(26, 24, 37, 0.45);
        }}
        .scan-sparkline {{
            display: block;
            margin: 0 auto;
            flex-shrink: 0;
        }}
        .scan-mini-dist {{
            display: block;
            margin: 0 auto;
            flex-shrink: 0;
        }}
        .chart-container .apexcharts-svg .apexcharts-background {{
            fill: var(--sd-surface) !important;
        }}
        .chart-container line.apexcharts-gridline {{
            stroke: var(--sd-surface-border) !important;
            opacity: 0.75;
        }}
        .chart-container .apexcharts-grid-borders line {{
            stroke: var(--sd-surface-border) !important;
            opacity: 0.8;
        }}
        .chart-container .apexcharts-yaxis line {{
            stroke: var(--sd-surface-border) !important;
            opacity: 0.95;
        }}
        .chart-container .apexcharts-xaxis line {{
            stroke: var(--sd-surface-border) !important;
            opacity: 0.95;
        }}
        .chart-container .apexcharts-boxPlot-series path.apexcharts-boxPlot-area {{
            stroke-width: 2.25px !important;
            stroke-linecap: round;
            stroke-linejoin: round;
        }}
        .chart-container #scanLatencyChart path.apexcharts-boxPlot-area {{
            stroke: #faf7ff !important;
        }}
        .chart-container #batchLatencyChart path.apexcharts-boxPlot-area {{
            stroke: #fff6fb !important;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="eyebrow">CRUD Benchmark</div>
        <h1>Benchmark <span class="grad">Results</span></h1>
        <div class="subtitle">{database_name}</div>

        {system_info}

        <div class="stats-grid">
            {stats_cards}
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <div class="chart-title">Operations per second</div>
                <div id="opsChart"></div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Latency distribution (ms)</div>
                <div id="latencyChart"></div>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Percentile comparison</div>
                <div id="percentileChart"></div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Resource usage</div>
                <div id="resourceChart"></div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Disk I/O</div>
                <div id="diskChart"></div>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Scan latency distribution</div>
                <div id="scanLatencyChart"></div>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Scan latency percentiles</div>
                {scan_percentile_table}
            </div>

            <div class="chart-container">
                <div class="chart-title">Batch throughput</div>
                <div id="batchThroughputChart"></div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Batch latency distribution</div>
                <div id="batchLatencyChart"></div>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Batch percentile comparison</div>
                <div id="batchPercentileChart"></div>
            </div>
        </div>
    </div>

    <script>
        {chart_scripts}
    </script>
</body>
</html>"##,
		database_name = database_name,
		system_info = system_info_html,
		stats_cards = generate_stat_cards(result),
		scan_percentile_table = generate_scan_percentile_table_html(&scan_chart_rows(result)),
		chart_scripts = generate_chart_scripts(result)
	)
}

fn format_number_with_commas(num: f64) -> String {
	let num_str = format!("{:.0}", num);
	let mut result = String::new();
	let chars: Vec<char> = num_str.chars().collect();
	let len = chars.len();

	for (i, ch) in chars.iter().enumerate() {
		if i > 0 && (len - i).is_multiple_of(3) {
			result.push(',');
		}
		result.push(*ch);
	}

	result
}

fn generate_system_info(result: &BenchmarkResult) -> String {
	if let Some(system) = &result.system {
		// Convert timestamp to readable format
		let datetime = chrono::DateTime::from_timestamp(system.timestamp, 0)
			.map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
			.unwrap_or_else(|| "Unknown".to_string());

		// Convert memory to GB
		let total_memory_gb = system.total_memory as f64 / 1024.0 / 1024.0 / 1024.0;
		let available_memory_gb = system.available_memory as f64 / 1024.0 / 1024.0 / 1024.0;

		format!(
			r##"<div class="system-info">
            <div class="system-info-title">System Information</div>
            <div class="system-info-grid">
                <div class="system-info-item">
                    <span class="system-info-label">Hostname:</span>
                    <span class="system-info-value">{}</span>
                </div>
                <div class="system-info-item">
                    <span class="system-info-label">Timestamp:</span>
                    <span class="system-info-value">{}</span>
                </div>
                <div class="system-info-item">
                    <span class="system-info-label">Operating System:</span>
                    <span class="system-info-value">{} {}</span>
                </div>
                <div class="system-info-item">
                    <span class="system-info-label">Kernel Version:</span>
                    <span class="system-info-value">{}</span>
                </div>
                <div class="system-info-item">
                    <span class="system-info-label">CPU Architecture:</span>
                    <span class="system-info-value">{}</span>
                </div>
                <div class="system-info-item">
                    <span class="system-info-label">CPU Cores:</span>
                    <span class="system-info-value">{} ({} physical)</span>
                </div>
                <div class="system-info-item">
                    <span class="system-info-label">Total Memory:</span>
                    <span class="system-info-value">{:.2} GB</span>
                </div>
                <div class="system-info-item">
                    <span class="system-info-label">Available Memory:</span>
                    <span class="system-info-value">{:.2} GB</span>
                </div>
            </div>
        </div>"##,
			system.hostname,
			datetime,
			system.os_name,
			system.os_version,
			system.kernel_version,
			system.cpu_arch,
			system.cpu_cores,
			system.cpu_physical_cores,
			total_memory_gb,
			available_memory_gb
		)
	} else {
		String::new()
	}
}

fn generate_stat_cards(result: &BenchmarkResult) -> String {
	let mut cards = String::new();

	if let Some(creates) = &result.creates {
		cards.push_str(&format!(
			r##"<div class="stat-card blue">
                    <div class="stat-label">Create Operations</div>
                    <div class="stat-value">{}<span class="stat-unit"> ops/s</span></div>
                </div>"##,
			format_number_with_commas(creates.ops())
		));
	}

	if let Some(reads) = &result.reads {
		cards.push_str(&format!(
			r##"<div class="stat-card green">
                    <div class="stat-label">Read Operations</div>
                    <div class="stat-value">{}<span class="stat-unit"> ops/s</span></div>
                </div>"##,
			format_number_with_commas(reads.ops())
		));
	}

	if let Some(updates) = &result.updates {
		cards.push_str(&format!(
			r##"<div class="stat-card orange">
                    <div class="stat-label">Update Operations</div>
                    <div class="stat-value">{}<span class="stat-unit"> ops/s</span></div>
                </div>"##,
			format_number_with_commas(updates.ops())
		));
	}

	if let Some(deletes) = &result.deletes {
		cards.push_str(&format!(
			r##"<div class="stat-card red">
                    <div class="stat-label">Delete Operations</div>
                    <div class="stat-value">{}<span class="stat-unit"> ops/s</span></div>
                </div>"##,
			format_number_with_commas(deletes.ops())
		));
	}

	cards
}

fn html_escape(s: &str) -> String {
	s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

/// Inline SVG sparkline for Min → Max percentile latencies (same order as the table).
fn percentile_sparkline_svg(values_us: &[f64]) -> String {
	if values_us.is_empty() {
		return String::new();
	}
	const W: f64 = 120.0;
	const H: f64 = 28.0;
	let min_v = values_us.iter().cloned().fold(f64::INFINITY, f64::min);
	let max_v = values_us.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
	let range = (max_v - min_v).max(1.0);
	let n = values_us.len();
	let mut points = String::new();
	for (i, &v) in values_us.iter().enumerate() {
		if i > 0 {
			points.push(' ');
		}
		let x = if n <= 1 {
			W / 2.0
		} else {
			1.0 + (i as f64 / (n - 1) as f64) * (W - 2.0)
		};
		let y = (H - 1.0) - ((v - min_v) / range) * (H - 2.0);
		points.push_str(&format!("{:.1},{:.1}", x, y));
	}
	format!(
		r##"<svg class="scan-sparkline" width="120" height="28" viewBox="0 0 120 28" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><polyline fill="none" stroke="#dcc6ff" stroke-width="2.25" stroke-linecap="round" stroke-linejoin="round" points="{points}"/></svg>"##,
		points = points
	)
}

/// Horizontal mini distribution (candlestick-style): min–max baseline, P01/P99 ticks, IQR box, median.
fn percentile_mini_distribution_svg(values_us: &[f64]) -> String {
	if values_us.len() != 8 {
		return String::new();
	}
	let min_v = values_us[0];
	let p01 = values_us[1];
	let p25 = values_us[2];
	let p50 = values_us[3];
	let p75 = values_us[4];
	let max_v = values_us[7];
	let p99 = values_us[6];

	const W: f64 = 88.0;
	const H: f64 = 28.0;
	let lo = min_v;
	let hi = max_v;
	let range = (hi - lo).max(1.0);
	let pad = 2.0;
	let usable = W - 2.0 * pad;
	let xf = |v: f64| pad + ((v - lo) / range) * usable;

	let x_min = xf(min_v);
	let x_max = xf(max_v);
	let x_p01 = xf(p01);
	let x_p25 = xf(p25);
	let x_p50 = xf(p50);
	let x_p75 = xf(p75);
	let x_p99 = xf(p99);

	let box_left = x_p25.min(x_p75);
	let box_right = x_p25.max(x_p75);
	let box_w = (box_right - box_left).max(1.25);

	let ym = H / 2.0;
	let body_top = ym - 5.0;
	let body_h = 10.0;
	let tick_top = ym - 5.5;
	let tick_bot = ym + 5.5;

	format!(
		r##"<svg class="scan-mini-dist" width="{w}" height="{h}" viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><line x1="{x_min}" y1="{ym}" x2="{x_max}" y2="{ym}" stroke="rgba(111,121,136,0.28)" stroke-width="1.5" stroke-linecap="round"/><line x1="{x_p01}" y1="{tick_top}" x2="{x_p01}" y2="{tick_bot}" stroke="#dcc6ff" stroke-width="1.15" opacity="0.95"/><line x1="{x_p99}" y1="{tick_top}" x2="{x_p99}" y2="{tick_bot}" stroke="#dcc6ff" stroke-width="1.15" opacity="0.95"/><rect x="{box_left}" y="{body_top}" width="{box_w}" height="{body_h}" rx="1.5" fill="#b794ff" opacity="0.65" stroke="#ebd9ff" stroke-width="1.15"/><line x1="{x_p50}" y1="4.5" x2="{x_p50}" y2="23.5" stroke="#f5eeff" stroke-width="2" stroke-linecap="round"/></svg>"##,
		w = W as i32,
		h = H as i32,
		x_min = x_min,
		x_max = x_max,
		x_p01 = x_p01,
		x_p99 = x_p99,
		box_left = box_left,
		box_w = box_w,
		body_top = body_top,
		body_h = body_h,
		x_p50 = x_p50,
		ym = ym,
		tick_top = tick_top,
		tick_bot = tick_bot,
	)
}

fn scan_distribution_title_attr(r: &OperationResult) -> String {
	let s = format!(
		"Min: {:.3} ms | Q1: {:.3} ms | Median: {:.3} ms | Q3: {:.3} ms | Max: {:.3} ms",
		r.min() as f64 / 1000.0,
		r.q25() as f64 / 1000.0,
		r.q50() as f64 / 1000.0,
		r.q75() as f64 / 1000.0,
		r.max() as f64 / 1000.0,
	);
	html_escape(&s)
}

fn generate_scan_percentile_table_html(rows: &[(String, &OperationResult)]) -> String {
	if rows.is_empty() {
		return r#"<p class="chart-subtitle">No scan benchmarks in this result.</p>"#.to_string();
	}
	let mut out = String::from(
		r#"<div class="scan-percentile-table-wrap"><table class="scan-percentile-table">
<thead>
<tr>
<th scope="col">Scan</th>
<th scope="col">Throughput</th>
<th scope="col">Min</th>
<th scope="col">P01</th>
<th scope="col">P25</th>
<th scope="col">P50</th>
<th scope="col">P75</th>
<th scope="col">P95</th>
<th scope="col">P99</th>
<th scope="col">Max</th>
<th scope="col">Trend</th>
<th scope="col" class="scan-dist-col">Distribution</th>
</tr>
</thead>
<tbody>"#,
	);
	for (name, r) in rows {
		let vals = [
			r.min() as f64,
			r.q01() as f64,
			r.q25() as f64,
			r.q50() as f64,
			r.q75() as f64,
			r.q95() as f64,
			r.q99() as f64,
			r.max() as f64,
		];
		let spark = percentile_sparkline_svg(&vals);
		let mini = percentile_mini_distribution_svg(&vals);
		let dist_title = scan_distribution_title_attr(r);
		let esc = html_escape(name);
		let mut row = format!(r#"<tr><td class="scan-name" title="{esc}">{esc}</td>"#);
		row.push_str(&format!(r#"<td class="num">{:.2}</td>"#, r.ops()));
		for v in &vals {
			row.push_str(&format!(r#"<td class="num">{:.3}</td>"#, v / 1000.0));
		}
		row.push_str(&format!(r#"<td class="spark">{spark}</td>"#));
		row.push_str(&format!(r#"<td class="scan-dist" title="{dist_title}">{mini}</td></tr>"#));
		out.push_str(&row);
	}
	out.push_str("</tbody></table></div>");
	out
}

fn generate_chart_scripts(result: &BenchmarkResult) -> String {
	let scan_rows = scan_chart_rows(result);
	let scan_n = scan_rows.len();
	let scan_chart_height_bar = scan_chart_bar_height(scan_n);
	let scan_boxplot_data = scan_boxplot_json(&scan_rows);
	let scan_ops_lookup = scan_ops_lookup_json(&scan_rows);

	format!(
		r##"
// Helper function to format numbers with commas
function formatNumber(num) {{
    return num.toFixed(0).replace(/\B(?=(\d{{3}})+(?!\d))/g, ",");
}}

// Matches :root Spectron tokens (--sd-*)
var SD = {{
    text: '#e8e4f0',
    muted: '#9990ab',
    dim: '#6f7988',
    line: '#3d3650',
    surfaceBorder: 'rgba(111, 121, 136, 0.2)',
    surface: '#16141f',
    accent: '#7c5cfc',
    energy: '#d255fe',
    passion: '#651ddd',
    write: '#c471f5',
    success: '#34d399'
}};

// Native SVG tooltip: hover truncated axis labels to see full scan names
function apexAttachFullCategoryTitles(chartContext) {{
    try {{
        var labels = chartContext.w.globals.labels;
        if (!labels || labels.length === 0) return;
        var root = chartContext.el;
        var groups = root.querySelectorAll('.apexcharts-yaxis-label');
        if (groups.length === 0) {{
            groups = root.querySelectorAll('.apexcharts-xaxis-label');
        }}
        for (var i = 0; i < groups.length; i++) {{
            var g = groups[i];
            var full = labels[i];
            if (full === undefined || full === null) continue;
            var old = g.getElementsByTagName('title')[0];
            if (old) old.remove();
            var titleEl = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            titleEl.textContent = String(full);
            g.insertBefore(titleEl, g.firstChild);
        }}
    }} catch (e) {{}}
}}

function apexAttachBoxplotCategoryTitles(chartContext) {{
    try {{
        var series = chartContext.w.config.series;
        var data = series && series[0] && series[0].data;
        if (!data || !data.length) return;
        var root = chartContext.el;
        var groups = root.querySelectorAll('.apexcharts-yaxis-label');
        if (groups.length === 0) {{
            groups = root.querySelectorAll('.apexcharts-xaxis-label');
        }}
        for (var i = 0; i < groups.length && i < data.length; i++) {{
            var g = groups[i];
            var full = data[i].x;
            if (full === undefined || full === null) continue;
            var old = g.getElementsByTagName('title')[0];
            if (old) old.remove();
            var titleEl = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            titleEl.textContent = String(full);
            g.insertBefore(titleEl, g.firstChild);
        }}
    }} catch (e) {{}}
}}

// Operations Per Second Chart
var opsChart = new ApexCharts(document.querySelector("#opsChart"), {{
    series: [{{
        name: 'Operations/Second',
        data: {ops_data}
    }}],
    chart: {{
        type: 'bar',
        height: 350,
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }}
    }},
    theme: {{ mode: 'dark' }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4
    }},
    plotOptions: {{
        bar: {{
            distributed: true,
            borderRadius: 4
        }}
    }},
    colors: ['#7c5cfc', '#d255fe', '#651ddd', '#c471f5'],
    dataLabels: {{ enabled: false }},
    legend: {{ show: false }},
    xaxis: {{
        categories: {ops_labels}
    }},
    yaxis: {{
        title: {{ text: 'Operations per Second' }},
        labels: {{
            formatter: function(val) {{
                return formatNumber(val);
            }}
        }}
    }},
    tooltip: {{
        y: {{
            formatter: function(val) {{
                return formatNumber(val) + ' ops/s';
            }}
        }}
    }}
}});
opsChart.render();

// Latency Distribution Chart
var latencyChart = new ApexCharts(document.querySelector("#latencyChart"), {{
    series: [{{
        name: 'Mean Latency (ms)',
        data: {latency_data}
    }}],
    chart: {{
        type: 'bar',
        height: 350,
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }}
    }},
    theme: {{ mode: 'dark' }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4
    }},
    plotOptions: {{
        bar: {{
            distributed: true,
            borderRadius: 4
        }}
    }},
    colors: ['#7c5cfc', '#a78bfa', '#d255fe', '#c471f5'],
    dataLabels: {{ enabled: false }},
    legend: {{ show: false }},
    xaxis: {{
        categories: {latency_labels}
    }},
    yaxis: {{
        title: {{ text: 'Latency (milliseconds)' }},
        labels: {{
            formatter: function(val) {{
                return val.toFixed(2);
            }}
        }}
    }},
    tooltip: {{
        y: {{
            formatter: function(val) {{
                return val.toFixed(2) + ' ms';
            }}
        }}
    }}
}});
latencyChart.render();

// Percentile Comparison Chart
var percentileChart = new ApexCharts(document.querySelector("#percentileChart"), {{
    series: {percentile_series},
    chart: {{
        type: 'line',
        height: 350,
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }}
    }},
    theme: {{ mode: 'dark' }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4
    }},
    stroke: {{
        width: 3,
        curve: 'smooth'
    }},
    colors: ['#7c5cfc', '#d255fe', '#651ddd', '#c471f5'],
    dataLabels: {{ enabled: false }},
    legend: {{
        position: 'top',
        horizontalAlign: 'left',
        labels: {{ colors: SD.text }},
        markers: {{
            strokeColors: SD.surface
        }}
    }},
    xaxis: {{
        categories: ['Min', 'P01', 'P25', 'P50', 'P75', 'P95', 'P99', 'Max']
    }},
    yaxis: {{
        title: {{ text: 'Latency (microseconds)' }},
        labels: {{
            formatter: function(val) {{
                return formatNumber(val);
            }}
        }}
    }},
    tooltip: {{
        shared: false,
        intersect: false,
        y: {{
            formatter: function(val) {{
                return (val / 1000).toFixed(3) + ' ms';
            }}
        }}
    }}
}});
percentileChart.render();

// Resource Usage Chart
var resourceChart = new ApexCharts(document.querySelector("#resourceChart"), {{
    series: [
        {{
            name: 'CPU Usage (%)',
            type: 'column',
            data: {cpu_data}
        }},
        {{
            name: 'Memory (MB)',
            type: 'column',
            data: {memory_data}
        }}
    ],
    chart: {{
        type: 'line',
        height: 350,
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }}
    }},
    theme: {{ mode: 'dark' }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4
    }},
    stroke: {{
        width: [0, 0]
    }},
    plotOptions: {{
        bar: {{
            borderRadius: 4
        }}
    }},
    colors: ['#d255fe', '#7c5cfc'],
    dataLabels: {{ enabled: false }},
    legend: {{
        position: 'top',
        horizontalAlign: 'left',
        labels: {{ colors: SD.text }},
        markers: {{
            strokeColors: SD.surface
        }}
    }},
    xaxis: {{
        categories: {resource_labels}
    }},
    yaxis: [
        {{
            title: {{ text: 'CPU (%)' }},
            labels: {{
                formatter: function(val) {{
                    return val.toFixed(1);
                }}
            }}
        }},
        {{
            opposite: true,
            title: {{ text: 'Memory (MB)' }},
            labels: {{
                formatter: function(val) {{
                    return formatNumber(val);
                }}
            }}
        }}
    ]
}});
resourceChart.render();

// Disk I/O Chart
var diskChart = new ApexCharts(document.querySelector("#diskChart"), {{
    series: [
        {{
            name: 'Writes (MB)',
            data: {disk_writes}
        }},
        {{
            name: 'Reads (MB)',
            data: {disk_reads}
        }}
    ],
    chart: {{
        type: 'bar',
        height: 350,
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }}
    }},
    theme: {{ mode: 'dark' }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4
    }},
    plotOptions: {{
        bar: {{
            borderRadius: 4
        }}
    }},
    colors: ['#d255fe', '#7c5cfc'],
    dataLabels: {{ enabled: false }},
    legend: {{
        position: 'top',
        horizontalAlign: 'left',
        labels: {{ colors: SD.text }},
        markers: {{
            strokeColors: SD.surface
        }}
    }},
    xaxis: {{
        categories: {disk_labels}
    }},
    yaxis: {{
        title: {{ text: 'Data (MB)' }},
        labels: {{
            formatter: function(val) {{
                return formatNumber(val);
            }}
        }}
    }},
    tooltip: {{
        y: {{
            formatter: function(val) {{
                return formatNumber(val) + ' MB';
            }}
        }}
    }}
}});
diskChart.render();

// Scan Latency Distribution Chart
// Create ops lookup for tooltips
var scanOpsLookup = {scan_ops_lookup};

var scanLatencyChart = new ApexCharts(document.querySelector("#scanLatencyChart"), {{
    series: [
        {{
            name: 'Latency Distribution',
            type: 'boxPlot',
            data: {scan_boxplot_data}
        }}
    ],
    chart: {{
        type: 'boxPlot',
        height: {scan_chart_height_bar},
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }},
        events: {{
            mounted: apexAttachBoxplotCategoryTitles,
            updated: apexAttachBoxplotCategoryTitles
        }}
    }},
    theme: {{ mode: 'dark' }},
    stroke: {{
        show: true,
        width: 2.25,
        colors: ['#faf7ff'],
        lineCap: 'round',
        lineJoin: 'round'
    }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4,
        padding: {{
            left: 24,
            right: 12
        }}
    }},
    plotOptions: {{
        bar: {{
            horizontal: true,
            barHeight: '44%'
        }},
        boxPlot: {{
            colors: {{
                upper: '#d4c4ff',
                lower: '#6d3dff'
            }}
        }}
    }},
    colors: ['#c9acff'],
    dataLabels: {{ enabled: false }},
    legend: {{ show: false }},
    xaxis: {{
        title: {{ text: 'Latency (milliseconds)' }},
        labels: {{
            formatter: function(val) {{
                return (Number(val) / 1000).toFixed(2);
            }}
        }}
    }},
    yaxis: {{
        title: {{ text: 'Scan' }},
        labels: {{
            trim: false,
            maxWidth: 520,
            style: {{
                fontSize: '11px'
            }}
        }},
        tooltip: {{
            enabled: true
        }}
    }},
    tooltip: {{
        shared: false,
        custom: function(options) {{
            const dataPointIndex = options.dataPointIndex;
            const w = options.w;
            const data = w.globals.initialSeries[0].data[dataPointIndex];

            const min = data.y[0];
            const q1 = data.y[1];
            const median = data.y[2];
            const q3 = data.y[3];
            const max = data.y[4];
            const ops = scanOpsLookup[data.x] || 0;

            return '<div class="apexcharts-tooltip-box" style="min-width: 200px;">' +
                '<div style="font-weight: 600; margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid ' + SD.line + '; color: ' + SD.text + ';">' + data.x + '</div>' +
                '<div style="margin-bottom: 4px;"><strong>Latency Distribution:</strong></div>' +
                '<div style="margin-left: 10px;">Min: ' + (min / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Q1: ' + (q1 / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Median: ' + (median / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Q3: ' + (q3 / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Max: ' + (max / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid ' + SD.line + ';"><strong>Throughput:</strong> ' + formatNumber(ops) + ' ops/s</div>' +
                '</div>';
        }}
    }}
}});
scanLatencyChart.render();

// Batch Throughput Chart
var batchThroughputChart = new ApexCharts(document.querySelector("#batchThroughputChart"), {{
    series: [{{
        name: 'Operations/Second',
        data: {batch_data}
    }}],
    chart: {{
        type: 'bar',
        height: 350,
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }},
        events: {{
            mounted: apexAttachFullCategoryTitles,
            updated: apexAttachFullCategoryTitles
        }}
    }},
    theme: {{ mode: 'dark' }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4
    }},
    plotOptions: {{
        bar: {{
            horizontal: true,
            borderRadius: 4
        }}
    }},
    colors: ['#ff9eed'],
    dataLabels: {{ enabled: false }},
    legend: {{ show: false }},
    xaxis: {{
        categories: {batch_labels},
        title: {{ text: 'Operations/Second' }},
        labels: {{
            formatter: function(val) {{
                return formatNumber(val);
            }}
        }},
        tooltip: {{
            enabled: true
        }}
    }},
    tooltip: {{
        y: {{
            formatter: function(val) {{
                return formatNumber(val) + ' ops/s';
            }}
        }}
    }}
}});
batchThroughputChart.render();

// Batch Latency Distribution Chart
// Create ops lookup for batch tooltips
var batchOpsLookup = {batch_ops_lookup};

var batchLatencyChart = new ApexCharts(document.querySelector("#batchLatencyChart"), {{
    series: [
        {{
            name: 'Latency Distribution',
            type: 'boxPlot',
            data: {batch_boxplot_data}
        }}
    ],
    chart: {{
        type: 'boxPlot',
        height: 350,
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }},
        events: {{
            mounted: apexAttachBoxplotCategoryTitles,
            updated: apexAttachBoxplotCategoryTitles
        }}
    }},
    theme: {{ mode: 'dark' }},
    stroke: {{
        show: true,
        width: 2.25,
        colors: ['#fff5fc'],
        lineCap: 'round',
        lineJoin: 'round'
    }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4
    }},
    plotOptions: {{
        bar: {{
            horizontal: true,
            barHeight: '60%'
        }},
        boxPlot: {{
            colors: {{
                upper: '#ffc8ef',
                lower: '#e93dcd'
            }}
        }}
    }},
    colors: ['#ff9eed'],
    dataLabels: {{ enabled: false }},
    legend: {{ show: false }},
    xaxis: {{
        title: {{ text: 'Latency (microseconds)' }},
        labels: {{
            formatter: function(val) {{
                return formatNumber(val);
            }}
        }}
    }},
    yaxis: {{
        title: {{ text: 'Batch Type' }},
        tooltip: {{
            enabled: true
        }}
    }},
    tooltip: {{
        shared: false,
        custom: function(options) {{
            const dataPointIndex = options.dataPointIndex;
            const w = options.w;
            const data = w.globals.initialSeries[0].data[dataPointIndex];

            const min = data.y[0];
            const q1 = data.y[1];
            const median = data.y[2];
            const q3 = data.y[3];
            const max = data.y[4];
            const ops = batchOpsLookup[data.x] || 0;

            return '<div class="apexcharts-tooltip-box" style="min-width: 200px;">' +
                '<div style="font-weight: 600; margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid ' + SD.line + '; color: ' + SD.text + ';">' + data.x + '</div>' +
                '<div style="margin-bottom: 4px;"><strong>Latency Distribution:</strong></div>' +
                '<div style="margin-left: 10px;">Min: ' + (min / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Q1: ' + (q1 / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Median: ' + (median / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Q3: ' + (q3 / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Max: ' + (max / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid ' + SD.line + ';"><strong>Throughput:</strong> ' + formatNumber(ops) + ' ops/s</div>' +
                '</div>';
        }}
    }}
}});
batchLatencyChart.render();

// Batch Percentile Comparison Chart
var batchPercentileChart = new ApexCharts(document.querySelector("#batchPercentileChart"), {{
    series: {batch_percentile_series},
    chart: {{
        type: 'line',
        height: 350,
        fontFamily: '"Inter", system-ui, sans-serif',
        foreColor: SD.muted,
        background: SD.surface,
        toolbar: {{ show: false }}
    }},
    theme: {{ mode: 'dark' }},
    grid: {{
        borderColor: SD.surfaceBorder,
        strokeDashArray: 4
    }},
    stroke: {{
        width: 3,
        curve: 'smooth'
    }},
    colors: ['#d255fe', '#7c5cfc', '#c471f5', '#651ddd', '#a78bfa', '#9b7dfb', '#e879f9', '#c084fc'],
    dataLabels: {{ enabled: false }},
    legend: {{
        position: 'top',
        horizontalAlign: 'left',
        labels: {{ colors: SD.text }},
        markers: {{
            strokeColors: SD.surface
        }}
    }},
    xaxis: {{
        categories: ['Min', 'P01', 'P25', 'P50', 'P75', 'P95', 'P99', 'Max']
    }},
    yaxis: {{
        title: {{ text: 'Latency (microseconds)' }},
        labels: {{
            formatter: function(val) {{
                return formatNumber(val);
            }}
        }}
    }},
    tooltip: {{
        shared: false,
        intersect: false,
        y: {{
            formatter: function(val) {{
                return (val / 1000).toFixed(3) + ' ms';
            }}
        }}
    }}
}});
batchPercentileChart.render();
"##,
		ops_labels = get_ops_labels(result),
		ops_data = get_ops_data(result),
		latency_labels = get_ops_labels(result),
		latency_data = get_latency_data(result),
		percentile_series = get_percentile_series(result),
		resource_labels = get_ops_labels(result),
		cpu_data = get_cpu_data(result),
		memory_data = get_memory_data(result),
		disk_labels = get_ops_labels(result),
		disk_writes = get_disk_writes(result),
		disk_reads = get_disk_reads(result),
		scan_boxplot_data = scan_boxplot_data,
		scan_ops_lookup = scan_ops_lookup,
		scan_chart_height_bar = scan_chart_height_bar,
		batch_labels = get_batch_labels(result),
		batch_data = get_batch_data(result),
		batch_boxplot_data = get_batch_boxplot_data(result),
		batch_ops_lookup = get_batch_ops_lookup(result),
		batch_percentile_series = get_batch_percentile_series(result),
	)
}

fn get_ops_labels(result: &BenchmarkResult) -> String {
	let mut labels = vec![];
	if result.creates.is_some() {
		labels.push("\"Create\"");
	}
	if result.reads.is_some() {
		labels.push("\"Read\"");
	}
	if result.updates.is_some() {
		labels.push("\"Update\"");
	}
	if result.deletes.is_some() {
		labels.push("\"Delete\"");
	}
	format!("[{}]", labels.join(", "))
}

fn get_ops_data(result: &BenchmarkResult) -> String {
	let mut data = vec![];
	if let Some(r) = &result.creates {
		data.push(format!("{:.2}", r.ops()));
	}
	if let Some(r) = &result.reads {
		data.push(format!("{:.2}", r.ops()));
	}
	if let Some(r) = &result.updates {
		data.push(format!("{:.2}", r.ops()));
	}
	if let Some(r) = &result.deletes {
		data.push(format!("{:.2}", r.ops()));
	}
	format!("[{}]", data.join(", "))
}

fn get_latency_data(result: &BenchmarkResult) -> String {
	let mut data = vec![];
	if let Some(r) = &result.creates {
		data.push(format!("{:.2}", r.mean() / 1000.0));
	}
	if let Some(r) = &result.reads {
		data.push(format!("{:.2}", r.mean() / 1000.0));
	}
	if let Some(r) = &result.updates {
		data.push(format!("{:.2}", r.mean() / 1000.0));
	}
	if let Some(r) = &result.deletes {
		data.push(format!("{:.2}", r.mean() / 1000.0));
	}
	format!("[{}]", data.join(", "))
}

fn get_percentile_series(result: &BenchmarkResult) -> String {
	let mut series = vec![];
	let labels = ["Create", "Read", "Update", "Delete"];
	let ops = [&result.creates, &result.reads, &result.updates, &result.deletes];

	for (i, op) in ops.iter().enumerate() {
		if let Some(r) = op {
			series.push(format!(
				r##"{{
                        name: '{}',
                        data: [{}, {}, {}, {}, {}, {}, {}, {}]
                    }}"##,
				labels[i],
				r.min(),
				r.q01(),
				r.q25(),
				r.q50(),
				r.q75(),
				r.q95(),
				r.q99(),
				r.max()
			));
		}
	}

	format!("[{}]", series.join(", "))
}

fn get_cpu_data(result: &BenchmarkResult) -> String {
	let mut data = vec![];
	if let Some(r) = &result.creates {
		data.push(format!("{:.2}", r.cpu_usage()));
	}
	if let Some(r) = &result.reads {
		data.push(format!("{:.2}", r.cpu_usage()));
	}
	if let Some(r) = &result.updates {
		data.push(format!("{:.2}", r.cpu_usage()));
	}
	if let Some(r) = &result.deletes {
		data.push(format!("{:.2}", r.cpu_usage()));
	}
	format!("[{}]", data.join(", "))
}

fn get_memory_data(result: &BenchmarkResult) -> String {
	let mut data = vec![];
	if let Some(r) = &result.creates {
		data.push(format!("{:.2}", r.used_memory() as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.reads {
		data.push(format!("{:.2}", r.used_memory() as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.updates {
		data.push(format!("{:.2}", r.used_memory() as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.deletes {
		data.push(format!("{:.2}", r.used_memory() as f64 / 1024.0 / 1024.0));
	}
	format!("[{}]", data.join(", "))
}

fn get_disk_writes(result: &BenchmarkResult) -> String {
	let mut data = vec![];
	if let Some(r) = &result.creates {
		data.push(format!("{:.2}", r.disk_usage().total_written_bytes as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.reads {
		data.push(format!("{:.2}", r.disk_usage().total_written_bytes as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.updates {
		data.push(format!("{:.2}", r.disk_usage().total_written_bytes as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.deletes {
		data.push(format!("{:.2}", r.disk_usage().total_written_bytes as f64 / 1024.0 / 1024.0));
	}
	format!("[{}]", data.join(", "))
}

fn get_disk_reads(result: &BenchmarkResult) -> String {
	let mut data = vec![];
	if let Some(r) = &result.creates {
		data.push(format!("{:.2}", r.disk_usage().total_read_bytes as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.reads {
		data.push(format!("{:.2}", r.disk_usage().total_read_bytes as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.updates {
		data.push(format!("{:.2}", r.disk_usage().total_read_bytes as f64 / 1024.0 / 1024.0));
	}
	if let Some(r) = &result.deletes {
		data.push(format!("{:.2}", r.disk_usage().total_read_bytes as f64 / 1024.0 / 1024.0));
	}
	format!("[{}]", data.join(", "))
}

fn scan_chart_rows(result: &BenchmarkResult) -> Vec<(String, &OperationResult)> {
	let mut v = Vec::new();
	for scan in &result.scans {
		for run in &scan.runs {
			if let Some(ref r) = run.result {
				v.push((run.chart_label(scan.name.as_str()), r));
			}
		}
	}
	v
}

fn scan_chart_bar_height(row_count: usize) -> u32 {
	const ROW_PX: u32 = 23;
	const PAD: u32 = 120;
	const MIN: u32 = 320;
	const MAX: u32 = 8000;
	MIN.max((row_count as u32).saturating_mul(ROW_PX).saturating_add(PAD)).min(MAX)
}

fn scan_boxplot_json(rows: &[(String, &OperationResult)]) -> String {
	let data: Vec<Value> = rows
		.iter()
		.map(|(name, r)| {
			json!({
				"x": name,
				"y": [r.min(), r.q25(), r.q50(), r.q75(), r.max()],
			})
		})
		.collect();
	serde_json::to_string(&data).expect("scan boxplot json")
}

fn scan_ops_lookup_json(rows: &[(String, &OperationResult)]) -> String {
	let mut m = Map::new();
	for (name, r) in rows {
		m.insert(name.clone(), json!(r.ops()));
	}
	serde_json::to_string(&Value::Object(m)).expect("scan ops lookup json")
}

fn get_batch_labels(result: &BenchmarkResult) -> String {
	let labels: Vec<String> = result
		.batches
		.iter()
		.filter_map(|(name, _, _, result)| result.as_ref().map(|_| format!("\"{}\"", name)))
		.collect();
	format!("[{}]", labels.join(", "))
}

fn get_batch_data(result: &BenchmarkResult) -> String {
	let data: Vec<String> = result
		.batches
		.iter()
		.filter_map(|(_, _, _, result)| result.as_ref().map(|r| format!("{:.2}", r.ops())))
		.collect();
	format!("[{}]", data.join(", "))
}

fn get_batch_boxplot_data(result: &BenchmarkResult) -> String {
	let data: Vec<String> = result
		.batches
		.iter()
		.filter_map(|(name, _, _, result)| {
			result.as_ref().map(|r| {
				format!(
					r##"{{ x: '{}', y: [{}, {}, {}, {}, {}] }}"##,
					name,
					r.min(),
					r.q25(),
					r.q50(),
					r.q75(),
					r.max()
				)
			})
		})
		.collect();
	format!("[{}]", data.join(", "))
}

fn get_batch_ops_lookup(result: &BenchmarkResult) -> String {
	let data: Vec<String> = result
		.batches
		.iter()
		.filter_map(|(name, _, _, result)| {
			result.as_ref().map(|r| format!(r##"'{}': {:.2}"##, name, r.ops()))
		})
		.collect();
	format!("{{{}}}", data.join(", "))
}

fn get_batch_percentile_series(result: &BenchmarkResult) -> String {
	let mut series = vec![];

	for (name, _, _, batch_result) in &result.batches {
		if let Some(r) = batch_result {
			series.push(format!(
				r##"{{
                        name: '{}',
                        data: [{}, {}, {}, {}, {}, {}, {}, {}]
                    }}"##,
				name,
				r.min(),
				r.q01(),
				r.q25(),
				r.q50(),
				r.q75(),
				r.q95(),
				r.q99(),
				r.max()
			));
		}
	}

	format!("[{}]", series.join(", "))
}
