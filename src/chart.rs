use crate::result::BenchmarkResult;

/// Generate an HTML file with interactive charts for a single benchmark result
pub(crate) fn generate_html(result: &BenchmarkResult, database_name: &str) -> String {
	format!(
		r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CRUD Benchmark Results - {database_name}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            margin-bottom: 10px;
            font-size: 2em;
        }}
        .subtitle {{
            color: #666;
            margin-bottom: 30px;
            font-size: 1.1em;
        }}
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
            gap: 30px;
            margin-bottom: 30px;
        }}
        .chart-container {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            border: 1px solid #e0e0e0;
        }}
        .chart-title {{
            font-size: 1.3em;
            font-weight: 600;
            margin-bottom: 15px;
            color: #444;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-card.green {{ background: linear-gradient(135deg, #10B981 0%, #059669 100%); }}
        .stat-card.blue {{ background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%); }}
        .stat-card.orange {{ background: linear-gradient(135deg, #F59E0B 0%, #D97706 100%); }}
        .stat-card.red {{ background: linear-gradient(135deg, #EF4444 0%, #DC2626 100%); }}
        .stat-label {{
            font-size: 0.9em;
            opacity: 0.9;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 2em;
            font-weight: bold;
        }}
        .stat-unit {{
            font-size: 0.7em;
            opacity: 0.8;
        }}
        canvas {{
            max-height: 400px;
        }}
        .full-width {{
            grid-column: 1 / -1;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Benchmark Results</h1>
        <div class="subtitle">{database_name}</div>

        <div class="stats-grid">
            {stats_cards}
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <div class="chart-title">Operations Per Second</div>
                <canvas id="opsChart"></canvas>
            </div>

            <div class="chart-container">
                <div class="chart-title">Latency Distribution (ms)</div>
                <canvas id="latencyChart"></canvas>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Percentile Comparison</div>
                <canvas id="percentileChart"></canvas>
            </div>

            <div class="chart-container">
                <div class="chart-title">Resource Usage</div>
                <canvas id="resourceChart"></canvas>
            </div>

            <div class="chart-container">
                <div class="chart-title">Disk I/O</div>
                <canvas id="diskChart"></canvas>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Scan Performance</div>
                <canvas id="scanChart"></canvas>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Batch Operations</div>
                <canvas id="batchChart"></canvas>
            </div>
        </div>
    </div>

    <script>
        Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
        Chart.defaults.color = '#666';

        {chart_scripts}
    </script>
</body>
</html>"#,
		database_name = database_name,
		stats_cards = generate_stat_cards(result),
		chart_scripts = generate_chart_scripts(result)
	)
}

fn format_number_with_commas(num: f64) -> String {
	let num_str = format!("{:.0}", num);
	let mut result = String::new();
	let chars: Vec<char> = num_str.chars().collect();
	let len = chars.len();

	for (i, ch) in chars.iter().enumerate() {
		if i > 0 && (len - i) % 3 == 0 {
			result.push(',');
		}
		result.push(*ch);
	}

	result
}

fn generate_stat_cards(result: &BenchmarkResult) -> String {
	let mut cards = String::new();

	if let Some(creates) = &result.creates {
		cards.push_str(&format!(
			r#"<div class="stat-card blue">
                    <div class="stat-label">Create Operations</div>
                    <div class="stat-value">{}<span class="stat-unit"> ops/s</span></div>
                </div>"#,
			format_number_with_commas(creates.ops())
		));
	}

	if let Some(reads) = &result.reads {
		cards.push_str(&format!(
			r#"<div class="stat-card green">
                    <div class="stat-label">Read Operations</div>
                    <div class="stat-value">{}<span class="stat-unit"> ops/s</span></div>
                </div>"#,
			format_number_with_commas(reads.ops())
		));
	}

	if let Some(updates) = &result.updates {
		cards.push_str(&format!(
			r#"<div class="stat-card orange">
                    <div class="stat-label">Update Operations</div>
                    <div class="stat-value">{}<span class="stat-unit"> ops/s</span></div>
                </div>"#,
			format_number_with_commas(updates.ops())
		));
	}

	if let Some(deletes) = &result.deletes {
		cards.push_str(&format!(
			r#"<div class="stat-card red">
                    <div class="stat-label">Delete Operations</div>
                    <div class="stat-value">{}<span class="stat-unit"> ops/s</span></div>
                </div>"#,
			format_number_with_commas(deletes.ops())
		));
	}

	cards
}

fn generate_chart_scripts(result: &BenchmarkResult) -> String {
	format!(
		r#"
// Operations Per Second Chart
new Chart(document.getElementById('opsChart'), {{
    type: 'bar',
    data: {{
        labels: {ops_labels},
        datasets: [{{
            label: 'Operations/Second',
            data: {ops_data},
            backgroundColor: [
                'rgba(59, 130, 246, 0.8)',
                'rgba(16, 185, 129, 0.8)',
                'rgba(245, 158, 11, 0.8)',
                'rgba(239, 68, 68, 0.8)',
            ],
            borderColor: [
                'rgb(59, 130, 246)',
                'rgb(16, 185, 129)',
                'rgb(245, 158, 11)',
                'rgb(239, 68, 68)',
            ],
            borderWidth: 2
        }}]
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: true,
        plugins: {{
            legend: {{ display: false }},
            tooltip: {{
                callbacks: {{
                    label: function(context) {{
                        return context.parsed.y.toFixed(2) + ' ops/s';
                    }}
                }}
            }}
        }},
        scales: {{
            y: {{
                beginAtZero: true,
                title: {{
                    display: true,
                    text: 'Operations per Second'
                }}
            }}
        }}
    }}
}});

// Latency Distribution Chart
new Chart(document.getElementById('latencyChart'), {{
    type: 'bar',
    data: {{
        labels: {latency_labels},
        datasets: [{{
            label: 'Mean Latency (ms)',
            data: {latency_data},
            backgroundColor: 'rgba(139, 92, 246, 0.6)',
            borderColor: 'rgb(139, 92, 246)',
            borderWidth: 2
        }}]
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: true,
        plugins: {{
            legend: {{ display: true }},
            tooltip: {{
                callbacks: {{
                    label: function(context) {{
                        return context.parsed.y.toFixed(2) + ' ms';
                    }}
                }}
            }}
        }},
        scales: {{
            y: {{
                beginAtZero: true,
                title: {{
                    display: true,
                    text: 'Latency (milliseconds)'
                }}
            }}
        }}
    }}
}});

// Percentile Comparison Chart
new Chart(document.getElementById('percentileChart'), {{
    type: 'line',
    data: {{
        labels: ['Min', 'P01', 'P25', 'P50', 'P75', 'P95', 'P99', 'Max'],
        datasets: {percentile_datasets}
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: true,
        plugins: {{
            legend: {{ display: true, position: 'top' }},
            tooltip: {{
                callbacks: {{
                    label: function(context) {{
                        return context.dataset.label + ': ' + (context.parsed.y / 1000).toFixed(2) + ' ms';
                    }}
                }}
            }}
        }},
        scales: {{
            y: {{
                beginAtZero: true,
                title: {{
                    display: true,
                    text: 'Latency (microseconds)'
                }}
            }}
        }}
    }}
}});

// Resource Usage Chart
new Chart(document.getElementById('resourceChart'), {{
    type: 'bar',
    data: {{
        labels: {resource_labels},
        datasets: [
            {{
                label: 'CPU Usage (%)',
                data: {cpu_data},
                backgroundColor: 'rgba(245, 158, 11, 0.6)',
                borderColor: 'rgb(245, 158, 11)',
                borderWidth: 2,
                yAxisID: 'y'
            }},
            {{
                label: 'Memory (MB)',
                data: {memory_data},
                backgroundColor: 'rgba(139, 92, 246, 0.6)',
                borderColor: 'rgb(139, 92, 246)',
                borderWidth: 2,
                yAxisID: 'y1'
            }}
        ]
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: true,
        plugins: {{
            legend: {{ display: true }}
        }},
        scales: {{
            y: {{
                type: 'linear',
                display: true,
                position: 'left',
                title: {{ display: true, text: 'CPU (%)' }}
            }},
            y1: {{
                type: 'linear',
                display: true,
                position: 'right',
                title: {{ display: true, text: 'Memory (MB)' }},
                grid: {{ drawOnChartArea: false }}
            }}
        }}
    }}
}});

// Disk I/O Chart
new Chart(document.getElementById('diskChart'), {{
    type: 'bar',
    data: {{
        labels: {disk_labels},
        datasets: [
            {{
                label: 'Writes (MB)',
                data: {disk_writes},
                backgroundColor: 'rgba(239, 68, 68, 0.6)',
                borderColor: 'rgb(239, 68, 68)',
                borderWidth: 2
            }},
            {{
                label: 'Reads (MB)',
                data: {disk_reads},
                backgroundColor: 'rgba(16, 185, 129, 0.6)',
                borderColor: 'rgb(16, 185, 129)',
                borderWidth: 2
            }}
        ]
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: true,
        plugins: {{
            legend: {{ display: true }}
        }},
        scales: {{
            y: {{
                beginAtZero: true,
                title: {{ display: true, text: 'Data (MB)' }}
            }}
        }}
    }}
}});

// Scan Performance Chart
new Chart(document.getElementById('scanChart'), {{
    type: 'bar',
    data: {{
        labels: {scan_labels},
        datasets: [{{
            label: 'Operations/Second',
            data: {scan_data},
            backgroundColor: 'rgba(59, 130, 246, 0.6)',
            borderColor: 'rgb(59, 130, 246)',
            borderWidth: 2
        }}]
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: true,
        indexAxis: 'y',
        plugins: {{
            legend: {{ display: false }}
        }},
        scales: {{
            x: {{
                beginAtZero: true,
                title: {{ display: true, text: 'Operations/Second' }}
            }}
        }}
    }}
}});

// Batch Operations Chart
new Chart(document.getElementById('batchChart'), {{
    type: 'bar',
    data: {{
        labels: {batch_labels},
        datasets: [{{
            label: 'Operations/Second',
            data: {batch_data},
            backgroundColor: 'rgba(16, 185, 129, 0.6)',
            borderColor: 'rgb(16, 185, 129)',
            borderWidth: 2
        }}]
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: true,
        indexAxis: 'y',
        plugins: {{
            legend: {{ display: false }}
        }},
        scales: {{
            x: {{
                beginAtZero: true,
                title: {{ display: true, text: 'Operations/Second' }}
            }}
        }}
    }}
}});
"#,
		ops_labels = get_ops_labels(result),
		ops_data = get_ops_data(result),
		latency_labels = get_ops_labels(result),
		latency_data = get_latency_data(result),
		percentile_datasets = get_percentile_datasets(result),
		resource_labels = get_ops_labels(result),
		cpu_data = get_cpu_data(result),
		memory_data = get_memory_data(result),
		disk_labels = get_ops_labels(result),
		disk_writes = get_disk_writes(result),
		disk_reads = get_disk_reads(result),
		scan_labels = get_scan_labels(result),
		scan_data = get_scan_data(result),
		batch_labels = get_batch_labels(result),
		batch_data = get_batch_data(result),
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

fn get_percentile_datasets(result: &BenchmarkResult) -> String {
	let mut datasets = vec![];
	let colors = [
		("59, 130, 246", "Create"),
		("16, 185, 129", "Read"),
		("245, 158, 11", "Update"),
		("239, 68, 68", "Delete"),
	];

	let ops = [&result.creates, &result.reads, &result.updates, &result.deletes];

	for (i, op) in ops.iter().enumerate() {
		if let Some(r) = op {
			let (color, label) = colors[i];
			datasets.push(format!(
				r#"{{
                        label: '{}',
                        data: [{}, {}, {}, {}, {}, {}, {}, {}],
                        borderColor: 'rgb({})',
                        backgroundColor: 'rgba({}, 0.1)',
                        borderWidth: 2,
                        fill: false,
                        tension: 0.4
                    }}"#,
				label,
				r.min(),
				r.q01(),
				r.q25(),
				r.q50(),
				r.q75(),
				r.q95(),
				r.q99(),
				r.max(),
				color,
				color
			));
		}
	}

	format!("[{}]", datasets.join(", "))
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

fn get_scan_labels(result: &BenchmarkResult) -> String {
	let labels: Vec<String> = result
		.scans
		.iter()
		.filter_map(|scan| {
			scan.without_index
				.as_ref()
				.or(scan.with_index.as_ref())
				.map(|_| format!("\"{}\"", scan.name))
		})
		.collect();
	format!("[{}]", labels.join(", "))
}

fn get_scan_data(result: &BenchmarkResult) -> String {
	let data: Vec<String> = result
		.scans
		.iter()
		.filter_map(|scan| {
			scan.without_index
				.as_ref()
				.or(scan.with_index.as_ref())
				.map(|r| format!("{:.2}", r.ops()))
		})
		.collect();
	format!("[{}]", data.join(", "))
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
