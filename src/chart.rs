use crate::result::BenchmarkResult;

/// Generate an HTML file with interactive charts for a single benchmark result
pub(crate) fn generate_html(result: &BenchmarkResult, database_name: &str) -> String {
	format!(
		r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CRUD Benchmark Results - {database_name}</title>
    <script src="https://cdn.jsdelivr.net/npm/apexcharts@3.45.0/dist/apexcharts.min.js"></script>
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
            background: linear-gradient(135deg, #9600FF 0%, #7000CC 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-card.blue {{ background: linear-gradient(135deg, #9600FF 0%, #7000CC 100%); }}
        .stat-card.green {{ background: linear-gradient(135deg, #FF00A0 0%, #CC0080 100%); }}
        .stat-card.orange {{ background: linear-gradient(135deg, #C000FF 0%, #9600FF 100%); }}
        .stat-card.red {{ background: linear-gradient(135deg, #FF33B8 0%, #FF00A0 100%); }}
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
        .chart {{
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
                <div id="opsChart"></div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Latency Distribution (ms)</div>
                <div id="latencyChart"></div>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Percentile Comparison</div>
                <div id="percentileChart"></div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Resource Usage</div>
                <div id="resourceChart"></div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Disk I/O</div>
                <div id="diskChart"></div>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Scan Performance</div>
                <div id="scanChart"></div>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Batch Operations</div>
                <div id="batchChart"></div>
            </div>
        </div>
    </div>

    <script>
        {chart_scripts}
    </script>
</body>
</html>"##,
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

fn generate_chart_scripts(result: &BenchmarkResult) -> String {
	format!(
		r##"
// Helper function to format numbers with commas
function formatNumber(num) {{
    return num.toFixed(0).replace(/\B(?=(\d{{3}})+(?!\d))/g, ",");
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
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    plotOptions: {{
        bar: {{
            distributed: true,
            borderRadius: 4
        }}
    }},
    colors: ['#9600FF', '#FF00A0', '#C000FF', '#FF33B8'],
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
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    plotOptions: {{
        bar: {{
            distributed: true,
            borderRadius: 4
        }}
    }},
    colors: ['#9600FF', '#B84FFF', '#C97FFF', '#DA9FFF'],
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
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    stroke: {{
        width: 3,
        curve: 'smooth'
    }},
    colors: ['#9600FF', '#FF00A0', '#C000FF', '#FF33B8'],
    dataLabels: {{ enabled: false }},
    legend: {{
        position: 'top',
        horizontalAlign: 'left'
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
        y: {{
            formatter: function(val) {{
                return formatNumber(val / 1000) + ' ms';
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
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    stroke: {{
        width: [0, 0]
    }},
    plotOptions: {{
        bar: {{
            borderRadius: 4
        }}
    }},
    colors: ['#FF00A0', '#9600FF'],
    dataLabels: {{ enabled: false }},
    legend: {{
        position: 'top',
        horizontalAlign: 'left'
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
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    plotOptions: {{
        bar: {{
            borderRadius: 4
        }}
    }},
    colors: ['#FF00A0', '#9600FF'],
    dataLabels: {{ enabled: false }},
    legend: {{
        position: 'top',
        horizontalAlign: 'left'
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

// Scan Performance Chart
var scanChart = new ApexCharts(document.querySelector("#scanChart"), {{
    series: [{{
        name: 'Operations/Second',
        data: {scan_data}
    }}],
    chart: {{
        type: 'bar',
        height: 350,
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    plotOptions: {{
        bar: {{
            horizontal: true,
            borderRadius: 4
        }}
    }},
    colors: ['#9600FF'],
    dataLabels: {{ enabled: false }},
    legend: {{ show: false }},
    xaxis: {{
        categories: {scan_labels},
        title: {{ text: 'Operations/Second' }},
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
scanChart.render();

// Batch Operations Chart
var batchChart = new ApexCharts(document.querySelector("#batchChart"), {{
    series: [{{
        name: 'Operations/Second',
        data: {batch_data}
    }}],
    chart: {{
        type: 'bar',
        height: 350,
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    plotOptions: {{
        bar: {{
            horizontal: true,
            borderRadius: 4
        }}
    }},
    colors: ['#FF00A0'],
    dataLabels: {{ enabled: false }},
    legend: {{ show: false }},
    xaxis: {{
        categories: {batch_labels},
        title: {{ text: 'Operations/Second' }},
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
batchChart.render();
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
