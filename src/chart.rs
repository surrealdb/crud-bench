use crate::result::BenchmarkResult;

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
        .system-info {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 25px;
            margin-bottom: 30px;
            border: 1px solid #e0e0e0;
        }}
        .system-info-title {{
            font-size: 1.4em;
            font-weight: 600;
            margin-bottom: 15px;
            color: #333;
        }}
        .system-info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 15px;
        }}
        .system-info-item {{
            display: flex;
            align-items: center;
            padding: 12px;
            background: white;
            border-radius: 6px;
            border: 1px solid #e8e8e8;
        }}
        .system-info-label {{
            font-weight: 600;
            color: #666;
            margin-right: 8px;
            min-width: 120px;
        }}
        .system-info-value {{
            color: #333;
            font-family: 'SF Mono', Monaco, 'Cascadia Code', 'Roboto Mono', Consolas, 'Courier New', monospace;
            font-size: 0.95em;
        }}
        .apexcharts-tooltip-box {{
            padding: 10px;
            background: white;
            border: 1px solid #e0e0e0;
            border-radius: 4px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .apexcharts-tooltip-box > div {{
            padding: 3px 0;
        }}
        .apexcharts-tooltip-box strong {{
            color: #333;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Benchmark Results</h1>
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

            <div class="chart-container">
                <div class="chart-title">Scan throughput</div>
                <div id="scanThroughputChart"></div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Scan latency distribution</div>
                <div id="scanLatencyChart"></div>
            </div>

            <div class="chart-container full-width">
                <div class="chart-title">Scan percentile comparison</div>
                <div id="scanPercentileChart"></div>
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

// Scan Throughput Chart
var scanThroughputChart = new ApexCharts(document.querySelector("#scanThroughputChart"), {{
    series: [{{
        name: 'Operations/Second',
        data: {scan_ops_array}
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
            borderRadius: 4,
            distributed: true
        }}
    }},
    colors: ['#9600FF', '#FF00A0', '#C000FF', '#FF33B8', '#9600FF', '#FF00A0', '#C000FF'],
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
scanThroughputChart.render();

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
        height: 350,
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    plotOptions: {{
        bar: {{
            horizontal: true,
            barHeight: '60%'
        }},
        boxPlot: {{
            colors: {{
                upper: '#9600FF',
                lower: '#C97FFF'
            }}
        }}
    }},
    colors: ['#9600FF'],
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
        title: {{ text: 'Scan Type' }}
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
                '<div style="font-weight: 600; margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid #e0e0e0;">' + data.x + '</div>' +
                '<div style="margin-bottom: 4px;"><strong>Latency Distribution:</strong></div>' +
                '<div style="margin-left: 10px;">Min: ' + (min / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Q1: ' + (q1 / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Median: ' + (median / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Q3: ' + (q3 / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Max: ' + (max / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #e0e0e0;"><strong>Throughput:</strong> ' + formatNumber(ops) + ' ops/s</div>' +
                '</div>';
        }}
    }}
}});
scanLatencyChart.render();

// Scan Percentile Comparison Chart
var scanPercentileChart = new ApexCharts(document.querySelector("#scanPercentileChart"), {{
    series: {scan_percentile_series},
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
    colors: ['#9600FF', '#FF00A0', '#C000FF', '#FF33B8', '#B84FFF', '#FF66CC', '#DA9FFF'],
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
                return (val / 1000).toFixed(3) + ' ms';
            }}
        }}
    }}
}});
scanPercentileChart.render();

// Batch Throughput Chart
var batchThroughputChart = new ApexCharts(document.querySelector("#batchThroughputChart"), {{
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
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    plotOptions: {{
        bar: {{
            horizontal: true,
            barHeight: '60%'
        }},
        boxPlot: {{
            colors: {{
                upper: '#FF00A0',
                lower: '#FF99D6'
            }}
        }}
    }},
    colors: ['#FF00A0'],
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
        title: {{ text: 'Batch Type' }}
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
                '<div style="font-weight: 600; margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid #e0e0e0;">' + data.x + '</div>' +
                '<div style="margin-bottom: 4px;"><strong>Latency Distribution:</strong></div>' +
                '<div style="margin-left: 10px;">Min: ' + (min / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Q1: ' + (q1 / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Median: ' + (median / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Q3: ' + (q3 / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-left: 10px;">Max: ' + (max / 1000).toFixed(2) + ' ms</div>' +
                '<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #e0e0e0;"><strong>Throughput:</strong> ' + formatNumber(ops) + ' ops/s</div>' +
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
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        toolbar: {{ show: false }}
    }},
    stroke: {{
        width: 3,
        curve: 'smooth'
    }},
    colors: ['#FF00A0', '#9600FF', '#FF33B8', '#C000FF', '#FF66CC', '#B84FFF', '#FF99D6', '#DA9FFF'],
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
		scan_labels = get_scan_labels(result),
		scan_ops_array = get_scan_ops_array(result),
		scan_boxplot_data = get_scan_boxplot_data(result),
		scan_ops_lookup = get_scan_ops_lookup(result),
		scan_percentile_series = get_scan_percentile_series(result),
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

fn get_scan_ops_array(result: &BenchmarkResult) -> String {
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

fn get_scan_boxplot_data(result: &BenchmarkResult) -> String {
	let data: Vec<String> = result
		.scans
		.iter()
		.filter_map(|scan| {
			scan.without_index.as_ref().or(scan.with_index.as_ref()).map(|r| {
				format!(
					r##"{{ x: '{}', y: [{}, {}, {}, {}, {}] }}"##,
					scan.name,
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

fn get_scan_ops_lookup(result: &BenchmarkResult) -> String {
	let data: Vec<String> = result
		.scans
		.iter()
		.filter_map(|scan| {
			scan.without_index
				.as_ref()
				.or(scan.with_index.as_ref())
				.map(|r| format!(r##"'{}': {:.2}"##, scan.name, r.ops()))
		})
		.collect();
	format!("{{{}}}", data.join(", "))
}

fn get_scan_percentile_series(result: &BenchmarkResult) -> String {
	let mut series = vec![];

	for scan in &result.scans {
		if let Some(r) = scan.without_index.as_ref().or(scan.with_index.as_ref()) {
			series.push(format!(
				r##"{{
                        name: '{}',
                        data: [{}, {}, {}, {}, {}, {}, {}, {}]
                    }}"##,
				scan.name,
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
