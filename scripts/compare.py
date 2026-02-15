#!/usr/bin/env python3
"""
compare.py - Compare benchmark CSV results with charts and summaries.

Replaces compare.sh and compare-multi.sh with richer terminal output
and an interactive HTML report using Chart.js.

Usage:
    python3 scripts/compare.py \\
        --title "Memory: v3 vs v2" \\
        results/runs/result-v3-memory.csv "SurrealDB 3" \\
        results/runs/result-v2-memory.csv "SurrealDB 2" \\
        --output results/runs/compare-memory.html

    python3 scripts/compare.py \\
        --title "SurrealDB 3: Backends" \\
        results/runs/result-v3-memory.csv "Memory" \\
        results/runs/result-v3-rocksdb.csv "RocksDB" \\
        results/runs/result-v3-surrealkv.csv "SurrealKV" \\
        --output results/runs/compare-v3.html
"""

import argparse
import csv
import html
import json
import os
import statistics
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------

def read_csv(path):
    """Read a benchmark CSV into a list of dicts. Handles both old (22-col) and new (27-col) formats."""
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def parse_ms(val):
    """Parse a latency string like '1.23 ms' into a float, or return None."""
    if not val or val.strip() == "-":
        return None
    val = val.strip().replace(" ms", "")
    try:
        return float(val)
    except ValueError:
        return None


def parse_float(val):
    """Parse a numeric string, return None on failure."""
    if not val or val.strip() == "-":
        return None
    try:
        return float(val.strip())
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Data alignment
# ---------------------------------------------------------------------------

def align_rows(datasets):
    """
    Given a list of [(label, rows)] datasets, produce a unified list of test names
    and per-dataset metric dicts keyed by test name.
    Returns (test_names, [{test: {mean, ops, ...}}])
    """
    test_order = []
    seen = set()
    metrics = []

    for label, rows in datasets:
        m = {}
        for row in rows:
            test = row.get("Test", "").strip()
            if not test:
                continue
            if test not in seen:
                test_order.append(test)
                seen.add(test)
            mean = parse_ms(row.get("Mean", ""))
            ops = parse_float(row.get("OPS", ""))
            p99 = parse_ms(row.get("99th", ""))
            p95 = parse_ms(row.get("95th", ""))
            p50 = parse_ms(row.get("50th", ""))
            min_v = parse_ms(row.get("Min", ""))
            max_v = parse_ms(row.get("Max", ""))
            query = row.get("Query", "").strip() if "Query" in row else ""
            samples = row.get("Samples", "").strip() if "Samples" in row else ""
            clients = row.get("Clients", "").strip() if "Clients" in row else ""
            threads = row.get("Threads", "").strip() if "Threads" in row else ""
            concurrency = row.get("Concurrency", "").strip() if "Concurrency" in row else ""
            m[test] = {
                "mean": mean, "ops": ops, "p99": p99, "p95": p95,
                "p50": p50, "min": min_v, "max": max_v,
                "query": query, "samples": samples,
                "clients": clients, "threads": threads,
                "concurrency": concurrency,
            }
        metrics.append(m)

    return test_order, metrics


# ---------------------------------------------------------------------------
# Categorise tests
# ---------------------------------------------------------------------------

CATEGORIES = [
    ("CRUD", lambda t: t.startswith("[C]") or t.startswith("[R]ead") or t.startswith("[U]") or t.startswith("[D]")),
    ("Scans", lambda t: t.startswith("[S]can::") and "indexed" not in t.lower()),
    ("Indexed Scans", lambda t: t.startswith("[S]can::") and "indexed" in t.lower()),
    ("Index Build/Remove", lambda t: t.startswith("[I]ndex::") or t.startswith("[R]emoveIndex::")),
    ("Batches", lambda t: t.startswith("[B]atch::")),
]


def categorise(test_name):
    for cat, pred in CATEGORIES:
        if pred(test_name):
            return cat
    return "Other"


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

# ANSI colours
GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"


def fmt_delta(pct):
    """Format a delta percentage with colour."""
    if pct is None:
        return "       -"
    sign = "+" if pct >= 0 else ""
    colour = GREEN if pct <= 0 else RED  # lower latency = green
    return f"{colour}{sign}{pct:.1f}%{RESET}"


def fmt_delta_ops(pct):
    """Format an OPS delta percentage with colour (higher = green)."""
    if pct is None:
        return "       -"
    sign = "+" if pct >= 0 else ""
    colour = GREEN if pct >= 0 else RED
    return f"{colour}{sign}{pct:.1f}%{RESET}"


def delta_pct(a, b):
    if a is None or b is None or a == 0:
        return None
    return ((b - a) / a) * 100


def print_terminal(title, labels, test_order, metrics):
    print()
    print("=" * 80)
    print(f" {title}")
    print("=" * 80)
    print()

    n = len(labels)
    is_two = n == 2

    # Build format string
    if is_two:
        hdr = f"{'Test':<48s}  {'Mean A':>9s}  {'Mean B':>9s}  {'Mean %':>9s}  {'OPS A':>12s}  {'OPS B':>12s}  {'OPS %':>9s}"
        sep = f"{'---':<48s}  {'------':>9s}  {'------':>9s}  {'------':>9s}  {'-----':>12s}  {'-----':>12s}  {'-----':>9s}"
    else:
        parts = [f"{'Test':<48s}"]
        sep_parts = [f"{'---':<48s}"]
        for i, lbl in enumerate(labels):
            tag = chr(65 + i)  # A, B, C, ...
            parts.append(f"{'Mean '+tag:>9s}")
            parts.append(f"{'OPS '+tag:>12s}")
            sep_parts.append(f"{'------':>9s}")
            sep_parts.append(f"{'-----':>12s}")
        hdr = "  ".join(parts)
        sep = "  ".join(sep_parts)

    print(hdr)
    print(sep)

    faster_count = 0
    slower_count = 0
    ops_deltas = []

    for test in test_order:
        vals = [m.get(test, {}) for m in metrics]
        means = [v.get("mean") for v in vals]
        opss = [v.get("ops") for v in vals]

        # Skip if all sides have no data
        if all(m is None for m in means) and all(o is None for o in opss):
            continue
        # Skip if any side has no data (can't compare)
        if any(m is None for m in means) or any(o is None for o in opss):
            continue

        if is_two:
            m_a, m_b = means
            o_a, o_b = opss
            m_pct = delta_pct(m_a, m_b)
            o_pct = delta_pct(o_a, o_b)

            if o_pct is not None:
                ops_deltas.append(o_pct)
                if o_pct > 0:
                    faster_count += 1
                elif o_pct < 0:
                    slower_count += 1

            print(
                f"{test:<48s}  {m_a:9.2f}  {m_b:9.2f}  {fmt_delta(m_pct):>18s}  {o_a:12.1f}  {o_b:12.1f}  {fmt_delta_ops(o_pct):>18s}"
            )
        else:
            parts = [f"{test:<48s}"]
            for i in range(n):
                m_val = means[i] if means[i] is not None else 0
                o_val = opss[i] if opss[i] is not None else 0
                parts.append(f"{m_val:9.2f}")
                parts.append(f"{o_val:12.1f}")
            print("  ".join(parts))

    print()
    legend = " | ".join(f"{chr(65+i)} = {lbl}" for i, lbl in enumerate(labels))
    print(legend)

    if is_two:
        print(f"Mean: ms (lower=better). Negative Mean% = {labels[1]} faster.")
        print(f"OPS: ops/sec (higher=better). Positive OPS% = {labels[1]} faster.")
        if ops_deltas:
            med = statistics.median(ops_deltas)
            print(
                f"\nSummary: {labels[1]} faster in {faster_count} tests, "
                f"slower in {slower_count} tests. "
                f"Median OPS change: {med:+.1f}%"
            )
    print()


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

CHART_COLORS = [
    "rgba(54, 162, 235, 0.8)",   # blue
    "rgba(255, 99, 132, 0.8)",   # red
    "rgba(75, 192, 192, 0.8)",   # teal
    "rgba(255, 206, 86, 0.8)",   # yellow
    "rgba(153, 102, 255, 0.8)",  # purple
]

CHART_BORDERS = [
    "rgba(54, 162, 235, 1)",
    "rgba(255, 99, 132, 1)",
    "rgba(75, 192, 192, 1)",
    "rgba(255, 206, 86, 1)",
    "rgba(153, 102, 255, 1)",
]


def generate_html(title, labels, test_order, metrics):
    is_two = len(labels) == 2

    # Metadata from first dataset
    first_m = metrics[0]
    sample_row = next(iter(first_m.values()), {})
    meta_info = {
        "samples": sample_row.get("samples", "-"),
        "clients": sample_row.get("clients", "-"),
        "threads": sample_row.get("threads", "-"),
        "concurrency": sample_row.get("concurrency", "-"),
    }

    # Group tests by category
    cat_tests = {}
    for test in test_order:
        cat = categorise(test)
        cat_tests.setdefault(cat, []).append(test)

    # Filter to tests that have data in all datasets
    def has_data(test):
        return all(
            m.get(test, {}).get("mean") is not None and m.get(test, {}).get("ops") is not None
            for m in metrics
        )

    # Summary stats (two-way only)
    summary = {}
    if is_two:
        ops_deltas_all = []
        mean_deltas_all = []
        for test in test_order:
            if not has_data(test):
                continue
            m_a = metrics[0][test]["mean"]
            m_b = metrics[1][test]["mean"]
            o_a = metrics[0][test]["ops"]
            o_b = metrics[1][test]["ops"]
            if m_a and m_a > 0:
                mean_deltas_all.append(((m_b - m_a) / m_a) * 100)
            if o_a and o_a > 0:
                ops_deltas_all.append(((o_b - o_a) / o_a) * 100)
        summary["total"] = len([t for t in test_order if has_data(t)])
        summary["faster"] = sum(1 for d in ops_deltas_all if d > 0)
        summary["slower"] = sum(1 for d in ops_deltas_all if d < 0)
        summary["median_ops"] = statistics.median(ops_deltas_all) if ops_deltas_all else 0
        summary["median_mean"] = statistics.median(mean_deltas_all) if mean_deltas_all else 0

    # Build chart data per category
    chart_blocks = []
    chart_id = 0
    for cat_name, _ in CATEGORIES + [("Other", lambda t: True)]:
        tests = cat_tests.get(cat_name, [])
        tests = [t for t in tests if has_data(t)]
        if not tests:
            continue

        # Mean latency chart
        mean_labels_js = json.dumps([_short_name(t) for t in tests])
        mean_datasets = []
        for i, lbl in enumerate(labels):
            data = [round(metrics[i].get(t, {}).get("mean", 0) or 0, 3) for t in tests]
            mean_datasets.append({
                "label": lbl,
                "data": data,
                "backgroundColor": CHART_COLORS[i % len(CHART_COLORS)],
                "borderColor": CHART_BORDERS[i % len(CHART_BORDERS)],
                "borderWidth": 1,
            })

        # OPS chart
        ops_datasets = []
        for i, lbl in enumerate(labels):
            data = [round(metrics[i].get(t, {}).get("ops", 0) or 0, 1) for t in tests]
            ops_datasets.append({
                "label": lbl,
                "data": data,
                "backgroundColor": CHART_COLORS[i % len(CHART_COLORS)],
                "borderColor": CHART_BORDERS[i % len(CHART_BORDERS)],
                "borderWidth": 1,
            })

        bar_height = max(len(tests) * 25 * len(labels), 200)
        mean_id = f"chart_mean_{chart_id}"
        ops_id = f"chart_ops_{chart_id}"
        chart_id += 1

        chart_blocks.append({
            "category": cat_name,
            "mean_id": mean_id,
            "ops_id": ops_id,
            "labels_js": mean_labels_js,
            "mean_datasets_js": json.dumps(mean_datasets),
            "ops_datasets_js": json.dumps(ops_datasets),
            "bar_height": bar_height,
        })

    # Build detail table rows
    table_rows = []
    for test in test_order:
        if not has_data(test):
            continue
        row = {"test": test, "category": categorise(test), "values": []}
        for i in range(len(labels)):
            d = metrics[i].get(test, {})
            entry = {
                "mean": d.get("mean"), "ops": d.get("ops"),
                "p99": d.get("p99"), "p95": d.get("p95"),
                "p50": d.get("p50"), "min": d.get("min"), "max": d.get("max"),
            }
            row["values"].append(entry)
        if is_two:
            m_a = row["values"][0]["mean"]
            m_b = row["values"][1]["mean"]
            o_a = row["values"][0]["ops"]
            o_b = row["values"][1]["ops"]
            row["mean_delta"] = ((m_b - m_a) / m_a * 100) if m_a else None
            row["ops_delta"] = ((o_b - o_a) / o_a * 100) if o_a else None
        query_text = metrics[0].get(test, {}).get("query", "")
        row["query"] = query_text
        table_rows.append(row)

    # Render HTML
    return _render_html(title, labels, is_two, meta_info, summary, chart_blocks, table_rows)


def _short_name(test):
    """Shorten a test name for chart labels."""
    # Remove prefixes like [S]can::, [C]reate, etc. and sample counts
    name = test
    for prefix in ["[S]can::", "[C]reate", "[R]ead", "[U]pdate", "[D]elete",
                    "[I]ndex::", "[R]emoveIndex::", "[B]atch::"]:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    # Remove trailing sample count like " (10000)"
    if "(" in name:
        name = name[:name.rfind("(")].strip()
    return name


def _render_html(title, labels, is_two, meta_info, summary, chart_blocks, table_rows):
    escaped_title = html.escape(title)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Summary cards (two-way only)
    summary_html = ""
    if is_two:
        summary_html = f"""
        <div class="summary-cards">
            <div class="card">
                <div class="card-value">{summary['total']}</div>
                <div class="card-label">Total Tests</div>
            </div>
            <div class="card card-green">
                <div class="card-value">{summary['faster']}</div>
                <div class="card-label">{html.escape(labels[1])} Faster</div>
            </div>
            <div class="card card-red">
                <div class="card-value">{summary['slower']}</div>
                <div class="card-label">{html.escape(labels[1])} Slower</div>
            </div>
            <div class="card">
                <div class="card-value">{summary['median_ops']:+.1f}%</div>
                <div class="card-label">Median OPS Change</div>
            </div>
            <div class="card">
                <div class="card-value">{summary['median_mean']:+.1f}%</div>
                <div class="card-label">Median Latency Change</div>
            </div>
        </div>
        """

    # Chart sections
    charts_html = ""
    charts_js = ""
    for cb in chart_blocks:
        charts_html += f"""
        <div class="chart-section">
            <h2>{html.escape(cb['category'])}</h2>
            <div class="chart-row">
                <div class="chart-container">
                    <h3>Mean Latency (ms) — lower is better</h3>
                    <div style="height:{cb['bar_height']}px"><canvas id="{cb['mean_id']}"></canvas></div>
                </div>
                <div class="chart-container">
                    <h3>Operations/sec — higher is better</h3>
                    <div style="height:{cb['bar_height']}px"><canvas id="{cb['ops_id']}"></canvas></div>
                </div>
            </div>
        </div>
        """
        charts_js += f"""
        new Chart(document.getElementById('{cb['mean_id']}'), {{
            type: 'bar',
            data: {{ labels: {cb['labels_js']}, datasets: {cb['mean_datasets_js']} }},
            options: {{
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'top' }} }},
                scales: {{ x: {{ beginAtZero: true, title: {{ display: true, text: 'ms' }} }} }}
            }}
        }});
        new Chart(document.getElementById('{cb['ops_id']}'), {{
            type: 'bar',
            data: {{ labels: {cb['labels_js']}, datasets: {cb['ops_datasets_js']} }},
            options: {{
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'top' }} }},
                scales: {{ x: {{ beginAtZero: true, title: {{ display: true, text: 'ops/sec' }} }} }}
            }}
        }});
        """

    # Detail table
    # Build header
    th_labels = ""
    for lbl in labels:
        th_labels += f'<th colspan="3">{html.escape(lbl)}</th>'

    delta_th = ""
    if is_two:
        delta_th = '<th>Mean %</th><th>OPS %</th>'

    # Build rows
    tbody = ""
    for row in table_rows:
        tds = ""
        for v in row["values"]:
            mean_s = f"{v['mean']:.2f}" if v["mean"] is not None else "-"
            ops_s = f"{v['ops']:.1f}" if v["ops"] is not None else "-"
            p99_s = f"{v['p99']:.2f}" if v["p99"] is not None else "-"
            tds += f"<td>{mean_s}</td><td>{p99_s}</td><td>{ops_s}</td>"

        delta_tds = ""
        if is_two:
            md = row.get("mean_delta")
            od = row.get("ops_delta")
            md_cls = "delta-good" if md is not None and md < 0 else "delta-bad" if md is not None and md > 0 else ""
            od_cls = "delta-good" if od is not None and od > 0 else "delta-bad" if od is not None and od < 0 else ""
            md_s = f"{md:+.1f}%" if md is not None else "-"
            od_s = f"{od:+.1f}%" if od is not None else "-"
            delta_tds = f'<td class="{md_cls}">{md_s}</td><td class="{od_cls}">{od_s}</td>'

        query_td = f"<td class='query-cell'>{html.escape(row.get('query', ''))}</td>"
        cat_td = f"<td>{html.escape(row['category'])}</td>"
        tbody += f"<tr><td>{html.escape(row['test'])}</td>{cat_td}{tds}{delta_tds}{query_td}</tr>\n"

    # Sub-headers for each label
    sub_th = ""
    for _ in labels:
        sub_th += "<th>Mean</th><th>P99</th><th>OPS</th>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escaped_title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
    :root {{
        --bg: #1a1a2e; --surface: #16213e; --surface2: #0f3460;
        --text: #e0e0e0; --text2: #a0a0a0; --accent: #e94560;
        --green: #00c853; --red: #ff5252; --border: #2a2a4a;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        background: var(--bg); color: var(--text); padding: 24px;
        line-height: 1.5;
    }}
    h1 {{ font-size: 1.8rem; margin-bottom: 4px; }}
    h2 {{ font-size: 1.3rem; margin: 24px 0 12px; color: var(--accent); }}
    h3 {{ font-size: 0.95rem; margin-bottom: 8px; color: var(--text2); }}
    .meta {{ color: var(--text2); font-size: 0.85rem; margin-bottom: 20px; }}
    .meta span {{ margin-right: 18px; }}
    .summary-cards {{
        display: flex; gap: 16px; flex-wrap: wrap; margin: 20px 0;
    }}
    .card {{
        background: var(--surface); border: 1px solid var(--border);
        border-radius: 8px; padding: 16px 24px; min-width: 140px;
        text-align: center;
    }}
    .card-value {{ font-size: 1.6rem; font-weight: 700; }}
    .card-label {{ font-size: 0.8rem; color: var(--text2); margin-top: 4px; }}
    .card-green .card-value {{ color: var(--green); }}
    .card-red .card-value {{ color: var(--red); }}
    .chart-section {{ margin: 32px 0; }}
    .chart-row {{
        display: grid; grid-template-columns: 1fr 1fr; gap: 24px;
    }}
    .chart-container {{
        background: var(--surface); border: 1px solid var(--border);
        border-radius: 8px; padding: 16px;
    }}
    table {{
        width: 100%; border-collapse: collapse; margin-top: 24px;
        font-size: 0.82rem;
    }}
    th, td {{
        padding: 6px 10px; border: 1px solid var(--border);
        text-align: right; white-space: nowrap;
    }}
    th {{ background: var(--surface2); position: sticky; top: 0; }}
    td:first-child, th:first-child {{ text-align: left; }}
    .delta-good {{ color: var(--green); font-weight: 600; }}
    .delta-bad {{ color: var(--red); font-weight: 600; }}
    .query-cell {{
        text-align: left; max-width: 360px; overflow: hidden;
        text-overflow: ellipsis; font-family: monospace; font-size: 0.78rem;
        color: var(--text2);
    }}
    @media (max-width: 1000px) {{
        .chart-row {{ grid-template-columns: 1fr; }}
    }}
</style>
</head>
<body>
<h1>{escaped_title}</h1>
<div class="meta">
    <span>Generated: {now}</span>
    <span>Samples: {meta_info['samples']}</span>
    <span>Clients: {meta_info['clients']}</span>
    <span>Threads: {meta_info['threads']}</span>
    <span>Concurrency: {meta_info['concurrency']}</span>
</div>

{summary_html}

{charts_html}

<h2>Detailed Comparison</h2>
<div style="overflow-x:auto">
<table>
<thead>
<tr>
    <th rowspan="2">Test</th>
    <th rowspan="2">Category</th>
    {th_labels}
    {delta_th}
    <th rowspan="2">Query</th>
</tr>
<tr>
    {sub_th}
</tr>
</thead>
<tbody>
{tbody}
</tbody>
</table>
</div>

<script>
Chart.defaults.color = '#a0a0a0';
Chart.defaults.borderColor = '#2a2a4a';
{charts_js}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Compare benchmark CSV results with charts and summaries.",
        usage="%(prog)s --title TITLE file1.csv label1 file2.csv label2 [file3.csv label3 ...] [--output report.html]",
    )
    parser.add_argument("--title", required=True, help="Comparison title")
    parser.add_argument("--output", default=None, help="Path for the HTML report (optional)")
    parser.add_argument("inputs", nargs="+", help="Alternating file.csv label pairs")

    args = parser.parse_args()

    # Parse file/label pairs
    if len(args.inputs) < 4 or len(args.inputs) % 2 != 0:
        parser.error("Need at least 2 file/label pairs (file1.csv label1 file2.csv label2)")

    pairs = []
    for i in range(0, len(args.inputs), 2):
        fpath = args.inputs[i]
        label = args.inputs[i + 1]
        if not os.path.isfile(fpath):
            print(f"ERROR: {fpath} not found. Run the benchmark first.", file=sys.stderr)
            sys.exit(1)
        pairs.append((label, read_csv(fpath)))

    labels = [p[0] for p in pairs]
    test_order, metrics = align_rows(pairs)

    # Terminal output
    print_terminal(args.title, labels, test_order, metrics)

    # HTML report
    if args.output:
        html_content = generate_html(args.title, labels, test_order, metrics)
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            f.write(html_content)
        print(f"HTML report saved to: {args.output}")


if __name__ == "__main__":
    main()
