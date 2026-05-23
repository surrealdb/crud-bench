#!/usr/bin/env bash
#
# dev.sh — Run crud-bench against a locally compiled SurrealDB
#
# Builds SurrealDB from a local checkout, starts a native (non-Docker) server
# with RocksDB storage, and runs crud-bench against it.
#
# Two modes:
#
#   profiling  Builds with `--profile profiling` + frame pointers, attaches
#              `perf record` to the SurrealDB process once per requested
#              phase, and renders one flamegraph SVG per phase. Use this
#              when you want flamegraphs.
#
#   release    Plain `cargo build --release`. No perf, no flamegraph — just
#              a benchmark run against your local build. Use this to check
#              the effect of a code change on end-to-end numbers.
#
# In profiling mode the script watches crud-bench's log for per-phase
# `starting` / `took` markers (emitted under `--emit-phase-markers`) and
# brackets a separate perf window per individual phase. Each window writes
# its own `perf-<phase>.data` and renders to `flamegraph-<phase>.svg`.
#
# crud-bench emits (with `--emit-phase-markers` in profiling mode; phase lines stay
# ASCII at column 0 for tooling; optional leading ANSI SGR may be added later):
#   "Connecting to datastore" / "Datastore ready"
#   "Setting up N client(s)"  / "Benchmark starting"
#   "<rich phase label> starting" / "<rich phase label> took …"
#   "Benchmark complete"      / "Disconnecting from datastore"
#
# The rich phase label embeds the scan id / batch name so per-run markers
# are unique:
#   Create / Read / Update / Delete                                  (CRUD)
#   Scan :: <id> :: <name> :: no-index|indexed[, writes N%]           (scans)
#   BuildIndex :: <id> / RemoveIndex :: <id>                          (index DDL)
#   BatchCreate::<name> / BatchRead::<name> / …                       (batches)
#
# Phases always run in crud-bench's fixed order:
#
#     Create → Read → Update → Scans (incl. BuildIndex / ScanWithIndex /
#                                       RemoveIndex) → Delete → Batches
#
# Supported PHASES categories (comma-separated; default "all"):
#
#     crud        One perf window per CRUD op: Create, Read, Update, Delete.
#                 Passes --skip-scans --skip-indexes --skip-batches so the
#                 windows stay tight.
#     scans       One perf window per scan run (each Scan, ScanWithWrites,
#                 BuildIndex, RemoveIndex captured separately).
#                 Passes --skip-batches.
#     batches     One perf window per [[batches]] entry — c/r/u/d × size
#                 combinations are split apart (e.g. batch_create_100,
#                 batch_read_1000). Passes --skip-scans --skip-indexes.
#     all         crud + scans + batches with no --skip-* flags. Captures
#                 every phase as its own perf window in crud-bench's
#                 natural execution order.
#
# Short windows (< ~2s) get a warning since `-F PERF_FREQ` (default 997 Hz)
# leaves the flamegraph sparse.
#
# Usage:
#   ./dev.sh
#
# The script asks a handful of questions at the top (mode, source path,
# samples, clients, threads). Hit <Enter> to accept the defaults.
#
# Unwinding strategy (profiling mode only):
#   SurrealDB is built with frame pointers enabled
#   (RUSTFLAGS="-C force-frame-pointers=yes") and perf records with
#   `--call-graph fp`. This keeps `perf.data` small (~100 MB) and makes
#   `perf script` finish in under a minute. The first profiling build is a
#   full LTO recompile (~5–10 min); subsequent runs are incremental.
#   DWARF unwinding is not supported — on this codebase `perf script` on a
#   DWARF capture takes hours even for a handful of seconds of samples.
#
# Environment overrides (all optional):
#   MODE              profiling | release                (prompted)
#   SURREALDB_DIR     Path to surrealdb checkout         (prompted)
#   SAMPLES           Number of samples                  (prompted)
#   CLIENTS           Concurrent clients                 (prompted)
#   THREADS           Worker threads                     (prompted)
#   PHASES            Comma-sep categories to profile    (prompted, profiling mode)
#                     Any of: crud, scans, batches, all
#   KEY_TYPE          Primary key type                   (default: integer)
#   DB_PATH           RocksDB data dir for the server    (default: ./data next to dev.sh)
#   OUTPUT_DIR        Where logs/perf/flamegraph go      (default: ./dev-results-<mode>-<ts>)
#   SURREAL_PORT      TCP port for SurrealDB             (default: 8000)
#   PERF_FREQ         perf sampling frequency (Hz)       (default: 997)
#   PERF_MAX_SECS     Hard cap on each perf window       (default: 600)
#   CRUD_BENCH_CONFIG  Path to benchmark TOML (default: config/bench.toml)
#   CRUD_BENCH_EMIT_PHASE_MARKERS  Set to 1/true/yes/on so crud-bench prints grep-friendly
#                     `… starting` lines without passing `--emit-phase-markers` (profiling
#                     mode passes the flag automatically).
#   FLAMEGRAPH_BIN    Path to flamegraph binary          (default: ~/.cargo/bin/flamegraph)
#   COMPACTION        Prompted at start; any value set means crud-bench runs storage
#                     compaction between phases (SurrealDB: ALTER SYSTEM COMPACT). SST
#                     compaction does not shrink RocksDB WAL (*.log) files the same
#                     way — large .log under data/ is normal.
#
# Prerequisites (one-time setup — profiling mode only):
#   sudo apt install linux-tools-common linux-tools-generic linux-tools-$(uname -r)
#   cargo install flamegraph
#   sudo sysctl -w kernel.perf_event_paranoid=-1
#

set -euo pipefail

# -----------------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------------
SCRIPT_DIR="$( cd -- "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd )"
TS=$(date +%Y%m%d-%H%M%S)

DEFAULT_SURREALDB_DIR="$(cd "$SCRIPT_DIR/../surrealdb" 2>/dev/null && pwd || echo "../surrealdb")"

# Fixed defaults (can be overridden via env)
KEY_TYPE="${KEY_TYPE:-integer}"
DB_PATH="${DB_PATH:-$SCRIPT_DIR/data}"
SURREAL_PORT="${SURREAL_PORT:-8000}"
PERF_FREQ="${PERF_FREQ:-997}"
# Back-compat: honour SCAN_MAX_SECS if someone has it in their env
PERF_MAX_SECS="${PERF_MAX_SECS:-${SCAN_MAX_SECS:-600}}"
FLAMEGRAPH_BIN="${FLAMEGRAPH_BIN:-$HOME/.cargo/bin/flamegraph}"
CRUD_BENCH_DIR="${CRUD_BENCH_DIR:-$SCRIPT_DIR}"

# -----------------------------------------------------------------------------
# Logging helpers
# -----------------------------------------------------------------------------
log()  { printf '\033[0;34m[dev]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[dev]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[0;31m[dev]\033[0m %s\n' "$*" >&2; exit 1; }

# -----------------------------------------------------------------------------
# Interactive prompts
# -----------------------------------------------------------------------------
# Prompt the user for a value, falling back to a default if they press Enter.
# Respects a pre-set environment variable (skips the prompt if already set).
# Args: $1 = variable name, $2 = question text, $3 = default value
ask() {
	local __var=$1
	local __q=$2
	local __def=$3
	local __cur=${!__var:-}
	local __ans

	if [[ -n "$__cur" ]]; then
		printf '  %s: %s (from env)\n' "$__q" "$__cur"
		return
	fi

	read -rp "  $__q [$__def]: " __ans || true
	printf -v "$__var" '%s' "${__ans:-$__def}"
}

echo "================================================================"
echo "  crud-bench :: local SurrealDB runner"
echo "================================================================"
echo

while true; do
	ask MODE "Mode (profiling|release)" "profiling"
	case "$MODE" in
		profiling|release) break ;;
		*)
			warn "Please answer 'profiling' or 'release'."
			unset MODE
			;;
	esac
done

ask SURREALDB_DIR "SurrealDB source directory" "$DEFAULT_SURREALDB_DIR"

if [[ "$MODE" == "release" ]]; then
	DEFAULT_SAMPLES=1000000
else
	DEFAULT_SAMPLES=500000
fi
ask SAMPLES "Number of samples" "$DEFAULT_SAMPLES"
ask CLIENTS "Concurrent clients"  "128"
ask THREADS "Worker threads"      "48"

# -----------------------------------------------------------------------------
# Compaction toggle
#
# crud-bench treats any non-unset COMPACTION env var as "enabled", so a
# "no" answer must unset the variable entirely rather than setting it to
# an empty string or "0".
# -----------------------------------------------------------------------------
while true; do
	ask COMPACTION_CHOICE "Run compaction between phases? (yes|no)" "no"
	case "${COMPACTION_CHOICE,,}" in
		y|yes|true|1)  export COMPACTION=1; COMPACTION_CHOICE=yes; break ;;
		n|no|false|0) unset COMPACTION;     COMPACTION_CHOICE=no;  break ;;
		*)
			warn "Please answer 'yes' or 'no'."
			unset COMPACTION_CHOICE
			;;
	esac
done

# -----------------------------------------------------------------------------
# Phase selection (profiling mode only)
# -----------------------------------------------------------------------------
#
# PHASES is a comma-separated list of categories:
#
#   crud     → Create, Read, Update, Delete  (one perf window each)
#   scans    → every Scan / ScanWithWrites / BuildIndex / RemoveIndex run
#              (one perf window per run, named after the scan id)
#   batches  → every [[batches]] entry (one perf window per c/r/u/d × size)
#   all      → crud + scans + batches
#
# `all` expands to {crud, scans, batches}; duplicates after expansion are
# de-deduplicated.
VALID_CATEGORIES="all crud scans batches"
declare -A VALID_CATEGORY_SET=([all]=1 [crud]=1 [scans]=1 [batches]=1)

# Selected categories (lowercase, deduped). `all` expanded to the three
# concrete categories so downstream logic only needs to check those.
CATEGORIES=()
HAS_CRUD=0
HAS_SCANS=0
HAS_BATCHES=0

if [[ "$MODE" == "profiling" ]]; then
	while true; do
		ask PHASES "Categories to profile (comma-sep: $VALID_CATEGORIES)" "all"
		IFS=',' read -ra __req <<<"$PHASES"
		CATEGORIES=()
		HAS_CRUD=0; HAS_SCANS=0; HAS_BATCHES=0
		__ok=1
		for __c in "${__req[@]}"; do
			__c="${__c// /}"; __c="${__c,,}"
			[[ -z "$__c" ]] && continue
			if [[ -z "${VALID_CATEGORY_SET[$__c]+x}" ]]; then
				warn "Unknown category: '$__c' (valid: $VALID_CATEGORIES)"
				__ok=0
				break
			fi
			case "$__c" in
				all)     HAS_CRUD=1; HAS_SCANS=1; HAS_BATCHES=1 ;;
				crud)    HAS_CRUD=1 ;;
				scans)   HAS_SCANS=1 ;;
				batches) HAS_BATCHES=1 ;;
			esac
		done
		if (( __ok )); then
			(( HAS_CRUD ))    && CATEGORIES+=(crud)
			(( HAS_SCANS ))   && CATEGORIES+=(scans)
			(( HAS_BATCHES )) && CATEGORIES+=(batches)
			(( ${#CATEGORIES[@]} > 0 )) && break
		fi
		unset PHASES
	done
fi

OUTPUT_DIR="${OUTPUT_DIR:-$CRUD_BENCH_DIR/dev-results-$MODE-$TS}"

echo

# -----------------------------------------------------------------------------
# Prerequisites
# -----------------------------------------------------------------------------
if [[ "$MODE" == "profiling" ]]; then
	command -v perf >/dev/null \
		|| die "perf not found. Install: sudo apt install linux-tools-common linux-tools-generic linux-tools-\$(uname -r)"

	[[ -x "$FLAMEGRAPH_BIN" ]] \
		|| die "flamegraph not found at $FLAMEGRAPH_BIN. Install: cargo install flamegraph"

	PARANOID=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)
	(( PARANOID <= 1 )) \
		|| die "kernel.perf_event_paranoid=$PARANOID; run: sudo sysctl -w kernel.perf_event_paranoid=-1"
fi

[[ -n "$SURREALDB_DIR" && -d "$SURREALDB_DIR" ]] \
	|| die "SurrealDB source tree not found at: $SURREALDB_DIR"
[[ -d "$CRUD_BENCH_DIR" ]] \
	|| die "crud-bench not found at $CRUD_BENCH_DIR"

if ss -lnt "sport = :$SURREAL_PORT" 2>/dev/null | grep -q LISTEN; then
	die "Port $SURREAL_PORT already in use — stop whatever is listening before running"
fi

# -----------------------------------------------------------------------------
# Output paths
# -----------------------------------------------------------------------------
mkdir -p "$OUTPUT_DIR"
CRUD_LOG="$OUTPUT_DIR/crud-bench.log"
SURREAL_LOG="$OUTPUT_DIR/surreal.log"

# -----------------------------------------------------------------------------
# Derive crud-bench --skip-* flags from selected categories.
#
# Rationale: if a category isn't selected, skip its phases to keep the run
# tight. Scans and indexes run together (with-index scans live between
# BuildIndex and RemoveIndex), so they share a skip flag pair. CRUD ops
# (Create/Read/Update/Delete) always run — there are no skip flags for
# them — when 'crud' isn't selected they just run unprofiled.
# -----------------------------------------------------------------------------
CRUD_SKIP_ARGS=()
if [[ "$MODE" == "profiling" ]]; then
	(( HAS_SCANS ))   || CRUD_SKIP_ARGS+=(--skip-scans --skip-indexes)
	(( HAS_BATCHES )) || CRUD_SKIP_ARGS+=(--skip-batches)
fi

echo "================================================================"
echo "  Running in $MODE mode"
echo "================================================================"
printf "  %-14s %s\n" "crud-bench:" "$CRUD_BENCH_DIR"
printf "  %-14s %s\n" "SurrealDB:"  "$SURREALDB_DIR"
printf "  %-14s %s\n" "Data path:"    "$DB_PATH"
printf "  %-14s %s\n" "Output:"     "$OUTPUT_DIR"
printf "  %-14s samples=%s  clients=%s  threads=%s  key=%s\n" \
       "Params:" "$SAMPLES" "$CLIENTS" "$THREADS" "$KEY_TYPE"
printf "  %-14s %s\n" "Compaction:" "$COMPACTION_CHOICE"
if [[ "$MODE" == "profiling" ]]; then
	printf "  %-14s %s\n" "Categories:"  "${CATEGORIES[*]}"
	printf "  %-14s %s Hz  max=%ss  (--call-graph fp)\n" \
	       "perf:" "$PERF_FREQ" "$PERF_MAX_SECS"
	if (( ${#CRUD_SKIP_ARGS[@]} > 0 )); then
		printf "  %-14s %s\n" "crud-bench:" "${CRUD_SKIP_ARGS[*]}"
	fi
fi
echo

# -----------------------------------------------------------------------------
# Cleanup trap
# -----------------------------------------------------------------------------
SURREAL_PID=""
CRUD_PID=""
PERF_PID=""

cleanup() {
	local rc=$?
	set +e
	if [[ -n "$PERF_PID" ]] && kill -0 "$PERF_PID" 2>/dev/null; then
		log "Stopping perf ($PERF_PID)..."
		kill -INT "$PERF_PID" 2>/dev/null
		wait "$PERF_PID" 2>/dev/null
	fi
	if [[ -n "$CRUD_PID" ]] && kill -0 "$CRUD_PID" 2>/dev/null; then
		log "Stopping crud-bench ($CRUD_PID)..."
		kill -TERM "$CRUD_PID" 2>/dev/null
		wait "$CRUD_PID" 2>/dev/null
	fi
	if [[ -n "$SURREAL_PID" ]] && kill -0 "$SURREAL_PID" 2>/dev/null; then
		log "Stopping SurrealDB ($SURREAL_PID)..."
		kill -TERM "$SURREAL_PID" 2>/dev/null
		wait "$SURREAL_PID" 2>/dev/null
	fi
	exit $rc
}
trap cleanup EXIT INT TERM

# -----------------------------------------------------------------------------
# 1) Build SurrealDB
#
#    profiling mode: `cargo build --profile profiling` with frame pointers so
#    perf can stack-walk cheaply. The first build is a full LTO recompile
#    (~5–10 min on this box); subsequent runs are incremental.
#
#    release mode: plain `cargo build --release`, no frame-pointer flags.
# -----------------------------------------------------------------------------
SURREAL_TARGET_DIR="$SURREALDB_DIR/target"

if [[ "$MODE" == "profiling" ]]; then
	SURREAL_BIN="$SURREAL_TARGET_DIR/profiling/surreal"
	log "[1/6] Building SurrealDB (--profile profiling, frame pointers)"
	if [[ ! -x "$SURREAL_BIN" ]]; then
		log "      (first profiling build is a full recompile; ~5–10 min on this box)"
	fi
	(
		cd "$SURREALDB_DIR"
		CARGO_TARGET_DIR="$SURREAL_TARGET_DIR" \
		RUSTFLAGS="-C force-frame-pointers=yes" \
			cargo build --profile profiling --bin surreal
	)
else
	SURREAL_BIN="$SURREAL_TARGET_DIR/release/surreal"
	log "[1/6] Building SurrealDB (--release)"
	(
		cd "$SURREALDB_DIR"
		CARGO_TARGET_DIR="$SURREAL_TARGET_DIR" \
			cargo build --release --bin surreal
	)
fi
[[ -x "$SURREAL_BIN" ]] || die "Built binary missing: $SURREAL_BIN"

# -----------------------------------------------------------------------------
# 2) Build crud-bench (release)
# -----------------------------------------------------------------------------
log "[2/6] Building crud-bench (--release --bin crud-bench)"
(cd "$CRUD_BENCH_DIR" && cargo build --release --bin crud-bench)
CRUD_BIN="$CRUD_BENCH_DIR/target/release/crud-bench"
[[ -x "$CRUD_BIN" ]] || die "Built binary missing: $CRUD_BIN"

# -----------------------------------------------------------------------------
# 3) Start SurrealDB
# -----------------------------------------------------------------------------
log "[3/6] Starting SurrealDB (rocksdb:$DB_PATH)"
rm -rf "$DB_PATH"
"$SURREAL_BIN" start \
	--bind "127.0.0.1:$SURREAL_PORT" \
	--allow-all -u root -p root \
	"rocksdb:$DB_PATH" \
	> "$SURREAL_LOG" 2>&1 &
SURREAL_PID=$!
log "      PID=$SURREAL_PID  log=$SURREAL_LOG"

for _ in $(seq 1 60); do
	if curl -sf "http://127.0.0.1:$SURREAL_PORT/status" >/dev/null 2>&1; then
		log "      SurrealDB is up"
		break
	fi
	if ! kill -0 "$SURREAL_PID" 2>/dev/null; then
		warn "SurrealDB exited early; tail of log:"
		tail -40 "$SURREAL_LOG" >&2
		die "SurrealDB did not start"
	fi
	sleep 1
done
curl -sf "http://127.0.0.1:$SURREAL_PORT/status" >/dev/null 2>&1 \
	|| die "SurrealDB never answered /status within 60s"

# -----------------------------------------------------------------------------
# 4) Launch crud-bench
#    `stdbuf -oL` forces line-buffered stdout so we see phase messages live.
# -----------------------------------------------------------------------------
log "[4/6] Launching crud-bench"
if [[ -n "${COMPACTION:-}" ]]; then
	log "      COMPACTION is set — expect \"Compaction took …\" lines in crud-bench.log between phases"
fi
CRUD_PHASE_MARKER_ARGS=()
if [[ "$MODE" == "profiling" ]]; then
	CRUD_PHASE_MARKER_ARGS=(--emit-phase-markers)
fi
(
	cd "$CRUD_BENCH_DIR"
	stdbuf -oL -eL "$CRUD_BIN" \
		-d surrealdb -e "ws://127.0.0.1:$SURREAL_PORT" \
		-s "$SAMPLES" -c "$CLIENTS" -t "$THREADS" -k "$KEY_TYPE" \
		-n "dev-$MODE-$TS" -r \
		"${CRUD_PHASE_MARKER_ARGS[@]}" \
		"${CRUD_SKIP_ARGS[@]}"
) > "$CRUD_LOG" 2>&1 &
CRUD_PID=$!
log "      PID=$CRUD_PID  log=$CRUD_LOG"

# -----------------------------------------------------------------------------
# 5) Wait for crud-bench to finish.
#
#    In profiling mode we walk PHASE_LIST and open a separate perf window
#    for each requested phase, using regex markers over the crud-bench
#    log. Each phase gets its own perf-<phase>.data file.
#
#    Phase order printed by crud-bench (AFTER each phase completes):
#      "Create took …"  →  "Read took …"  →  "Update took …"
#      → ["Scan ::<ctx> took …", …]
#      → ["BuildIndex took …", "Scan :: … took …", "RemoveIndex took …"]
#      → "Delete took …"
#      → ["Batch… took …", …]
#
#    In release mode we just wait for crud-bench to exit.
# -----------------------------------------------------------------------------

# Attach perf to SURREAL_PID for up to PERF_MAX_SECS, writing to $1.
# Stores pid in PERF_PID so the cleanup trap can kill it if we abort.
start_perf() {
	local out=$1 log_path=$2
	perf record \
		-F "$PERF_FREQ" \
		--call-graph fp \
		-g \
		-p "$SURREAL_PID" \
		-o "$out" \
		-- sleep "$PERF_MAX_SECS" \
		> "$log_path" 2>&1 &
	PERF_PID=$!
}

stop_perf() {
	if [[ -n "$PERF_PID" ]] && kill -0 "$PERF_PID" 2>/dev/null; then
		kill -INT "$PERF_PID" 2>/dev/null || true
		wait "$PERF_PID" 2>/dev/null || true
	fi
	PERF_PID=""
}

# -----------------------------------------------------------------------------
# Per-line observer state.
#
# The profiling loop walks the crud-bench log line-by-line so each phase
# can open its own perf window — every Scan, BuildIndex, and Batch run
# gets a distinct marker (rich `--emit-phase-markers` labels carry the
# scan id / batch name).
# -----------------------------------------------------------------------------
LINE_CURSOR=1
MATCH_LINE=""

# Lowercase, non-alnum → '-', collapse runs, trim. Used for filenames.
slugify() {
	local s=${1,,}
	s=$(printf '%s' "$s" | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//')
	[[ -z "$s" ]] && s="unnamed"
	printf '%s' "$s"
}

# Escape ERE metacharacters so $1 can be spliced into a regex literally.
escape_re() {
	printf '%s' "$1" | sed -E 's/[][\\.^$*+?(){}|]/\\&/g'
}

# Read forward from LINE_CURSOR until a line matches $1 (regex). Sets
# MATCH_LINE and advances LINE_CURSOR past the matching line.
#
#   $2 (optional): terminator regex — if any line matches it before $1,
#                  set MATCH_LINE and return 2.
#   $3 (optional, "1"): also abort when PERF_PID dies (return 3) —
#                       used while a window is open so a perf-self-exit
#                       (PERF_MAX_SECS hit) doesn't leave us waiting.
#
# Return 1 if crud-bench exits before any of the above.
wait_for_line() {
	local pat=$1 terminator=${2:-} watch_perf=${3:-0}
	while true; do
		if [[ -f "$CRUD_LOG" ]]; then
			local lineno=$LINE_CURSOR
			local line
			while IFS= read -r line; do
				if [[ -n "$terminator" ]] && [[ "$line" =~ $terminator ]]; then
					MATCH_LINE=$line
					LINE_CURSOR=$((lineno + 1))
					return 2
				fi
				if [[ "$line" =~ $pat ]]; then
					MATCH_LINE=$line
					LINE_CURSOR=$((lineno + 1))
					return 0
				fi
				lineno=$((lineno + 1))
			done < <(tail -n +"$LINE_CURSOR" "$CRUD_LOG" 2>/dev/null)
			LINE_CURSOR=$lineno
		fi
		if (( watch_perf )) && [[ -n "${PERF_PID:-}" ]] && ! kill -0 "$PERF_PID" 2>/dev/null; then
			return 3
		fi
		if ! kill -0 "$CRUD_PID" 2>/dev/null; then
			return 1
		fi
		sleep 0.5
	done
}

PERF_DATA_FILES=()
SHOULD_STOP_LOOP=0

# Open a perf window named $1, wait for the took regex $2, then close it.
# Records the .data file into PERF_DATA_FILES on success; warns on short
# windows (< 2s) since sparse sampling makes the flamegraph noisy.
capture_window() {
	local name=$1 took_pat=$2
	local data_file="$OUTPUT_DIR/perf-${name}.data"
	local log_file="$OUTPUT_DIR/perf-${name}.log"
	local ts_start ts_end elapsed note=""
	ts_start=$(date +%s)
	log "      [$name] attaching perf (pid=$SURREAL_PID)"
	start_perf "$data_file" "$log_file"

	wait_for_line "$took_pat" '^Benchmark complete$' 1
	local rc=$?

	ts_end=$(date +%s)
	stop_perf
	elapsed=$((ts_end - ts_start))
	case $rc in
		0) ;;
		1) note=", crud-bench exited before took marker"; SHOULD_STOP_LOOP=1 ;;
		2) note=", Benchmark complete before took marker"; SHOULD_STOP_LOOP=1 ;;
		3) note=", perf hit PERF_MAX_SECS=$PERF_MAX_SECS" ;;
	esac
	log "      [$name] detaching perf (captured ~${elapsed}s${note})"

	if [[ -s "$data_file" ]]; then
		PERF_DATA_FILES+=("$data_file")
		if (( elapsed < 2 )) && (( rc == 0 )); then
			warn "[$name] window was only ${elapsed}s — flamegraph will be sparse at ${PERF_FREQ}Hz"
		fi
	else
		warn "[$name] no perf data recorded (see $log_file)"
	fi
}

if [[ "$MODE" == "profiling" ]]; then
	log "[5/6] Profiling categories: ${CATEGORIES[*]}"

	# Combined "any starting marker we care about" regex, built from the
	# selected categories. Each branch is anchored so we can dispatch
	# unambiguously on the matched line.
	any_start_parts=()
	(( HAS_CRUD ))    && any_start_parts+=('^(Create|Read|Update|Delete) starting$')
	(( HAS_SCANS ))   && any_start_parts+=('^Scan :: .+ starting$' \
	                                       '^BuildIndex :: .+ starting$' \
	                                       '^RemoveIndex :: .+ starting$')
	(( HAS_BATCHES )) && any_start_parts+=('^Batch(Create|Read|Update|Delete)::.+ starting$')
	any_start_re=""
	for i in "${!any_start_parts[@]}"; do
		(( i > 0 )) && any_start_re+="|"
		any_start_re+="${any_start_parts[$i]}"
	done
	any_start_re="(${any_start_re})"

	while (( ! SHOULD_STOP_LOOP )); do
		if ! kill -0 "$CRUD_PID" 2>/dev/null; then break; fi
		wait_for_line "$any_start_re" '^Benchmark complete$'
		case $? in
			0) ;;
			*) break ;;
		esac

		name=""; took_pat=""
		if [[ "$MATCH_LINE" =~ ^(Create|Read|Update|Delete)\ starting$ ]]; then
			op=${BASH_REMATCH[1]}
			name="crud-${op,,}"
			took_pat="^${op} took"
		elif [[ "$MATCH_LINE" =~ ^Scan\ ::\ (.+)\ starting$ ]]; then
			body=${BASH_REMATCH[1]}
			name="scan-$(slugify "$body")"
			took_pat="^Scan :: $(escape_re "$body") took"
		elif [[ "$MATCH_LINE" =~ ^BuildIndex\ ::\ (.+)\ starting$ ]]; then
			id=${BASH_REMATCH[1]}
			name="scan-build-index-$(slugify "$id")"
			took_pat="^BuildIndex :: $(escape_re "$id") took"
		elif [[ "$MATCH_LINE" =~ ^RemoveIndex\ ::\ (.+)\ starting$ ]]; then
			id=${BASH_REMATCH[1]}
			name="scan-remove-index-$(slugify "$id")"
			took_pat="^RemoveIndex :: $(escape_re "$id") took"
		elif [[ "$MATCH_LINE" =~ ^Batch(Create|Read|Update|Delete)::(.+)\ starting$ ]]; then
			op=${BASH_REMATCH[1]}; bname=${BASH_REMATCH[2]}
			name="batch-${op,,}-$(slugify "$bname")"
			took_pat="^Batch${op}::$(escape_re "$bname") took"
		else
			warn "Unrecognised starting marker: $MATCH_LINE"
			continue
		fi
		capture_window "$name" "$took_pat"
	done

	log "      Waiting for crud-bench to finish remaining phases..."
else
	log "[5/6] Waiting for crud-bench to finish..."
fi

wait "$CRUD_PID" || warn "crud-bench exited non-zero — see $CRUD_LOG"

# -----------------------------------------------------------------------------
# 6) Render flamegraphs (profiling mode only) — one per captured phase.
# -----------------------------------------------------------------------------
FLAME_SVGS=()
if [[ "$MODE" == "profiling" ]]; then
	if (( ${#PERF_DATA_FILES[@]} == 0 )); then
		warn "[6/6] No perf data captured — skipping flamegraph rendering"
	else
		log "[6/6] Rendering ${#PERF_DATA_FILES[@]} flamegraph(s)"
		for data_file in "${PERF_DATA_FILES[@]}"; do
			phase=$(basename "$data_file" .data)
			phase=${phase#perf-}
			svg="$OUTPUT_DIR/flamegraph-${phase}.svg"
			if (cd "$OUTPUT_DIR" && "$FLAMEGRAPH_BIN" \
					--perfdata "$(basename "$data_file")" \
					-o "$(basename "$svg")"); then
				FLAME_SVGS+=("$svg")
			else
				warn "[$phase] flamegraph rendering failed — $data_file left in place"
			fi
		done
	fi
else
	log "[6/6] Skipping flamegraph (release mode)"
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo
echo "================================================================"
log "Done ($MODE mode)"
echo "================================================================"
printf "  %-14s %s\n" "crud-bench:"  "$CRUD_LOG"
printf "  %-14s %s\n" "surreal:"     "$SURREAL_LOG"
if [[ "$MODE" == "profiling" ]]; then
	for svg in "${FLAME_SVGS[@]}"; do
		phase=$(basename "$svg" .svg)
		phase=${phase#flamegraph-}
		data_file="$OUTPUT_DIR/perf-${phase}.data"
		size="?"
		[[ -s "$data_file" ]] && size=$(du -h "$data_file" | cut -f1)
		printf "  %-14s %s  (perf.data %s)\n" "[$phase]" "$svg" "$size"
	done
fi
echo
grep -E '(Benchmark (starting|complete)|(Create|Read|Update|Delete|Compaction|BuildIndex|RemoveIndex).*(starting|took)|(Scan ::).*(starting|took)|(Batch[A-Za-z]*::[^[:space:]]+) (starting|took)|ScanWithWrites::.*(starting|took))' \
	"$CRUD_LOG" | sed 's/^/  /' || true
echo
if (( ${#FLAME_SVGS[@]} > 0 )); then
	echo "Open in a browser:"
	for svg in "${FLAME_SVGS[@]}"; do
		echo "  xdg-open '$svg'"
	done
	echo
fi
