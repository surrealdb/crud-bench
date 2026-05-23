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
#   BuildIndex :: <id> :: <name> / RemoveIndex :: <id> :: <name>      (index DDL)
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
	printf "  %-14s %s Hz  (--call-graph fp, --control fifo, --switch-output)\n" \
	       "perf:" "$PERF_FREQ"
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
PERF_CTL_FIFO=""
PERF_CTL_FD=""
PERF_DATA_BASE=""
PERF_LOG=""

cleanup() {
	local rc=$?
	set +e
	if [[ -n "$PERF_PID" ]] && kill -0 "$PERF_PID" 2>/dev/null; then
		log "Stopping perf ($PERF_PID)..."
		# Prefer a clean 'quit' via the control fifo so perf flushes its
		# .data file; fall back to SIGINT if the fifo write fails or perf
		# doesn't exit promptly.
		if [[ -n "$PERF_CTL_FD" ]]; then
			printf 'quit\n' >&"$PERF_CTL_FD" 2>/dev/null
			local _i
			for _i in {1..40}; do
				kill -0 "$PERF_PID" 2>/dev/null || break
				sleep 0.05
			done
		fi
		if kill -0 "$PERF_PID" 2>/dev/null; then
			kill -INT "$PERF_PID" 2>/dev/null
		fi
		wait "$PERF_PID" 2>/dev/null
	fi
	if [[ -n "$PERF_CTL_FIFO" && -p "$PERF_CTL_FIFO" ]]; then
		rm -f "$PERF_CTL_FIFO"
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
#    In profiling mode we attach a single long-running perf to SurrealDB
#    and enable/disable sampling around each phase via perf's control
#    fifo, rotating the perf.data file on each `Server idle` marker so
#    every phase gets its own perf-<phase>.data file. See
#    start_perf_session for the SIG_IGN race that motivates the
#    single-perf design.
#
#    Phase order printed by crud-bench (each followed by `Server idle`
#    once quiesce confirms the server has drained):
#      "Create took …"  →  "Read took …"  →  "Update took …"
#      → ["Scan ::<ctx> took …", …]
#      → ["BuildIndex took …", "Scan :: … took …", "RemoveIndex took …"]
#      → "Delete took …"
#      → ["Batch… took …", …]
#
#    In release mode we just wait for crud-bench to exit.
# -----------------------------------------------------------------------------

# One long-running `perf record` attached to SurrealDB for the entire
# benchmark — orchestrated via `perf record --control fifo:$CTL`
# (enable/disable to gate sampling) and `--switch-output` (SIGUSR2 to
# rotate the active perf.data into a per-phase file).
#
# Why one perf, not one-per-phase: starting perf each phase races against
# bash's SIG_IGN setup window (perf inherits SIG_IGN on SIGINT until it
# installs its own handler ~1s in, which is longer than several batch
# phases). The old per-phase design's `stop_perf` retry loop bridged that
# race but meant short phases blocked the streaming reader for ~1s every
# time, which let crud-bench race ahead and shifted later phases' perf
# windows forward — by the last 1-2 phases the window opened so late it
# captured no samples at all (the empty `perf-batch-update-1000.data` /
# `perf-batch-delete-1000.data` symptoms we were debugging).
#
# `--delay=-1` starts perf in disabled state — sampling only enables when
# we write `enable` to the control fifo. No SIG_IGN race anywhere; perf
# stops exactly once, at benchmark end, via `quit` on the control fifo.
start_perf_session() {
	PERF_CTL_FIFO="$OUTPUT_DIR/perf.ctl"
	PERF_DATA_BASE="$OUTPUT_DIR/perf.data"
	PERF_LOG="$OUTPUT_DIR/perf.log"
	mkfifo "$PERF_CTL_FIFO"
	# Open the control fifo read-write from the script so per_ctl writes
	# don't block / EOF on perf between commands.
	exec {PERF_CTL_FD}<>"$PERF_CTL_FIFO"
	log "      Starting perf (pid=$SURREAL_PID, freq=${PERF_FREQ}Hz)"
	perf record \
		-F "$PERF_FREQ" \
		--call-graph fp \
		-g \
		-p "$SURREAL_PID" \
		--control "fifo:$PERF_CTL_FIFO" \
		--switch-output \
		--delay=-1 \
		-o "$PERF_DATA_BASE" \
		> "$PERF_LOG" 2>&1 &
	PERF_PID=$!
	# Give perf a beat to open the fifo + arm itself before we start
	# writing commands. (perf opens the control fifo lazily during its
	# event loop init; missing this can drop the first `enable`.)
	local i
	for i in {1..40}; do
		[[ -f "$PERF_DATA_BASE" ]] && return 0
		if ! kill -0 "$PERF_PID" 2>/dev/null; then
			warn "perf exited during startup; tail of $PERF_LOG:"
			tail -20 "$PERF_LOG" >&2 || true
			die "perf failed to start"
		fi
		sleep 0.05
	done
	warn "perf did not create $PERF_DATA_BASE within 2s — continuing anyway"
}

# Write one newline-terminated command into perf's control fifo. perf
# accepts: enable | disable | snapshot | evlist | quit.
perf_ctl() {
	[[ -z "$PERF_CTL_FD" ]] && return 0
	printf '%s\n' "$1" >&"$PERF_CTL_FD"
}

# List rotated perf.data files (`perf.data.<ts>`) in mtime order. Used to
# pick the newly-rotated file after each SIGUSR2.
list_rotated_perf_files() {
	# `find -printf` so we can sort by mtime with second resolution; for
	# the rare same-second case the filename's timestamp suffix breaks
	# the tie (perf names them `perf.data.YYYYMMDDhhmmss[NN]`).
	find "$OUTPUT_DIR" -maxdepth 1 -name 'perf.data.*' -printf '%T@ %p\n' 2>/dev/null \
		| sort -k1,1n -k2,2 \
		| awk '{ $1=""; sub(/^ /, ""); print }'
}

stop_perf_session() {
	if [[ -z "$PERF_PID" ]]; then return 0; fi
	local pid=$PERF_PID
	PERF_PID=""
	if kill -0 "$pid" 2>/dev/null; then
		perf_ctl quit 2>/dev/null || true
		local i
		for i in {1..200}; do  # ~10s
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.05
		done
		if kill -0 "$pid" 2>/dev/null; then
			warn "perf didn't exit on 'quit' — sending SIGINT"
			kill -INT "$pid" 2>/dev/null || true
		fi
	fi
	wait "$pid" 2>/dev/null || true
	if [[ -n "$PERF_CTL_FD" ]]; then
		eval "exec ${PERF_CTL_FD}>&-"
		PERF_CTL_FD=""
	fi
	[[ -n "$PERF_CTL_FIFO" && -p "$PERF_CTL_FIFO" ]] && rm -f "$PERF_CTL_FIFO"
}

# -----------------------------------------------------------------------------
# Per-line observer helpers.
#
# The profiling loop streams the crud-bench log line-by-line so each phase
# can open its own perf window — every Scan, BuildIndex, and Batch run
# gets a distinct marker (rich `--emit-phase-markers` labels carry the
# scan id / batch name).
# -----------------------------------------------------------------------------

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

# Maps a "starting" log line to (name, took_pat) via NAME / TOOK_PAT
# globals. Returns 0 on a recognised marker, 1 otherwise.
NAME=""
TOOK_PAT=""
parse_start_marker() {
	local line=$1
	NAME=""; TOOK_PAT=""
	if [[ "$line" =~ ^(Create|Read|Update|Delete)\ starting$ ]]; then
		(( HAS_CRUD )) || return 1
		local op=${BASH_REMATCH[1]}
		NAME="crud-${op,,}"
		TOOK_PAT="^${op} took"
	elif [[ "$line" =~ ^Scan\ ::\ (.+)\ starting$ ]]; then
		(( HAS_SCANS )) || return 1
		local body=${BASH_REMATCH[1]}
		NAME="scan-$(slugify "$body")"
		TOOK_PAT="^Scan :: $(escape_re "$body") took"
	elif [[ "$line" =~ ^BuildIndex\ ::\ (.+)\ starting$ ]]; then
		# Rich marker is `BuildIndex :: <id> :: <name> starting` — the
		# `<id> :: <name>` combo is what disambiguates the count-vs-select
		# query-shape variants under the same scan id (slugify produces
		# different filenames for each).
		(( HAS_SCANS )) || return 1
		local body=${BASH_REMATCH[1]}
		NAME="scan-build-index-$(slugify "$body")"
		TOOK_PAT="^BuildIndex :: $(escape_re "$body") took"
	elif [[ "$line" =~ ^RemoveIndex\ ::\ (.+)\ starting$ ]]; then
		# Same shape as BuildIndex above; see comment there.
		(( HAS_SCANS )) || return 1
		local body=${BASH_REMATCH[1]}
		NAME="scan-remove-index-$(slugify "$body")"
		TOOK_PAT="^RemoveIndex :: $(escape_re "$body") took"
	elif [[ "$line" =~ ^Batch(Create|Read|Update|Delete)::(.+)\ starting$ ]]; then
		(( HAS_BATCHES )) || return 1
		local op=${BASH_REMATCH[1]} bname=${BASH_REMATCH[2]}
		NAME="batch-${op,,}-$(slugify "$bname")"
		TOOK_PAT="^Batch${op}::$(escape_re "$bname") took"
	else
		return 1
	fi
	return 0
}

# Phases captured in execution order. Each entry is the slug we use for
# the matching `perf-<slug>.data` / `flamegraph-<slug>.svg`. Combined
# flamegraph rendering also walks this list to preserve order.
PERF_DATA_FILES=()
PERF_PHASE_NAMES=()

# Currently-open perf window. We track `took_pat` only so the streaming
# loop can warn if a `<phase> took` line ever arrives without the
# matching `Server idle` (which would indicate the Rust-side
# quiesce_and_mark wiring is broken on some code path).
ACTIVE_WINDOW=""
ACTIVE_TOOK_PAT=""
ACTIVE_START_TS=0

# Open a new perf window for $1 (slug) with $2 as the took regex.
# Enables perf sampling via the control fifo.
open_window() {
	local name=$1 took_pat=$2
	ACTIVE_WINDOW=$name
	ACTIVE_TOOK_PAT=$took_pat
	ACTIVE_START_TS=$(date +%s)
	log "      [$name] enabling perf"
	perf_ctl enable
}

# Close the active window: disable sampling, rotate perf.data via
# SIGUSR2, and rename the newly-rotated file to `perf-<phase>.data`.
# $1 is an optional note explaining how the window closed (e.g.
# "" for a clean Server idle, " (Benchmark complete before Server idle)"
# at end-of-run, " (forced close before next phase: …)" for the safety
# net).
close_window() {
	[[ -z "$ACTIVE_WINDOW" ]] && return 0
	local note=${1:-}
	local elapsed=$(( $(date +%s) - ACTIVE_START_TS ))
	local name=$ACTIVE_WINDOW
	ACTIVE_WINDOW=""
	ACTIVE_TOOK_PAT=""
	ACTIVE_START_TS=0

	perf_ctl disable
	# Snapshot the rotated-file set, fire SIGUSR2, then wait for a new
	# rotated file to appear (bounded). With --switch-output, USR2
	# closes the active perf.data, renames it to perf.data.<ts>, and
	# opens a fresh perf.data for subsequent samples.
	local before_count
	before_count=$(list_rotated_perf_files | wc -l | tr -d ' ')
	kill -USR2 "$PERF_PID" 2>/dev/null || true
	local i now_count
	for i in {1..40}; do  # ~2s
		now_count=$(list_rotated_perf_files | wc -l | tr -d ' ')
		(( now_count > before_count )) && break
		sleep 0.05
	done

	local newest
	newest=$(list_rotated_perf_files | tail -1)
	if [[ -n "$newest" && -s "$newest" ]]; then
		# Wait for the rotated file's size to stabilise before mv'ing.
		# Empirically perf can still be flushing the AUX buffer after
		# the rename — taking a partial file produces a .data that opens
		# but contains no complete sample records, which renders as the
		# 611-byte "ERROR: No valid input provided to flamegraph" SVG.
		# We poll the size every 50ms and consider the file stable when
		# we see the same size on two consecutive checks (bounded ~3s).
		local prev_size=-1 cur_size stable=0
		for i in {1..60}; do
			cur_size=$(wc -c < "$newest" 2>/dev/null | tr -d ' ')
			if [[ "$cur_size" == "$prev_size" ]]; then
				stable=1
				break
			fi
			prev_size=$cur_size
			sleep 0.05
		done
		(( stable )) || warn "[$name] rotated file size still changing after 3s — mv'ing anyway"
		local dest="$OUTPUT_DIR/perf-${name}.data"
		mv "$newest" "$dest"
		PERF_DATA_FILES+=("$dest")
		PERF_PHASE_NAMES+=("$name")
		log "      [$name] disabled perf, rotated → $(basename "$dest") (captured ~${elapsed}s${note})"
		if (( elapsed < 2 )) && [[ -z "$note" ]]; then
			warn "[$name] window was only ${elapsed}s — flamegraph will be sparse at ${PERF_FREQ}Hz"
		fi
	else
		warn "[$name] perf rotation produced no new file (see $PERF_LOG)"
	fi
}

if [[ "$MODE" == "profiling" ]]; then
	log "[5/6] Streaming crud-bench log; profiling categories: ${CATEGORIES[*]}"

	# Attach perf once for the whole benchmark — see start_perf_session
	# for why we no longer launch perf per phase.
	start_perf_session

	# Tail the crud-bench log line-by-line as it's being written. The
	# `--pid=$CRUD_PID` flag makes `tail -F` exit as soon as crud-bench
	# does, so the `while read` loop never wedges on a process that's
	# already gone.
	#
	# Phase boundaries (all emitted only under --emit-phase-markers):
	#   `<name> starting`  → enable perf sampling for this phase
	#   `<name> took …`    → client-side phase complete; server may still
	#                        be draining (open snapshots, deferred tasks).
	#                        We KEEP sampling so that tail is attributed
	#                        to this phase, not the next one.
	#   `Server idle`      → server-side drain confirmed by quiesce(); now
	#                        disable + rotate so the just-closed
	#                        perf.data is finalised as this phase's file.
	while IFS= read -r line; do
		# Benchmark complete is the terminal marker — close any open
		# window cleanly and stop processing further log lines.
		if [[ "$line" == "Benchmark complete" ]]; then
			close_window " (Benchmark complete before Server idle)"
			break
		fi
		# Server-quiesced marker for the currently open window → close it.
		if [[ "$line" == "Server idle" ]]; then
			if [[ -n "$ACTIVE_WINDOW" ]]; then
				close_window ""
			else
				warn "Server idle with no active perf window — ignoring"
			fi
			continue
		fi
		# Starting marker → open a new window (or force-close a stale
		# one first). The force-close path should never trigger: every
		# crud-bench phase pairs `<name> starting` with a `Server idle`
		# emitted from quiesce_and_mark. If we ever see it, the Rust
		# side dropped a `Server idle` somewhere — rotate now so two
		# phases' samples don't fold into one file.
		if ! parse_start_marker "$line"; then
			continue
		fi
		if [[ -n "$ACTIVE_WINDOW" ]]; then
			warn "[$ACTIVE_WINDOW] no Server idle before next starting line — force-closing"
			close_window " (forced close before next phase: $NAME)"
		fi
		open_window "$NAME" "$TOOK_PAT"
	done < <(tail -n +1 -F --pid="$CRUD_PID" "$CRUD_LOG" 2>/dev/null)

	# crud-bench can exit before emitting "Benchmark complete" (e.g.
	# an unsupported op or a worker error) — make sure any still-open
	# perf window is finalised and perf itself is stopped cleanly.
	close_window " (crud-bench exited before Server idle)"
	stop_perf_session

	log "      Waiting for crud-bench to finish remaining phases..."
else
	log "[5/6] Waiting for crud-bench to finish..."
fi

wait "$CRUD_PID" || warn "crud-bench exited non-zero — see $CRUD_LOG"

# -----------------------------------------------------------------------------
# 6) Render flamegraphs (profiling mode only) — one per captured phase,
#    plus a combined per-phase-bands SVG if inferno's standalone CLI
#    tools are available.
# -----------------------------------------------------------------------------
FLAME_SVGS=()
COMBINED_SVG=""
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

		# Combined per-phase-bands flamegraph: each phase becomes its
		# own top-level frame at the bottom of the SVG, with the phase's
		# stacks above it. Useful for spotting where wall time goes
		# across the whole benchmark in one view.
		#
		# Needs inferno's standalone CLI (`cargo install inferno`) —
		# cargo-flamegraph's wrapper doesn't expose the folded-stacks
		# intermediate format. Skip with a hint if not installed.
		if command -v inferno-collapse-perf >/dev/null \
			&& command -v inferno-flamegraph >/dev/null
		then
			log "      Rendering combined per-phase-bands flamegraph"
			combined_folded="$OUTPUT_DIR/perf-all-phases.folded"
			: > "$combined_folded"
			combine_ok=1
			for i in "${!PERF_DATA_FILES[@]}"; do
				data_file=${PERF_DATA_FILES[$i]}
				phase=${PERF_PHASE_NAMES[$i]}
				# `perf script` decodes per-sample stacks; pipe through
				# inferno's collapse, then prepend the phase slug as a
				# synthetic root frame so the combined SVG segments by
				# phase. `2>/dev/null` swallows perf's noisy per-sample
				# warnings — they don't affect collapse output.
				if ! perf script -i "$data_file" 2>/dev/null \
					| inferno-collapse-perf \
					| awk -v p="$phase" '{ print p ";" $0 }' \
					>> "$combined_folded"
				then
					warn "[$phase] failed to collapse stacks — combined SVG skipped"
					combine_ok=0
					break
				fi
			done
			if (( combine_ok )) && [[ -s "$combined_folded" ]]; then
				COMBINED_SVG="$OUTPUT_DIR/flamegraph-all-phases.svg"
				if ! inferno-flamegraph \
					--title "crud-bench — all phases (dev-$MODE-$TS)" \
					< "$combined_folded" > "$COMBINED_SVG"
				then
					warn "Combined flamegraph rendering failed — folded stacks left at $combined_folded"
					COMBINED_SVG=""
				fi
			else
				COMBINED_SVG=""
			fi
		else
			log "      (skipping combined flamegraph — install inferno CLI to enable: cargo install inferno)"
		fi
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
	if [[ -n "$COMBINED_SVG" ]]; then
		printf "  %-14s %s\n" "[all-phases]" "$COMBINED_SVG"
	fi
fi
echo
grep -E '(Benchmark (starting|complete)|Server idle|(Create|Read|Update|Delete|Compaction|BuildIndex|RemoveIndex).*(starting|took)|(Scan ::).*(starting|took)|(Batch[A-Za-z]*::[^[:space:]]+) (starting|took)|ScanWithWrites::.*(starting|took))' \
	"$CRUD_LOG" | sed 's/^/  /' || true
echo
if (( ${#FLAME_SVGS[@]} > 0 )); then
	echo "Open in a browser:"
	for svg in "${FLAME_SVGS[@]}"; do
		echo "  xdg-open '$svg'"
	done
	if [[ -n "$COMBINED_SVG" ]]; then
		echo "  xdg-open '$COMBINED_SVG'"
	fi
	echo
fi
