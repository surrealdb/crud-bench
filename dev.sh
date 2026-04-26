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
# In profiling mode the script watches crud-bench's log for lifecycle and
# phase markers and brackets a separate perf window per requested phase.
#
# crud-bench emits:
#   "Connecting to datastore" / "Datastore ready"
#   "Setting up N client(s)"  / "Benchmark starting"
#   "<Operation> starting"    / "<Operation> took …"     (once per phase)
#   "Benchmark complete"      / "Disconnecting from datastore"
#
# Phases always run in a fixed order:
#
#     Create → Read → Update → Scan(s) [→ BuildIndex → ScanWithIndex →
#                                          RemoveIndex] → Delete → Batches
#
# Supported phase names for PHASES:
#
#     create        "Create starting"         → "Create took …"
#     read          "Read starting"           → "Read took …"
#     update        "Update starting"         → "Update took …"
#     scan          "Scan::… starting"        → "Scan::… took …"         (first match)
#     delete        "Delete starting"         → "Delete took …"
#     batch         first "Batch…:: starting" → "Benchmark complete"     (all batches)
#     all           "Benchmark starting"      → crud-bench exit           (end-to-end)
#
# You can pass a comma-separated list, e.g. PHASES="create,update". If
# "delete" or "update" is requested without "scan", the script
# automatically passes --skip-scans / --skip-indexes to crud-bench so the
# window stays tight. --skip-batches is passed unless PHASES contains
# "all" or any "batch*" phase.
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
#   PHASES            Comma-sep phases to profile        (prompted, profiling mode)
#                     Any of: create, read, update, scan, delete,
#                             batch, all
#   KEY_TYPE          Primary key type                   (default: integer)
#   DB_PATH           RocksDB data dir for the server    (default: ./data next to dev.sh)
#   OUTPUT_DIR        Where logs/perf/flamegraph go      (default: ./dev-results-<mode>-<ts>)
#   SURREAL_PORT      TCP port for SurrealDB             (default: 8000)
#   PERF_FREQ         perf sampling frequency (Hz)       (default: 997)
#   PERF_MAX_SECS     Hard cap on each perf window       (default: 600)
#   CRUD_BENCH_VALUE  Value template (inline JSON or @path)
#   CRUD_BENCH_SCANS  Scan spec       (inline JSON or @path)
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
# One perf window + one flamegraph is produced for each phase in PHASES.
# Keys mapped to (start, stop) regexes over the crud-bench log; an empty
# start means "attach as soon as crud-bench starts".
VALID_PHASES="create read update scan delete batch all"

declare -A PHASE_START=(
	[create]='^Create starting'
	[read]='^Read starting'
	[update]='^Update starting'
	[scan]='^Scan::[A-Za-z0-9_]+ starting'
	[delete]='^Delete starting'
	[batch]='^Batch[A-Za-z]+::[A-Za-z0-9_]+ starting'
	[all]='^Benchmark starting'
)
# An empty PHASE_STOP value means "stop when crud-bench exits" (used by
# the `all` phase so batches/compaction-after-delete are included).
declare -A PHASE_STOP=(
	[create]='^Create took'
	[read]='^Read took'
	[update]='^Update took'
	[scan]='Scan::[A-Za-z0-9_]+ took'
	[delete]='^Delete took'
	[batch]='^Benchmark complete'
	[all]=''
)

PHASE_LIST=()
if [[ "$MODE" == "profiling" ]]; then
	while true; do
		ask PHASES "Phases to profile (comma-sep: $VALID_PHASES)" "scan"
		IFS=',' read -ra __req <<<"$PHASES"
		PHASE_LIST=()
		__ok=1
		for __p in "${__req[@]}"; do
			__p="${__p// /}"
			[[ -z "$__p" ]] && continue
			if [[ -z "${PHASE_STOP[$__p]+x}" ]]; then
				warn "Unknown phase: '$__p' (valid: $VALID_PHASES)"
				__ok=0
				break
			fi
			PHASE_LIST+=("$__p")
		done
		if (( __ok )) && (( ${#PHASE_LIST[@]} > 0 )); then
			break
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
# Derive crud-bench --skip-* flags from PHASE_LIST.
#
# Rationale:
#   - Scans / indexes / batches run between Update and Delete. If they're
#     included while we're trying to profile, say, "update" or "delete",
#     the perf window either has to wait through them or stops at the
#     wrong marker. So we skip them unless they've been explicitly asked
#     for (scan) or the user wants everything (all).
#   - There's no CLI flag to skip Read; it always runs. The "read" phase
#     just profiles it in place.
# -----------------------------------------------------------------------------
CRUD_SKIP_ARGS=()
if [[ "$MODE" == "profiling" ]]; then
	phases_has() {
		local needle=$1 p
		for p in "${PHASE_LIST[@]}"; do [[ "$p" == "$needle" ]] && return 0; done
		return 1
	}
	phases_has_prefix() {
		local prefix=$1 p
		for p in "${PHASE_LIST[@]}"; do [[ "$p" == "$prefix"* ]] && return 0; done
		return 1
	}
	if ! phases_has scan && ! phases_has all; then
		CRUD_SKIP_ARGS+=(--skip-scans --skip-indexes)
	fi
	if ! phases_has all && ! phases_has_prefix batch; then
		CRUD_SKIP_ARGS+=(--skip-batches)
	fi
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
	printf "  %-14s %s\n" "Phases:"  "${PHASE_LIST[*]}"
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
(
	cd "$CRUD_BENCH_DIR"
	stdbuf -oL -eL "$CRUD_BIN" \
		-d surrealdb -e "ws://127.0.0.1:$SURREAL_PORT" \
		-s "$SAMPLES" -c "$CLIENTS" -t "$THREADS" -k "$KEY_TYPE" \
		-n "dev-$MODE-$TS" -r \
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
#      → ["Scan::<name> took …", …]
#      → ["BuildIndex::… took …", "Scan::… took …", "RemoveIndex::… took …"]
#      → "Delete took …"
#      → ["Batch… took …", …]
#
#    In release mode we just wait for crud-bench to exit.
# -----------------------------------------------------------------------------

# Block until $1 (an extended regex) matches somewhere in $CRUD_LOG, or
# until crud-bench exits. Empty pattern returns immediately. Returns 0
# on match, 1 if crud-bench died first.
wait_for_pattern() {
	local pat=$1
	[[ -z "$pat" ]] && return 0
	while true; do
		if grep -Eq "$pat" "$CRUD_LOG" 2>/dev/null; then
			return 0
		fi
		if ! kill -0 "$CRUD_PID" 2>/dev/null; then
			# One last look in case the match landed right before exit.
			grep -Eq "$pat" "$CRUD_LOG" 2>/dev/null && return 0
			return 1
		fi
		sleep 0.5
	done
}

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

PERF_DATA_FILES=()

if [[ "$MODE" == "profiling" ]]; then
	log "[5/6] Profiling phases: ${PHASE_LIST[*]}"

	for phase in "${PHASE_LIST[@]}"; do
		start_pat=${PHASE_START[$phase]}
		stop_pat=${PHASE_STOP[$phase]}
		data_file="$OUTPUT_DIR/perf-${phase}.data"
		log_file="$OUTPUT_DIR/perf-${phase}.log"

		# Wait for the phase's start marker (or start immediately if empty).
		if [[ -n "$start_pat" ]]; then
			log "      [$phase] waiting for /$start_pat/"
			if ! wait_for_pattern "$start_pat"; then
				warn "[$phase] crud-bench exited before start marker appeared — skipping"
				continue
			fi
		fi

		ts_start=$(date +%s)
		log "      [$phase] attaching perf (pid=$SURREAL_PID, -F $PERF_FREQ, out=$(basename "$data_file"))"
		start_perf "$data_file" "$log_file"

		# Wait for the phase's stop marker, or for perf / crud-bench to die.
		# An empty stop_pat means "run until crud-bench exits" (used by `all`).
		matched=0
		while true; do
			if [[ -n "$stop_pat" ]] && grep -Eq "$stop_pat" "$CRUD_LOG" 2>/dev/null; then
				matched=1
				break
			fi
			if [[ -n "$PERF_PID" ]] && ! kill -0 "$PERF_PID" 2>/dev/null; then
				warn "[$phase] perf stopped on its own (hit PERF_MAX_SECS=$PERF_MAX_SECS?)"
				break
			fi
			if ! kill -0 "$CRUD_PID" 2>/dev/null; then
				if [[ -z "$stop_pat" ]]; then
					matched=1
				else
					grep -Eq "$stop_pat" "$CRUD_LOG" 2>/dev/null && matched=1
				fi
				break
			fi
			sleep 0.5
		done

		ts_end=$(date +%s)
		note=""
		(( matched )) || note=', stop marker not seen'
		log "      [$phase] detaching perf (captured ~$((ts_end - ts_start))s${note})"
		stop_perf

		if [[ -s "$data_file" ]]; then
			PERF_DATA_FILES+=("$data_file")
		else
			warn "[$phase] no perf data recorded (see $log_file)"
		fi

		if ! kill -0 "$CRUD_PID" 2>/dev/null; then
			break
		fi
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
grep -E '(Benchmark (starting|complete)|(Create|Read|Update|Delete|Scan::|BuildIndex::|RemoveIndex::|Batch[A-Za-z]*::|Compaction) (starting|took))' \
	"$CRUD_LOG" | sed 's/^/  /' || true
echo
if (( ${#FLAME_SVGS[@]} > 0 )); then
	echo "Open in a browser:"
	for svg in "${FLAME_SVGS[@]}"; do
		echo "  xdg-open '$svg'"
	done
	echo
fi
