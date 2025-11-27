#!/usr/bin/env bash

set -e

# Detect OS
IS_MACOS=false
IS_LINUX=false

case "$(uname -s)" in
    Darwin*)
        IS_MACOS=true
        ;;
    Linux*)
        IS_LINUX=true
        ;;
esac

# Default values
SAMPLES="5000000"
CLIENTS="128"
THREADS="48"
KEY_TYPE="string26"
SYNC="false"
OPTIMISED="false"
TIMEOUT=""
DATASTORE=""
BUILD="true"
DATA_DIR="$(pwd)/data"
NAME=""
NOWAIT="false"

# ============================================================================
# LOGGING FUNCTIONS
# ============================================================================

# Print informational message in blue
# Args: $1 - message to display
log_info() {
	echo -e "\033[0;34m[INFO]\033[0m $1"
}

# Print success message in green
# Args: $1 - message to display
log_success() {
	echo -e "\033[0;32m[SUCCESS]\033[0m $1"
}

# Print warning message in yellow
# Args: $1 - message to display
log_warning() {
	echo -e "\033[1;33m[WARNING]\033[0m $1"
}

# Print error message in red
# Args: $1 - message to display
log_error() {
	echo -e "\033[0;31m[ERROR]\033[0m $1"
}

# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

# Display usage information and available options
# Outputs help text with examples and available datastores
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run benchmarks with optimized system configuration.

OPTIONS:
    -d, --datastore <name>    Database to benchmark (required)
                              Use 'all' to run all enabled databases
                              Use comma-separated list for multiple databases
    -s, --samples <num>       Number of samples (default: 5000000)
    -c, --clients <num>       Number of clients (default: 128)
    -t, --threads <num>       Number of threads (default: 48)
    -k, --key <type>          Primary key type (default: string26)
                              Options: integer, string26, string90, string250, string506
    --name <name>             Custom name for this benchmark run (default: database name)
    --sync                    Acknowledge disk writes (default: false)
    --optimised               Use optimised database configurations (default: false)
    --timeout <minutes>       Timeout in minutes (default: none)
    --no-build                Skip the cargo build step
    --no-wait                 Skip waiting for system load to drop (default: false)
    --data-dir <path>         Data directory path (default: ./data)
    -h, --help                Show this help message

EXAMPLES:
    # Run all database benchmarks
    $0 -d all

    # Run benchmark for postgres
    $0 -d postgres

    # Run with optimised configurations
    $0 -d postgres --optimised

    # Run with a 2 hour timeout
    $0 -d postgres --timeout 120

    # Run multiple specific databases
    $0 -d postgres,mysql,mongodb

    # Run with custom benchmark name
    $0 -d postgres --name my-custom-benchmark

    # Run with custom parameters
    $0 -d rocksdb -s 1000000 -c 64 -t 24 --sync --optimised

AVAILABLE DATASTORES:
    arangodb, dragonfly, dry, fjall, keydb, lmdb, map, mdbx, mongodb,
    mysql, neo4j, postgres, redb, redis, rocksdb, sqlite,
    surrealdb-memory, surrealdb-rocksdb, surrealdb-surrealkv,
    surrealdb-embedded-memory, surrealdb-embedded-rocksdb,
    surrealdb-embedded-surrealkv, surrealkv, surrealmx

ENVIRONMENT VARIABLES:
    CRUD_BENCH_VALUE
        Configure the data structure/value template for benchmarks.
        JSON format defining fields with generators (e.g., "text:50", "int:1..5000").

    CRUD_BENCH_SCANS
        Configure scan operations for benchmarks.
        JSON array of scan configurations with name, samples, and projection.

    CRUD_BENCH_BATCHES
        Configure batch operations for benchmarks.
        JSON array of batch configurations with operation, batch_size, and samples.

EOF
}

# Parse and validate command line arguments
# Sets global variables based on provided options
# Args: $@ - all command line arguments
# Exits with error if required arguments are missing
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--datastore)
                DATASTORE="$2"
                shift 2
                ;;
            -s|--samples)
                SAMPLES="$2"
                shift 2
                ;;
            -c|--clients)
                CLIENTS="$2"
                shift 2
                ;;
            -t|--threads)
                THREADS="$2"
                shift 2
                ;;
            -k|--key)
                KEY_TYPE="$2"
                shift 2
                ;;
            --name)
                NAME="$2"
                shift 2
                ;;
            --sync)
                SYNC="true"
                shift
                ;;
            --optimised)
                OPTIMISED="true"
                shift
                ;;
            --timeout)
                TIMEOUT="$2"
                shift 2
                ;;
            --no-build)
                BUILD="false"
                shift
                ;;
            --no-wait)
                NOWAIT="true"
                shift
                ;;
            --data-dir)
                DATA_DIR="$2"
                shift 2
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done

    if [[ -z "$DATASTORE" ]]; then
        log_error "Datastore is required"
        show_usage
        exit 1
    fi
}

# ============================================================================
# OS-SPECIFIC UTILITY FUNCTIONS
# ============================================================================

# Get the number of available CPU cores
# Returns: CPU count for current platform (macOS or Linux)

get_cpu_count() {
    if [[ "$IS_MACOS" == "true" ]]; then
        sysctl -n hw.ncpu
    else
        nproc
    fi
}

# Get the current system load average (1 minute)
# Returns: Load average as a decimal number

get_load_average() {
    if [[ "$IS_MACOS" == "true" ]]; then
        sysctl -n vm.loadavg | awk '{print $2}'
    else
        awk '{print $1}' /proc/loadavg
    fi
}

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

# Database configuration matrix (similar to benchmark.yml)
# Format: name|database|category|description|endpoint
#
# Fields:
#   - name: Database configuration identifier
#   - database: Actual binary name to invoke
#   - category: "embedded" (runs in process) or "networked" (runs in Docker)
#   - description: Human-readable name
#   - endpoint: Additional CLI flags (e.g., "-e memory")
#
# Categories:
#   - embedded: Uses nice -n -20 ionice -c 1 -n 0 (highest priority)
#   - networked: Uses nice -n -10 ionice -c 2 -n 0 (normal priority)
DATABASE_MATRIX="
arangodb|arangodb|networked|ArangoDB|
dragonfly|dragonfly|networked|Dragonfly|
dry|dry|embedded|Dry|
fjall|fjall|embedded|Fjall|
keydb|keydb|networked|KeyDB|
lmdb|lmdb|embedded|LMDB|
map|map|embedded|Map|
mdbx|mdbx|embedded|MDBX|
mongodb|mongodb|networked|MongoDB|
mysql|mysql|networked|MySQL|
neo4j|neo4j|networked|Neo4j|
postgres|postgres|networked|Postgres|
redb|redb|embedded|ReDB|
redis|redis|networked|Redis|
rocksdb|rocksdb|embedded|RocksDB|
sqlite|sqlite|embedded|SQLite|
surrealdb-memory|surrealdb-memory|networked|SurrealDB with in-memory storage|
surrealdb-rocksdb|surrealdb-rocksdb|networked|SurrealDB with RocksDB storage|
surrealdb-surrealkv|surrealdb-surrealkv|networked|SurrealDB with SurrealKV storage|
surrealdb-embedded-memory|surrealdb|embedded|SurrealDB embedded with in-memory storage|-e memory
surrealdb-embedded-rocksdb|surrealdb|embedded|SurrealDB embedded with RocksDB storage|-e rocksdb:DATA_DIR
surrealdb-embedded-surrealkv|surrealdb|embedded|SurrealDB embedded with SurrealKV storage|-e surrealkv:DATA_DIR
surrealkv|surrealkv|embedded|SurrealKV|
surrealmx|surrealmx|embedded|SurrealMX|
"

# Retrieve a specific property for a database from the configuration matrix
# Args:
#   $1 - database name (e.g., "postgres", "surrealdb-embedded-memory")
#   $2 - property field name (name|database|category|description|endpoint)
# Returns: The requested property value, or empty string if not found
# Note: Handles DATA_DIR placeholder substitution for endpoint fields
get_db_property() {
    local name=$1
    local field=$2

    # Field indices: 0=name, 1=database, 2=category, 3=description, 4=endpoint
    local field_index
    case $field in
        name) field_index=0 ;;
        database) field_index=1 ;;
        category) field_index=2 ;;
        description) field_index=3 ;;
        endpoint) field_index=4 ;;
        *) return 1 ;;
    esac

    # Search for the database in the matrix
    echo "$DATABASE_MATRIX" | grep -v '^$' | while IFS='|' read -r db_name db_database db_category db_desc db_endpoint; do
        if [[ "$db_name" == "$name" ]]; then
            case $field_index in
                0) echo "$db_name" ;;
                1) echo "$db_database" ;;
                2) echo "$db_category" ;;
                3) echo "$db_desc" ;;
                4)
                    # Handle DATA_DIR placeholder
                    local endpoint_value="$db_endpoint"
                    endpoint_value="${endpoint_value//DATA_DIR/${DATA_DIR}}"
                    echo "$endpoint_value"
                    ;;
            esac
            return 0
        fi
    done
}

# Get platform-specific CLI arguments for a database (nice/ionice)
# Args: $1 - database name
# Returns: CLI args appropriate for current OS based on database category
# Categories:
#   - embedded: nice -n -20 ionice -c 1 -n 0 (highest priority, runs in-process)
#   - networked: nice -n -10 ionice -c 2 -n 0 (normal priority, runs in Docker)
get_db_cli_args() {
    local db=$1
    local category=$(get_db_property "$db" "category")

    if [[ "$category" == "embedded" ]]; then
        # Embedded databases: highest priority
        if [[ "$IS_MACOS" == "true" ]]; then
            echo "nice -n -20"
        else
            echo "nice -n -20 ionice -c 1 -n 0"
        fi
    else
        # Networked databases: normal priority
        if [[ "$IS_MACOS" == "true" ]]; then
            echo "nice -n -10"
        else
            echo "nice -n -10 ionice -c 2 -n 0"
        fi
    fi
}

# Get human-readable description for a database
# Args: $1 - database name
# Returns: Description string (e.g., "Postgres", "SurrealDB with RocksDB storage")
get_db_description() {
    get_db_property "$1" "description"
}

# Get additional endpoint arguments for a database if needed
# Args: $1 - database name
# Returns: Endpoint flags (e.g., "-e memory", "-e rocksdb:/data/crud-bench")
get_db_endpoint() {
    get_db_property "$1" "endpoint"
}

# Get the actual database binary name for CLI invocation
# Args: $1 - database name
# Returns: Binary name (e.g., "postgres", "surrealdb")
# Note: Handles cases where multiple configs use the same binary (e.g., surrealdb-embedded-*)
get_db_name() {
    get_db_property "$1" "database"
}

# Get a space-separated list of all configured database names
# Returns: All database names from the DATABASE_MATRIX
get_all_databases() {
    echo "$DATABASE_MATRIX" | grep -v '^$' | cut -d'|' -f1 | tr '\n' ' '
}

# ============================================================================
# BUILD TOOLING
# ============================================================================

# Build the crud-bench binary in release mode for the current platform
# Respects the BUILD global variable (can be disabled with --no-build)
# Exits with error code 1 if build fails
build_benchmark() {

    if [[ "$BUILD" == "false" ]]; then
        log_info "Skipping build step"
        return 0
    fi

    log_info "Building crud-bench in release mode..."

    if cargo build --release; then
        log_success "Build completed successfully"
    else
        log_error "Build failed"
        exit 1
    fi

}

# ============================================================================
# ENVIRONMENT SETUP
# ============================================================================

# Clean up Docker containers, volumes, and data directories
# Ensures a clean state before running benchmarks
# Operations:
#   - Kills and removes crud-bench Docker containers
#   - Prunes Docker containers, volumes, and system
#   - Recreates data directory with proper permissions
#   - Creates fresh Docker volume
# Note: Result files are preserved to keep benchmark outputs
cleanup_environment() {

    log_info "Cleaning up environment..."

    # Kill and remove any existing crud-bench container
    docker container kill crud-bench &>/dev/null || true
    docker container rm crud-bench &>/dev/null || true

    # Prune stopped containers and volumes
    docker container prune --force &>/dev/null || true
    docker volume prune --all --force &>/dev/null || true

    # Clean up system
    docker system prune --force &>/dev/null || true

    # Clean up data directory
    rm -rf "${DATA_DIR}"
    mkdir -p "${DATA_DIR}"
    chmod 777 "${DATA_DIR}"

    # Create volume
    docker volume create crud-bench &>/dev/null || true

    log_success "Cleanup completed"

}

# ============================================================================
# SYSTEM OPTIMIZATION FUNCTIONS
# ============================================================================

# Optimize system settings for benchmarking performance
# Linux optimizations:
#   - Stop background services (unattended-upgrades)
#   - Compact memory and drop caches
#   - Disable Transparent Huge Pages (THP)
#   - Disable swap
#   - Set ulimits for file descriptors, processes, and memory
# macOS optimizations:
#   - Purge memory cache
#   - Set ulimits where supported
# Requires sudo privileges for most operations
optimize_system() {

    log_info "Optimizing system for benchmarking..."

    if [[ "$IS_LINUX" == "true" ]]; then
        # Disable services (Linux only)
        log_info "Stopping unattended-upgrades service..."
        sudo systemctl stop unattended-upgrades 2>/dev/null || log_warning "Could not stop unattended-upgrades"

        # Flush disk writes
        log_info "Flushing disk writes..."
        sync

        # Clear page tables (Linux only)
        log_info "Compacting memory..."
        echo 1 | sudo tee /proc/sys/vm/compact_memory >/dev/null 2>&1 || log_warning "Could not compact memory"

        # Drop memory caches (Linux only)
        log_info "Dropping memory caches..."
        echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || log_warning "Could not drop caches"

        # Disable Transparent Huge Pages (Linux only)
        log_info "Disabling Transparent Huge Pages..."
        echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null 2>&1 || log_warning "Could not disable THP"

        # Disable swap memory
        log_info "Disabling swap..."
        sudo swapoff -a 2>/dev/null || log_warning "Could not disable swap"
    else
        # macOS optimizations
        log_info "Flushing disk writes..."
        sync

        log_info "Purging memory cache..."
        sudo purge 2>/dev/null || log_warning "Could not purge memory"
    fi

    # Increase max limits (both platforms)
    log_info "Setting ulimits..."
    ulimit -n 65536 || log_warning "Could not set file descriptor limit"
    ulimit -u unlimited 2>/dev/null || ulimit -u 2048 || log_warning "Could not set process limit"
    ulimit -l unlimited 2>/dev/null || log_warning "Could not set memory lock limit"

    log_success "System optimization completed"

}

# Restore system to normal state after benchmarking
# Linux:
#   - Re-enable services (unattended-upgrades)
#   - Compact memory and drop caches
#   - Re-enable Transparent Huge Pages
# macOS:
#   - Flush disk writes
normalize_system() {

    log_info "Normalizing system..."

    if [[ "$IS_LINUX" == "true" ]]; then
        # Enable services (Linux only)
        log_info "Starting unattended-upgrades service..."
        sudo systemctl start unattended-upgrades 2>/dev/null || log_warning "Could not start unattended-upgrades"

        # Flush disk writes
        sync

        # Clear page tables (Linux only)
        echo 1 | sudo tee /proc/sys/vm/compact_memory >/dev/null 2>&1 || true

        # Drop memory caches (Linux only)
        echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true

        # Enable Transparent Huge Pages (Linux only)
        log_info "Enabling Transparent Huge Pages..."
        echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null 2>&1 || log_warning "Could not enable THP"
    else
        # macOS normalization
        sync
    fi

    log_success "System normalization completed"

}

# Wait until system load average drops below acceptable threshold before proceeding
# Ensures system is ready for benchmarking
# Threshold:
#   - macOS: Half the CPU count (e.g., 4.0 for 8-core system)
#   - Linux: 2.0 (dedicated benchmarking machine)
# Timeout: 900 seconds (15 minutes)
# Exits with error if timeout is reached
wait_for_system() {

    log_info "Waiting for system to be ready..."

    local timeout=900  # 15 minutes
    local elapsed=0

    # Calculate threshold based on OS
    local threshold
    if [[ "$IS_MACOS" == "true" ]]; then
        # macOS: half the CPU count (machine is likely being used for other things)
        local cpu_count=$(get_cpu_count)
        threshold=$(echo "scale=1; $cpu_count / 2" | bc)
    else
        # Linux: fixed threshold of 2.0
        threshold="2.0"
    fi

    while true; do
        load=$(get_load_average)
        if (( $(echo "$load < $threshold" | bc -l) )); then
            log_success "System ready - load: $load (target: < $threshold)"
            break
        fi

        if [ $elapsed -ge $timeout ]; then
            log_error "Timeout waiting for system load to decrease"
            exit 1
        fi

        log_info "Waiting for load to decrease (current: $load, target: < $threshold)..."
        sleep 15
        elapsed=$((elapsed + 15))
    done

}

# ============================================================================
# BENCHMARK EXECUTION
# ============================================================================

# Execute benchmark for a specific database
# Args: $1 - database name
# Returns: 0 on success, 1 on failure
# Process:
#   1. Validates database exists
#   2. Retrieves database configuration from matrix
#   3. Sets environment variables
#   4. Builds platform-specific command with nice/ionice/taskset
#   5. Executes benchmark with timeout
#   6. Reports results
run_benchmark() {

    local db=$1

    local description=$(get_db_description "$db")
    if [[ -z "$description" ]]; then
        log_error "Unknown database: $db"
        return 1
    fi

    log_info "Running benchmark for $description..."

    # Get the database name for CLI
    local db_name=$(get_db_name "$db")

    # Get CLI args and endpoint
    local cli_args=$(get_db_cli_args "$db")
    local endpoint=$(get_db_endpoint "$db")

    # Get number of CPUs
    local num_cpus=$(get_cpu_count)

    # Build sync flag
    local sync_flag=""
    if [[ "$SYNC" == "true" ]]; then
        sync_flag="--sync"
    fi

    # Build optimised flag
    local optimised_flag=""
    if [[ "$OPTIMISED" == "true" ]]; then
        optimised_flag="--optimised"
    fi

    # Set environment variables
    export CRUD_BENCH_LMDB_DATABASE_SIZE="${CRUD_BENCH_LMDB_DATABASE_SIZE:-53687091200}"  # 50 GiB
    export CRUD_BENCH_VALUE="${CRUD_BENCH_VALUE:-{ \"text\": \"text:50\", \"number\": \"int:1..5000\", \"integer\": \"int\", \"words\": \"words:100;hello,world,foo,bar,test,search,data,query,index,document,database,performance\", \"nested\": { \"text\": \"text:1000\", \"array\": [ \"string:50\", \"string:50\", \"string:50\", \"string:50\", \"string:50\" ] } }}"
    export DOCKER_PRE_ARGS="${DOCKER_PRE_ARGS:-}"
    export DOCKER_POST_ARGS="${DOCKER_POST_ARGS:-}"

    # Get binary path (from default target directory)
    local binary_path="target/release/crud-bench"

    # Use custom name if provided, otherwise use database name
    local run_name="${NAME:-$db}"

    # Build command based on platform
    local bench_cmd
    if [[ "$IS_LINUX" == "true" ]]; then
        # Linux: use taskset for CPU affinity
        local cpu_range="0-$((num_cpus - 1))"
        bench_cmd="sudo -E taskset -c $cpu_range $cli_args $binary_path --privileged $sync_flag $optimised_flag -d $db_name $endpoint -s $SAMPLES -c $CLIENTS -t $THREADS -k $KEY_TYPE -n $run_name -r"
    else
        # macOS: no taskset, just nice
        bench_cmd="sudo -E $cli_args $binary_path --privileged $sync_flag $optimised_flag -d $db_name $endpoint -s $SAMPLES -c $CLIENTS -t $THREADS -k $KEY_TYPE -n $run_name -r"
    fi

    # Run the benchmark with timeout (if specified)
    log_info "Command: $bench_cmd"

    local exit_code=0
    if [[ -n "$TIMEOUT" ]]; then
        # Run with timeout
        if timeout "${TIMEOUT}m" bash -c "$bench_cmd"; then
            log_success "Benchmark completed for $description"
        else
            exit_code=$?
            if [ $exit_code -eq 124 ]; then
                log_error "Benchmark timed out for $description"
            else
                log_error "Benchmark failed for $description (exit code: $exit_code)"
            fi
            return 1
        fi
    else
        # Run without timeout
        if bash -c "$bench_cmd"; then
            log_success "Benchmark completed for $description"
        else
            exit_code=$?
            log_error "Benchmark failed for $description (exit code: $exit_code)"
            return 1
        fi
    fi

}

# Parse datastore input and return list of databases to benchmark
# Args: $1 - datastore input (single name, "all", or comma-separated list)
# Returns: Space-separated list of database names
# Examples:
#   "postgres" -> "postgres"
#   "all" -> "arangodb dragonfly dry fjall ..."
#   "postgres,mysql,mongodb" -> "postgres mysql mongodb"
get_databases_to_run() {

    local input=$1
    local databases=()

    if [[ "$input" == "all" ]]; then
        # Return all databases
        local all_dbs=$(get_all_databases)
        for db in $all_dbs; do
            databases+=("$db")
        done
    else
        # Split comma-separated list
        IFS=',' read -ra databases <<< "$input"
    fi

    echo "${databases[@]}"

}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

# Main entry point for the benchmark script
# Orchestrates the complete benchmark workflow:
#   1. Parse command line arguments
#   2. Build benchmark binary
#   3. Determine which databases to benchmark
#   4. For each database:
#      - Clean up environment
#      - Optimize system
#      - Wait for system readiness
#      - Run benchmark
#      - Normalize system
#      - Clean up environment
#   5. Display summary and exit with appropriate status
# Args: $@ - command line arguments
main() {

    # Parse arguments
    parse_args "$@"

    # Build the benchmark
    build_benchmark

    # Get list of databases to run
    local databases=($(get_databases_to_run "$DATASTORE"))

    if [[ ${#databases[@]} -eq 0 ]]; then
        log_error "No databases to benchmark"
        exit 1
    fi

    log_info "Will benchmark ${#databases[@]} database(s): ${databases[*]}"

    # Run benchmarks for each database
    local failed_benchmarks=()
    for db in "${databases[@]}"; do
        log_info ""
        log_info "=========================================="
        log_info "Starting benchmark for: $db"
        log_info "=========================================="

        # Clean up environment
        cleanup_environment

        # Optimize system
        optimize_system

        # Wait for system to be ready (unless --no-wait is specified)
        if [[ "$NOWAIT" != "true" ]]; then
            wait_for_system
        else
            log_info "Skipping system load wait (--no-wait specified)"
        fi

        # Run benchmark
        if ! run_benchmark "$db"; then
            failed_benchmarks+=("$db")
        fi

        # Normalize system
        normalize_system

        # Clean up environment
        cleanup_environment

        log_info "=========================================="
    done

    # Summary
    log_info ""
    log_info "=========================================="
    log_info "Benchmark Summary"
    log_info "=========================================="
    log_success "Completed: $((${#databases[@]} - ${#failed_benchmarks[@]}))/${#databases[@]}"

    if [[ ${#failed_benchmarks[@]} -gt 0 ]]; then
        log_error "Failed benchmarks: ${failed_benchmarks[*]}"
        exit 1
    else
        log_success "All benchmarks completed successfully!"
    fi

}

# Run main function
main "$@"
