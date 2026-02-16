# ============================================================
# SurrealDB 2 vs 3 Benchmark Suite
# ============================================================
#
# Usage:
#   make bench-v3-memory          Run v3 embedded memory benchmark
#   make bench-v2-memory          Run v2 embedded memory benchmark
#   make bench-all-memory         Run both v2 and v3 memory
#   make bench-all-rocksdb        Run both v2 and v3 rocksdb
#   make bench-all-surrealkv      Run both v2 and v3 surrealkv
#   make bench-all-tikv           Run both v2 and v3 tikv (container)
#   make bench-all                Run all 8 benchmarks
#   make compare-memory           Compare v2 vs v3 memory results
#   make compare-all              Compare all backends
#   make compare-v3               Compare v3 across all backends
#   make compare-v2               Compare v2 across all backends
#   make clean                    Remove tmp data and result files
#
# Override variables:
#   make bench-v3-memory SAMPLES=500000 CLIENTS=8 THREADS=16
#

# --------------------------------------------------
# Tunables
# --------------------------------------------------
SAMPLES      ?= 1000000
CLIENTS      ?= 12
THREADS      ?= 24
SCAN_SAMPLES ?=
DATADIR      ?= $(CURDIR)/tmp
RELEASE      ?= 1

# TiKV settings
TIKV_PD   ?= 127.0.0.1:2379
export SURREAL_TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE ?= 1073741824

# Config files
VALUE     := @config/value-surrealdb.json
SCANS     := @config/scans-surrealdb.json
SETUP     := @config/setup-surrealdb.json
BATCHES   := @config/batches.json

# Output directory for results
RESULTDIR := $(CURDIR)/results/runs

# Common flags
COMMON_FLAGS := -s $(SAMPLES) -c $(CLIENTS) -t $(THREADS) \
	--value $(VALUE) --scans $(SCANS) --setup $(SETUP) --batches $(BATCHES)

ifneq ($(SCAN_SAMPLES),)
COMMON_FLAGS += --scan-samples $(SCAN_SAMPLES)
endif

# Cargo build mode: RELEASE=1 (default) for --release, RELEASE=0 for debug
ifeq ($(RELEASE),1)
CARGO_BUILD_FLAGS := --release
CRUD_BENCH_BINARY := target/release/crud-bench
else
CARGO_BUILD_FLAGS :=
CRUD_BENCH_BINARY := target/debug/crud-bench
endif

# ============================================================
# Default target
# ============================================================
.PHONY: default
default:
	@echo ""
	@echo "SurrealDB Benchmark Suite"
	@echo "========================="
	@echo ""
	@echo "Individual benchmarks:"
	@echo "  make bench-v3-memory       SurrealDB 3, embedded memory"
	@echo "  make bench-v3-rocksdb      SurrealDB 3, embedded RocksDB"
	@echo "  make bench-v3-surrealkv    SurrealDB 3, embedded SurrealKV"
	@echo "  make bench-v3-tikv         SurrealDB 3, embedded TiKV (container)"
	@echo "  make bench-v2-memory       SurrealDB 2, embedded memory"
	@echo "  make bench-v2-rocksdb      SurrealDB 2, embedded RocksDB"
	@echo "  make bench-v2-surrealkv    SurrealDB 2, embedded SurrealKV"
	@echo "  make bench-v2-tikv         SurrealDB 2, embedded TiKV (container)"
	@echo ""
	@echo "Grouped benchmarks:"
	@echo "  make bench-all-memory      Both v2 and v3 memory"
	@echo "  make bench-all-rocksdb     Both v2 and v3 rocksdb"
	@echo "  make bench-all-surrealkv   Both v2 and v3 surrealkv"
	@echo "  make bench-all-tikv        Both v2 and v3 tikv"
	@echo "  make bench-all             All 8 benchmarks"
	@echo ""
	@echo "TiKV cluster:"
	@echo "  make tikv-start            Start PD + TiKV containers"
	@echo "  make tikv-stop             Stop TiKV containers"
	@echo "  make tikv-status           Show TiKV cluster status"
	@echo "  make tikv-clean            Stop containers and remove data"
	@echo ""
	@echo "Comparisons:"
	@echo "  make compare-memory        v2 vs v3 on memory"
	@echo "  make compare-rocksdb       v2 vs v3 on rocksdb"
	@echo "  make compare-surrealkv     v2 vs v3 on surrealkv"
	@echo "  make compare-tikv          v2 vs v3 on tikv"
	@echo "  make compare-all           v2 vs v3 on all backends"
	@echo "  make compare-v3            v3 across all backends"
	@echo "  make compare-v2            v2 across all backends"
	@echo ""
	@echo "Other:"
	@echo "  make build                 Build release binary"
	@echo "  make clean                 Remove tmp data and results"
	@echo "  make clean-data            Remove only tmp storage data"
	@echo ""
	@echo "Override defaults: SAMPLES=$(SAMPLES) CLIENTS=$(CLIENTS) THREADS=$(THREADS) RELEASE=$(RELEASE)"
	@echo "  Use RELEASE=0 for fast debug builds (e.g. make bench-v2-memory RELEASE=0)"
	@echo ""

# ============================================================
# Build
# ============================================================

RUST_SOURCES := $(wildcard src/*.rs) Cargo.toml Cargo.lock

$(CRUD_BENCH_BINARY): $(RUST_SOURCES)
	cargo build $(CARGO_BUILD_FLAGS)

build: $(CRUD_BENCH_BINARY)

# ============================================================
# TiKV cluster management
# ============================================================
.PHONY: tikv-start
tikv-start:
	TIKV_DATADIR=$(DATADIR)/tikv ./scripts/tikv.sh start

.PHONY: tikv-stop
tikv-stop:
	./scripts/tikv.sh stop

.PHONY: tikv-status
tikv-status:
	./scripts/tikv.sh status

.PHONY: tikv-clean
tikv-clean:
	TIKV_DATADIR=$(DATADIR)/tikv ./scripts/tikv.sh clean

.PHONY: tikv-restart
tikv-restart: tikv-clean tikv-start

# ============================================================
# SurrealDB v3 benchmarks
# ============================================================
.PHONY: bench-v3-memory
bench-v3-memory: build $(RESULTDIR)
	$(CRUD_BENCH_BINARY) -d surrealdb -e memory $(COMMON_FLAGS) -n v3-memory | tee $(RESULTDIR)/v3-memory.txt
	@mv -f result-v3-memory.json result-v3-memory.csv result-v3-memory.html $(RESULTDIR)/ 2>/dev/null || true
	@echo "✅ v3-memory done → $(RESULTDIR)/v3-memory.*"

.PHONY: bench-v3-rocksdb
bench-v3-rocksdb: build $(RESULTDIR) clean-data-v3-rocksdb
	$(CRUD_BENCH_BINARY) -d surrealdb -e rocksdb:$(DATADIR)/v3-rocksdb $(COMMON_FLAGS) -n v3-rocksdb | tee $(RESULTDIR)/v3-rocksdb.txt
	@mv -f result-v3-rocksdb.json result-v3-rocksdb.csv result-v3-rocksdb.html $(RESULTDIR)/ 2>/dev/null || true
	@echo "✅ v3-rocksdb done → $(RESULTDIR)/v3-rocksdb.*"

.PHONY: bench-v3-surrealkv
bench-v3-surrealkv: build $(RESULTDIR) clean-data-v3-surrealkv
	$(CRUD_BENCH_BINARY) -d surrealdb -e surrealkv:$(DATADIR)/v3-surrealkv $(COMMON_FLAGS) -n v3-surrealkv | tee $(RESULTDIR)/v3-surrealkv.txt
	@mv -f result-v3-surrealkv.json result-v3-surrealkv.csv result-v3-surrealkv.html $(RESULTDIR)/ 2>/dev/null || true
	@echo "✅ v3-surrealkv done → $(RESULTDIR)/v3-surrealkv.*"

.PHONY: bench-v3-tikv
bench-v3-tikv: build $(RESULTDIR) tikv-restart
	$(CRUD_BENCH_BINARY) -d surrealdb -e tikv://$(TIKV_PD) $(COMMON_FLAGS) -n v3-tikv | tee $(RESULTDIR)/v3-tikv.txt
	@mv -f result-v3-tikv.json result-v3-tikv.csv result-v3-tikv.html $(RESULTDIR)/ 2>/dev/null || true
	@echo "✅ v3-tikv done → $(RESULTDIR)/v3-tikv.*"

# ============================================================
# SurrealDB v2 benchmarks
# (SURREAL_SYNC_DATA=true enables fsync, matching v3 defaults)
# ============================================================
.PHONY: bench-v2-memory
bench-v2-memory: build $(RESULTDIR)
	SURREAL_SYNC_DATA=true $(CRUD_BENCH_BINARY) -d surrealdb2 -e memory $(COMMON_FLAGS) -n v2-memory | tee $(RESULTDIR)/v2-memory.txt
	@mv -f result-v2-memory.json result-v2-memory.csv result-v2-memory.html $(RESULTDIR)/ 2>/dev/null || true
	@echo "✅ v2-memory done → $(RESULTDIR)/v2-memory.*"

.PHONY: bench-v2-rocksdb
bench-v2-rocksdb: build $(RESULTDIR) clean-data-v2-rocksdb
	SURREAL_SYNC_DATA=true $(CRUD_BENCH_BINARY) -d surrealdb2 -e rocksdb:$(DATADIR)/v2-rocksdb $(COMMON_FLAGS) -n v2-rocksdb | tee $(RESULTDIR)/v2-rocksdb.txt
	@mv -f result-v2-rocksdb.json result-v2-rocksdb.csv result-v2-rocksdb.html $(RESULTDIR)/ 2>/dev/null || true
	@echo "✅ v2-rocksdb done → $(RESULTDIR)/v2-rocksdb.*"

.PHONY: bench-v2-surrealkv
bench-v2-surrealkv: build $(RESULTDIR) clean-data-v2-surrealkv
	SURREAL_SYNC_DATA=true $(CRUD_BENCH_BINARY) -d surrealdb2 -e surrealkv:$(DATADIR)/v2-surrealkv $(COMMON_FLAGS) -n v2-surrealkv | tee $(RESULTDIR)/v2-surrealkv.txt
	@mv -f result-v2-surrealkv.json result-v2-surrealkv.csv result-v2-surrealkv.html $(RESULTDIR)/ 2>/dev/null || true
	@echo "✅ v2-surrealkv done → $(RESULTDIR)/v2-surrealkv.*"

.PHONY: bench-v2-tikv
bench-v2-tikv: build $(RESULTDIR) tikv-restart
	SURREAL_SYNC_DATA=true $(CRUD_BENCH_BINARY) -d surrealdb2 -e tikv://$(TIKV_PD) $(COMMON_FLAGS) -n v2-tikv | tee $(RESULTDIR)/v2-tikv.txt
	@mv -f result-v2-tikv.json result-v2-tikv.csv result-v2-tikv.html $(RESULTDIR)/ 2>/dev/null || true
	@echo "✅ v2-tikv done → $(RESULTDIR)/v2-tikv.*"

# ============================================================
# Grouped benchmarks
# ============================================================
.PHONY: bench-all-memory
bench-all-memory: bench-v3-memory bench-v2-memory compare-memory

.PHONY: bench-all-rocksdb
bench-all-rocksdb: bench-v3-rocksdb bench-v2-rocksdb compare-rocksdb

.PHONY: bench-all-surrealkv
bench-all-surrealkv: bench-v3-surrealkv bench-v2-surrealkv compare-surrealkv

.PHONY: bench-all-tikv
bench-all-tikv:
	$(MAKE) bench-v3-tikv
	$(MAKE) bench-v2-tikv
	$(MAKE) compare-tikv

.PHONY: bench-all
bench-all: bench-all-memory bench-all-rocksdb bench-all-surrealkv bench-all-tikv
	@echo ""
	@echo "🏁 All benchmarks complete. Run 'make compare-all' for summary."

# ============================================================
# Comparisons (v2 vs v3 on same backend)
# ============================================================
.PHONY: compare-memory
compare-memory:
	@python3 scripts/compare.py \
		--title "Memory: v2 vs v3" \
		$(RESULTDIR)/result-v2-memory.csv "SurrealDB 2" \
		$(RESULTDIR)/result-v3-memory.csv "SurrealDB 3" \
		--output $(RESULTDIR)/compare-memory.html \
		| tee $(RESULTDIR)/compare-memory.txt

.PHONY: compare-rocksdb
compare-rocksdb:
	@python3 scripts/compare.py \
		--title "RocksDB: v2 vs v3" \
		$(RESULTDIR)/result-v2-rocksdb.csv "SurrealDB 2" \
		$(RESULTDIR)/result-v3-rocksdb.csv "SurrealDB 3" \
		--output $(RESULTDIR)/compare-rocksdb.html \
		| tee $(RESULTDIR)/compare-rocksdb.txt

.PHONY: compare-surrealkv
compare-surrealkv:
	@python3 scripts/compare.py \
		--title "SurrealKV: v2 vs v3" \
		$(RESULTDIR)/result-v2-surrealkv.csv "SurrealDB 2" \
		$(RESULTDIR)/result-v3-surrealkv.csv "SurrealDB 3" \
		--output $(RESULTDIR)/compare-surrealkv.html \
		| tee $(RESULTDIR)/compare-surrealkv.txt

.PHONY: compare-tikv
compare-tikv:
	@python3 scripts/compare.py \
		--title "TiKV: v2 vs v3" \
		$(RESULTDIR)/result-v2-tikv.csv "SurrealDB 2" \
		$(RESULTDIR)/result-v3-tikv.csv "SurrealDB 3" \
		--output $(RESULTDIR)/compare-tikv.html \
		| tee $(RESULTDIR)/compare-tikv.txt

.PHONY: compare-all
compare-all: compare-memory compare-rocksdb compare-surrealkv compare-tikv

# ============================================================
# Comparisons (same version across backends)
# ============================================================
.PHONY: compare-v3
compare-v3:
	@python3 scripts/compare.py \
		--title "SurrealDB 3: Memory vs RocksDB vs SurrealKV" \
		$(RESULTDIR)/result-v3-memory.csv "Memory" \
		$(RESULTDIR)/result-v3-rocksdb.csv "RocksDB" \
		$(RESULTDIR)/result-v3-surrealkv.csv "SurrealKV" \
		--output $(RESULTDIR)/compare-v3-backends.html \
		| tee $(RESULTDIR)/compare-v3-backends.txt

.PHONY: compare-v2
compare-v2:
	@python3 scripts/compare.py \
		--title "SurrealDB 2: Memory vs RocksDB vs SurrealKV" \
		$(RESULTDIR)/result-v2-memory.csv "Memory" \
		$(RESULTDIR)/result-v2-rocksdb.csv "RocksDB" \
		$(RESULTDIR)/result-v2-surrealkv.csv "SurrealKV" \
		--output $(RESULTDIR)/compare-v2-backends.html \
		| tee $(RESULTDIR)/compare-v2-backends.txt

# ============================================================
# Cleanup
# ============================================================
.PHONY: clean
clean: clean-data
	rm -f result*.json result*.csv result*.html
	rm -rf $(RESULTDIR)

.PHONY: clean-data
clean-data:
	rm -rf $(DATADIR)

.PHONY: clean-data-v3-rocksdb
clean-data-v3-rocksdb:
	rm -rf $(DATADIR)/v3-rocksdb

.PHONY: clean-data-v2-rocksdb
clean-data-v2-rocksdb:
	rm -rf $(DATADIR)/v2-rocksdb

.PHONY: clean-data-v3-surrealkv
clean-data-v3-surrealkv:
	rm -rf $(DATADIR)/v3-surrealkv

.PHONY: clean-data-v2-surrealkv
clean-data-v2-surrealkv:
	rm -rf $(DATADIR)/v2-surrealkv

# ============================================================
# Directory creation
# ============================================================
$(RESULTDIR):
	mkdir -p $(RESULTDIR)
