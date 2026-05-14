database ?= surrealdb

.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: build
build:
	cargo build -r

.PHONY: dev
dev:
	cargo run -- -d $(database) -s 100000 -c 128 -t 48 -k string26 -r

.PHONY: test
test:
	target/release/crud-bench -d $(database) -s 5000000 -c 128 -t 48 -k string26 -r

# ── CloudDB / RocksDB-plain benchmarks ────────────────────────────────────
#
# Convenience targets for the rocksdb-family benchmarks.  The `clouddb`
# feature pulls in the AWS C++ SDK, which has to be installed/built before
# cargo can link the binary:
#   - macOS: `brew install aws-sdk-cpp` (uses /opt/homebrew)
#   - Linux: `make ensure-aws-sdk` builds the SDK to .do-not-commit/aws-sdk-install
#
# Run a benchmark via e.g.:
#   make bench-clouddb ARGS="-s 1000000 -c 32"
#   make bench-rocksdb-plain ARGS="-s 1000000 -c 32"
#
# Forward arbitrary CLI args via ARGS="...".

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  AWS_SDK_INSTALL_DIR := /opt/homebrew
  CLOUD_CXXFLAGS      := -I/opt/homebrew/include
  CLOUD_RUSTFLAGS     := -L/opt/homebrew/lib
else
  AWS_SDK_INSTALL_DIR := $(CURDIR)/.do-not-commit/aws-sdk-install
  CLOUD_CXXFLAGS      :=
  CLOUD_RUSTFLAGS     :=
endif

CLOUD_ENV := \
  CXXFLAGS="$(CLOUD_CXXFLAGS)" \
  RUSTFLAGS="$(CLOUD_RUSTFLAGS)" \
  AWS_SDK_INSTALL_DIR=$(AWS_SDK_INSTALL_DIR) \
  AWS_EC2_METADATA_DISABLED=true

ARGS ?=

.PHONY: ensure-aws-sdk build-clouddb bench-clouddb bench-rocksdb-plain bench-rocksdb

ensure-aws-sdk:
ifeq ($(UNAME_S),Darwin)
	@if [ ! -f /opt/homebrew/lib/libaws-cpp-sdk-s3.dylib ] \
	&& [ ! -f /opt/homebrew/lib/libaws-cpp-sdk-s3.a ]; then \
	  echo "ERROR: AWS SDK not found at /opt/homebrew."; \
	  echo "       Install it with: brew install aws-sdk-cpp"; \
	  exit 1; \
	fi
	@echo "Using brew-installed AWS SDK at /opt/homebrew"
else
	@if [ -f $(AWS_SDK_INSTALL_DIR)/lib/libaws-cpp-sdk-s3.a ] \
	|| [ -f $(AWS_SDK_INSTALL_DIR)/lib64/libaws-cpp-sdk-s3.a ]; then \
	  echo "AWS C++ SDK ready at $(AWS_SDK_INSTALL_DIR)"; \
	else \
	  echo "Building AWS C++ SDK from source (first time only, ~5 min)..."; \
	  bash scripts/init_aws_sdk.sh; \
	fi
endif

build-clouddb: ensure-aws-sdk
	$(CLOUD_ENV) cargo build --release --no-default-features --features clouddb,rocksdb

bench-clouddb: build-clouddb
	$(CLOUD_ENV) cargo run --release --no-default-features --features clouddb,rocksdb -- --database clouddb $(ARGS)

bench-rocksdb-plain:
	cargo run --release --features rocksdb -- --database rocksdb-plain $(ARGS)

bench-rocksdb:
	cargo run --release --features rocksdb -- --database rocksdb $(ARGS)
