define CRUD_BENCH_VALUE
{
	"text": "text:50",
	"integer": "int",
	"number": "int:1..5000",
	"nested": {
		"text": "text:1000",
		"array": [
			"string:50",
			"string:50",
			"string:50",
			"string:50",
			"string:50"
		]
	}
}
endef

database ?= surrealdb

.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: build
build:
	cargo build -r

export CRUD_BENCH_VALUE
.PHONY: dev
dev:
	cargo run -- -d $(database) -s 100000 -c 128 -t 48 -k string26 -r

export CRUD_BENCH_VALUE
.PHONY: test
test:
	target/release/crud-bench -d $(database) -s 5000000 -c 128 -t 48 -k string26 -r
