define CRUD_BENCH_SCANS
[
	{ "name": "count_all", "samples": 100, "projection": "COUNT" },
	{ "name": "limit_id", "samples": 1000, "projection": "ID", "limit": 100, "expect": 100 },
	{ "name": "limit_all", "samples": 1000, "projection": "FULL", "limit": 100, "expect": 100 },
	{ "name": "limit_count", "samples": 1000, "projection": "COUNT", "limit": 100, "expect": 100 },
	{ "name": "limit_start_id", "samples": 1000, "projection": "ID", "start": 5000, "limit": 100, "expect": 100 },
	{ "name": "limit_start_all", "samples": 1000, "projection": "FULL", "start": 5000, "limit": 100, "expect": 100 },
	{ "name": "limit_start_count", "samples": 1000, "projection": "COUNT", "start": 5000, "limit": 100, "expect": 100 },
	{ "name": "where_field_integer_eq", "samples": 100, "projection": "FULL",
		"condition": {
			"sql": "age = 21",
			"mysql": "age = 21",
			"neo4j": "r.age = 21",
			"mongodb": { "age": { "$$eq": 21 } },
			"arangodb": "r.age == 21",
			"surrealdb": "age = 21"
		}
	},
	{ "name": "where_field_integer_gte_lte", "samples": 100, "projection": "FULL",
		"condition": {
			"sql": "age >= 18 AND age <= 21",
			"mysql": "age >= 18 AND age <= 21",
			"neo4j": "r.age >= 18 AND r.age <= 21",
			"mongodb": { "age": { "$$gte": 18, "$$lte": 21 } },
			"arangodb": "r.age >= 18 AND r.age <= 21",
			"surrealdb": "age >= 18 AND age <= 21"
		}
	}
]
endef

define CRUD_BENCH_VALUE
{
	"text": "text:50",
	"integer": "int",
	"age": "int:1..99",
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

export CRUD_BENCH_SCANS
export CRUD_BENCH_VALUE
.PHONY: dev
dev:
	cargo run -- -d $(database) -s 100000 -c 128 -t 48 -k string26 -r

export CRUD_BENCH_SCANS
export CRUD_BENCH_VALUE
.PHONY: test
test:
	target/release/crud-bench -d $(database) -s 5000000 -c 128 -t 48 -k string26 -r
