# crud-bench

## Purpose

The goal of this benchmark is for developers working on features in SurrealDB to assess their impact on CRUD performance.

Eg.:
- Testing a new operator
- Work on indexes
- Work on query planner and execution plan

## Requirements

- Docker
- RustC / Cargo

## Usage

```bash
cargo run -r -- --help
```

```bash
Usage: crud-bench [OPTIONS] --database <DATABASE> --samples <SAMPLES> --threads <THREADS>

Options:
  -i, --image <IMAGE>        Docker image
  -d, --database <DATABASE>  Database [possible values: dry, surrealdb, surrealdb-memory, surrealdb-rocksdb, surrealdb-speedb, mongodb, postgresql]
  -s, --samples <SAMPLES>    Number of samples
  -t, --threads <THREADS>    Number of concurrent threads
  -h, --help                 Print help
```

## Dry run

Run the benchmark without interaction with any database:

```bash
cargo run -r -- -d dry -s 100000 -t 3
```

Run the benchmark against Postgres:

## Postgresql benchmark

```bash
cargo run -r -- -d postgresql -s 100000 -t 3
```

## SurrealDB memory benchmark

Run the benchmark against SurrealDB in memory:

```bash
cargo run -r -- -d surrealdb-memory -s 100000 -t 3
```

## SurrealDB RocksDB benchmark

Run the benchmark against SurreadDB with RocksDB:

```bash
cargo run -r -- -d surrealdb-rocksdb -s 100000 -t 3
```

## SurrealDB local benchmark

Run the benchmark against an already running SurrealDB instance:

Eg.: Start a Speedb based Surreal instance:

```bash
 cargo run --features=storage-speedb -r -- start --auth --user root --pass root speedb:/tmp/sur-bench.db
```

Then run the bench:

```bash
cargo run -r -- -d surrealdb -s 100000 -t 3
```