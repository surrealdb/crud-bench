# crud-bench

## Purpose

The goal of this benchmark is for developers working on features in SurrealDB to assess their impact on CRUD performance.

E.g.:
- Testing a new operator
- Work on indexes
- Work on query planner and execution plan

## How does it work?

In one table, the benchmark will operate 4 main tasks:

- Create: inserting N unique records.
- Read: read N unique records.
- Update: update N unique records.
- Delete: delete N unique records.

The number of records (samples) and the number of threads are parameters.

## Requirements

- Docker
- Rust and Cargo

## Usage

```bash
cargo run -r -- --help
```

```bash
Usage: crud-bench [OPTIONS] --database <DATABASE> --samples <SAMPLES> --threads <THREADS>

Options:
  -i, --image <IMAGE>        Docker image
  -d, --database <DATABASE>  Database [possible values: dry, surrealdb, surrealdb-memory, surrealdb-rocksdb, surrealdb-surrealkv, mongodb, postgresql]
  -s, --samples <SAMPLES>    Number of samples
  -t, --threads <THREADS>    Number of concurrent threads
  -h, --help                 Print help
```

## Dry run

Run the benchmark without interaction with any database:

```bash
cargo run -r -- -d dry -s 100000 -t 3
```

## PostgreSQL benchmark

Run the benchmark against PostgreSQL:

```bash
cargo run -r -- -d postgresql -s 100000 -t 3
```

## MongoDB benchmark

Run the benchmark against MongoDB:

```bash
cargo run -r -- -d mongodb -s 100000 -t 3
```

## Redis benchmark

Run the benchmark against Redis:

```bash
cargo run -r -- -d redis -s 100000 -t 3
```

## RocksDB benchmark

Run the benchmark against RocksDB:

```bash
cargo run -r -- -d rocksdb -s 100000 -t 3
```

## SurrealDB+Memory benchmark

Run the benchmark against SurrealDB in memory:

```bash
cargo run -r -- -d surrealdb-memory -s 100000 -t 3
```

## SurrealDB+RocksDB benchmark

Run the benchmark against SurreadDB with RocksDB:

```bash
cargo run -r -- -d surrealdb-rocksdb -s 100000 -t 3
```

## SurrealDB+SurrealKV benchmark

Run the benchmark against SurreadDB with SurrealKV:

```bash
cargo run -r -- -d surrealdb-surrealkv -s 100000 -t 3
```

## SurrealDB local benchmark

Run the benchmark against an already running SurrealDB instance:

Eg.: Start a SurrealKV based SurrealDB instance:

```bash
cargo run --features=storage-surrealkv -r -- start --user root --pass root surrealkv:/tmp/sur-bench.db
```

Then run the bench:

```bash
cargo run -r -- -d surrealdb -s 100000 -t 3
```
