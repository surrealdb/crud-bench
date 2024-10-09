# crud-bench

## Purpose

The goal of this benchmark is for developers working on features in SurrealDB to assess their impact on CRUD
performance.

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
Usage: crud-bench [OPTIONS] --database <DATABASE> --samples <SAMPLES>

Options:
  -i, --image <IMAGE>        Docker image
  -d, --database <DATABASE>  Database [possible values: dry, redb, rocksdb, surrealkv, surrealdb, surrealdb-memory, surrealdb-rocksdb, surrealdb-surrealkv, scylladb, mongodb, postgres, redis, keydb]
  -e, --endpoint <ENDPOINT>  Endpoint
  -w, --workers <WORKERS>    Number of async runtime workers, defaulting to the number of CPUs [default: 12]
  -c, --clients <CLIENTS>    Number of concurrent clients [default: 1]
  -t, --threads <THREADS>    Number of concurrent threads per client [default: 1]
  -s, --samples <SAMPLES>    Number of samples to be created, read, updated, and deleted
  -r, --random               Generate the keys in a pseudo-randomized order
  -h, --help                 Print help
```

## Dry run

Run the benchmark without interaction with any database:

```bash
cargo run -r -- -d dry -s 100000 -t 3 -r
```

## PostgreSQL benchmark

Run the benchmark against PostgreSQL:

```bash
cargo run -r -- -d postgresql -s 100000 -t 3 -r
```

## MongoDB benchmark

Run the benchmark against MongoDB:

```bash
cargo run -r -- -d mongodb -s 100000 -t 3 -r
```

## Redis benchmark

Run the benchmark against Redis:

```bash
cargo run -r -- -d redis -s 100000 -t 3 -r
```

## RocksDB benchmark

Run the benchmark against RocksDB:

```bash
cargo run -r -- -d rocksdb -s 100000 -t 3 -r
```

## SurrealKV benchmark

Run the benchmark against SurrealKV:

```bash
cargo run -r -- -d surrealkv -s 100000 -t 3 -r
```

## SurrealDB+Memory benchmark

Run the benchmark against SurrealDB in memory:

```bash
cargo run -r -- -d surrealdb-memory -s 100000 -t 3 -r
```

## SurrealDB+RocksDB benchmark

Run the benchmark against SurreadDB with RocksDB:

```bash
cargo run -r -- -d surrealdb-rocksdb -s 100000 -t 3 -r
```

## SurrealDB+SurrealKV benchmark

Run the benchmark against SurreadDB with SurrealKV:

```bash
cargo run -r -- -d surrealdb-surrealkv -s 100000 -t 3 -r
```

## SurrealDB local benchmark

Run the benchmark against an already running SurrealDB instance:

Eg.: Start a SurrealKV based SurrealDB instance:

```bash
cargo run --features=storage-surrealkv -r -- start --user root --pass root surrealkv:/tmp/sur-bench.db
```

Then run the bench:

```bash
cargo run -r -- -d surrealdb -s 100000 -t 3 -r
```
