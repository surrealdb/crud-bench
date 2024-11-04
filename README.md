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
  -i, --image <IMAGE>
          Docker image

  -d, --database <DATABASE>
          Database
          
          [possible values: dry, map, dragonfly, keydb, mongodb, postgres, redis, rocksdb, scylladb, surrealkv, surrealdb, surrealdb-memory, surrealdb-rocksdb, surrealdb-surrealkv]

  -e, --endpoint <ENDPOINT>
          Endpoint

  -w, --workers <WORKERS>
          Number of async runtime workers, defaulting to the number of CPUs
          
          [default: 12]

  -c, --clients <CLIENTS>
          Number of concurrent clients
          
          [default: 1]

  -t, --threads <THREADS>
          Number of concurrent threads per client
          
          [default: 1]

  -s, --samples <SAMPLES>
          Number of samples to be created, read, updated, and deleted

  -r, --random
          Generate the keys in a pseudo-randomized order

  -k, --key <KEY>
          The type of the key
          
          [default: integer]

          Possible values:
          - integer:   4 bytes integer
          - string26:  26 ascii bytes
          - string90:  90 ascii bytes
          - string506: 506 ascii bytes
          - uuid:      UUID type 7

  -v, --value <VALUE>
          Size of the text value
          
          [env: CRUD_BENCH_VALUE=]
          [default: "{\"text\":\"string:50\", \"integer\":\"int\"}"]

  -h, --help
          Print help (see a summary with '-h')
```

### Customizable value

You can use the argument `--value` (or the environment variable `CRUD_BENCH_VALUE`) to customize the value
Pass a JSON structure that will serve as a template for generating a randomized value.

Eg.:

```json
{
  "text": "text:30",
  "string": "string:20",
  "bool": "bool",
  "enum": "enum:foo,bar",
  "datetime": "datetime",
  "float_range": "float:1..10",
  "integer": "int",
  "integer_range": "int:1..5",
  "uuid": "uuid",
  "nested": {
    "text_range": "text:10..50",
    "array": [
      "string:10",
      "string:2..5"
    ]
  }
}
```

- Every occurrence of `string:XX` will be replaced by a random string with XX characters.
- Every occurrence of `text:XX` will be replaced by a random string made of words of 2 to 10 characters, for a total of
  XX characters.
- Every occurrence of `string:X..Y` will be replaced by a random string between X and Y characters.
- Every occurrence of `text:X..Y` will be replaced by a random string made of words of 2 to 10 characters, for a total
  between X and Y characters.
- Every `int` will be replaced by a random integer (i32).
- Every `int:X..Y` will be replaced by a random integer (i32) between X and Y.
- Every `float` will be replaced by a random float (f32).
- Every `float:X..Y` will be replaced by a random float (f32) between X and Y.
- Every `uuid` will be replaced by a random UUID (v4).
- Every `bool` will be replaced by a `true` or `false` (v4).
- Every `enum:A,B,C` will be replaced by either `A` `B` or `C`.
- Every `datetime` will be replaced by a datetime (ISO 8601).

For column-oriented databases (e.g., PostgreSQL, ScyllaDB), the first-level fields of the JSON structure are translated
as columns.
Nested structures will be stored in a JSON column.

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
