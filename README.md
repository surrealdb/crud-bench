# crud-bench

The crud-bench benchmarking tool is an open-source benchmarking tool for testing and comparing the performance of a
number of different workloads on embedded, networked, and remote databases. It can be used to compare both SQL and NoSQL
platforms including key-value, and embedded databases. Importantly crud-bench focuses on testing additional features
which are not present in other benchmarking tools, but which are available in SurrealDB.

The primary purpose of crud-bench is to continually test and monitor the performance of features and functionality built
in to SurrealDB, enabling developers working on features in SurrealDB to assess the impact of their changes on database
queries and performance.

The crud-bench benchmarking tool is being actively developed with new features and functionality being added regularly.

## Purpose

The goal of this benchmark is for developers working on features in SurrealDB to assess their impact on CRUD
performance.

## How does it work?

In one table, the benchmark will operate 4 main tasks:

- Create: inserting N unique records.
- Read: read N unique records.
- Update: update N unique records.
- Delete: delete N unique records.

The number of records (samples), the number of concurrent clients, and the number of concurrent threads are configurable
parameters.

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

          [possible values: dry, map, dragonfly, keydb, mongodb, postgres, redb, redis, rocksdb, scylladb, surrealkv, surrealdb, surrealdb-memory, surrealdb-rocksdb, surrealdb-surrealkv]

  -e, --endpoint <ENDPOINT>
          Endpoint

  -w, --workers <WORKERS>
          Number of async runtime workers (default is the number of CPU cores)

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
          [default: "{\n\t\t\t\"text\": \"string:50\",\n\t\t\t\"integer\": \"int\"\n\t\t}"]

      --show-sample
          Print-out an example of a generated value

  -p, --pid <PID>
          Collect system information for a given pid

  -a, --scans <SCANS>
          An array of scan specifications

          [env: CRUD_BENCH_SCANS=]
          [default: "[\n\t\t\t{ \"name\": \"count_all\", \"samples\": 10, \"projection\": \"COUNT\" },\n\t\t\t{ \"name\": \"limit_keys\", \"samples\": 10, \"projection\": \"ID\", \"limit\": 100, \"expect\": 100 },\n\t\t\t{ \"name\": \"limit_full\", \"samples\": 10, \"projection\": \"FULL\", \"limit\": 100, \"expect\": 100 },\n\t\t\t{ \"name\": \"limit_count\", \"samples\": 10, \"projection\": \"COUNT\", \"limit\": 100, \"expect\": 100 }\n\t\t]"]

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
  "text_range": "text:10..50",
  "bool": "bool",
  "string_enum": "enum:foo,bar",
  "datetime": "datetime",
  "float": "float",
  "float_range": "float:1..10",
  "float_enum": "float:1.1,2.2,3.3",
  "integer": "int",
  "integer_range": "int:1..5",
  "integer_enum": "int:1,2,3",
  "uuid": "uuid",
  "nested": {
    "text": "text:100",
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
- Every `string_enum:A,B,C` will be replaced by a string from `A` `B` or `C`.
- Every `int_enum:A,B,C` will be replaced by a i32 from  `A` `B` or `C`.
- Every `float_enum:A,B,C` will be replaced by a f32 from  `A` `B` or `C`.
- Every `datetime` will be replaced by a datetime (ISO 8601).

For column-oriented databases (e.g., PostgreSQL, ScyllaDB), the first-level fields of the JSON structure are translated
as columns.
Nested structures will be stored in a JSON column.

### Scans

Scans can be tested using the `-a` parameter or the environment variable `CRUD_BENCH_SCANS`.
This parameter accepts a JSON array, where each item represents a different scan test.
Each test is defined as a JSON object specifying the scan parameters and the test name.

```json
[
  {
    "name": "limit100",
    "projection": "FULL",
    "start": 0,
    "limit": 100,
    "expect": 100
  },
  {
    "name": "start100",
    "projection": "ID",
    "start": 100,
    "limit": 100,
    "expect": 100
  }
]
```

- name: A descriptive name for the test.
- projection
    - `"ID"`: only the ID is returned.
    - `"FULL"`: (default) the whole record is returned.
    - `"COUNT"`: count the number of records.
- start: Skips the specified number of rows before starting to return rows.
- limit: Specifies the maximum number of rows to return.
- expect: Asserts the expected number of rows returned.

Note: Not every database adapter supports scans.
In such cases, the log will not fail but will instead indicate `skipped`.

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

## Embedded SurrealDB Memory Engine benchmark

Run the benchmark against an embedded SurrealDB memory engine:

```bash
cargo run -F surrealdb/allocator,surrealdb/kv-mem -r -- -d surrealdb -e memory -s 100000 -t 3 -r
```

## Embedded SurrealDB RocksDB Engine benchmark

Run the benchmark against an embedded SurreadDB RocksDB engine:

```bash
cargo run -F surrealdb/allocator,surrealdb/kv-rocksdb -r -- -d surrealdb -e rocksdb:/tmp/rocksdb-engine -s 100000 -t 3 -r
```

## Embedded SurrealDB SurrealKV Engine benchmark

Run the benchmark against an embedded SurreadDB SurrealKV engine:

```bash
cargo run -F surrealdb/allocator,surrealdb/kv-surrealkv -r -- -d surrealdb -e surrealkv:/tmp/surrealkv-engine -s 100000 -t 3 -r
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
