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
cargo run -r -- -h
```

```bash
Usage: crud-bench [OPTIONS] --database <DATABASE> --samples <SAMPLES>

Options:
  -i, --image <IMAGE>        Docker image
  -d, --database <DATABASE>  Database [possible values: dry, map, dragonfly, keydb, lmdb, mongodb, mysql, postgres, redb, redis, rocksdb, scylladb, sqlite, surrealkv]
  -e, --endpoint <ENDPOINT>  Endpoint
  -b, --blocking <BLOCKING>  Maximum number of blocking threads (default is the number of CPU cores) [default: 12]
  -w, --workers <WORKERS>    Number of async runtime workers (default is the number of CPU cores) [default: 12]
  -c, --clients <CLIENTS>    Number of concurrent clients [default: 1]
  -t, --threads <THREADS>    Number of concurrent threads per client [default: 1]
  -s, --samples <SAMPLES>    Number of samples to be created, read, updated, and deleted
  -r, --random               Generate the keys in a pseudo-randomized order
  -k, --key <KEY>            The type of the key [default: integer] [possible values: integer, string26, string90, string506, uuid]
  -v, --value <VALUE>        Size of the text value [env: CRUD_BENCH_VALUE=]
      --show-sample          Print-out an example of a generated value
  -p, --pid <PID>            Collect system information for a given pid
  -a, --scans <SCANS>        An array of scan specifications [env: CRUD_BENCH_SCANS=]
  -h, --help                 Print help (see more with '--help')
```

For more detailed help information run the following command:

```bash
cargo run -r -- --help
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

> [!NOTE]
> Not every database adapter supports scans. In such cases, the log will not fail but will instead indicate `skipped`.

## Databases

#### Dry (no datastore interaction)

```bash
cargo run -r -- -d dry -s 100000 -c 12 -t 24 -r
```

#### [Dragonfly](https://www.dragonflydb.io/) (in-memory datastore)

```bash
cargo run -r -- -d dragonfly -s 100000 -c 12 -t 24 -r
```

#### [KeyDB](https://docs.keydb.dev/) (in-memory datastore)

```bash
cargo run -r -- -d keydb -s 100000 -c 12 -t 24 -r
```

#### [LMDB](http://www.lmdb.tech/doc/) (embedded, persisted datastore)

```bash
cargo run -r -- -d lmdb -s 100000 -c 12 -t 24 -r
```

#### Map (concurrent HashMap)

```bash
cargo run -r -- -d map -s 100000 -c 12 -t 24 -r
```

#### [MongoDB](https://www.mongodb.com/) (document database)

```bash
cargo run -r -- -d mongodb -s 100000 -c 12 -t 24 -r
```

#### [Postgres](https://www.postgresql.org/) (relational database)

```bash
cargo run -r -- -d postgres -s 100000 -c 12 -t 24 -r
```

#### [MySQL](https://www.mysql.com/) (relational database)

```bash
cargo run -r -- -d mysql -s 100000 -c 12 -t 24 -r
```

#### [ReDB](https://www.redb.org/) (embedded, persisted datastore)

```bash
cargo run -r -- -d redb -s 100000 -c 12 -t 24 -r
```

#### [Redis](https://redis.io/) (in-memory datastore)

```bash
cargo run -r -- -d redis -s 100000 -c 12 -t 24 -r
```

#### [RocksDB](https://rocksdb.org/) (embedded, persisted datastore)

```bash
cargo run -r -- -d scylladb -s 100000 -c 12 -t 24 -r
```

#### [SQLite](https://www.sqlite.org/) (embedded, persisted database)

```bash
cargo run -r -- -d sqlite -s 100000 -c 12 -t 24 -r
```

#### [SurrealDB](https://surrealdb.com) (in-memory)

```bash
cargo run -r -- -d surrealdb-memory -s 100000 -c 12 -t 24 -r
```

#### [SurrealDB](https://surrealdb.com) (RocksDB)

```bash
cargo run -r -- -d surrealdb-rocksdb -s 100000 -c 12 -t 24 -r
```

#### [SurrealDB](https://surrealdb.com) (SurrealKV)

```bash
cargo run -r -- -d surrealdb-surrealkv -s 100000 -c 12 -t 24 -r
```

#### [SurrealDB](https://surrealdb.com) embedded (in-memory)

```bash
cargo run -r -- -d surrealdb -e memory -s 100000 -c 12 -t 24 -r
```

#### [SurrealDB](https://surrealdb.com) embedded (RocksDB)

```bash
cargo run -r -- -d surrealdb -e rocksdb:/tmp/db -s 100000 -c 12 -t 24 -r
```

#### [SurrealDB](https://surrealdb.com) embedded (SurrealKV)

```bash
cargo run -r -- -d surrealdb -e surrealkv:/tmp/db -s 100000 -c 12 -t 24 -r -c 12 -t 24 -r
```

#### [SurrealKV](https://surrealkv.org) (embedded, persisted datastore)

```bash
cargo run -r -- -d surrealdb -e surrealkv:/tmp/db -s 100000 -c 12 -t 24 -r -c 12 -t 24 -r
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
