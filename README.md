<img width="100%" src="./img/hero.png" alt="CRUD-bench hero">

# crud-bench

The crud-bench benchmarking tool is an open-source benchmarking tool for testing and comparing the performance of a
number of different workloads on embedded, networked, and remote databases. It can be used to compare both SQL and NoSQL
platforms including key-value, embedded, relational, document, and multi-model databases. Importantly crud-bench focuses
on testing additional features which are not present in other benchmarking tools, but which are available in SurrealDB.

The primary purpose of crud-bench is to continually test and monitor the performance of features and functionality built
in to SurrealDB, enabling developers working on features in SurrealDB to assess the impact of their changes on database
queries and performance.

The crud-bench benchmarking tool is being actively developed with new features and functionality being added regularly.

## Contributing

The crud-bench benchmarking tool is open-source, and we encourage additions, modifications, and improvements to the
benchmark runtime, and the datastore implementations.

## How does it work?

When running simple, automated tests, the crud-bench benchmarking tool will automatically start a Docker container for
the datastore or database which is being benchmarked (when the datastore or database is networked). This configuration
can be modified so that an optimised, remote environment can be connected to, instead of running a Docker container
locally. This allows for running crud-bench against remote datastores, and distributed datastores on a local network
or remotely in the cloud.

In one table, the benchmark will operate 5 main tasks:

- Create: inserting N unique records, with the specified concurrency.
- Read: read N unique records, with the specified concurrency.
- Update: update N unique records, with the specified concurrency.
- Scans: perform a number of range and table scans, with the specified concurrency.
- Delete: delete N unique records, with the specified concurrency.

With crud-bench almost all aspects of the benchmark engine are configurable:

- The number of rows or records (samples).
- The number of concurrent clients or connections.
- The number of concurrent threads (concurrent messages per client).
- Whether rows or records are modified sequentially or randomly.
- The primary id or key type for the records.
- The row or record content including support for nested objects and arrays.
- The scan specifications for range or table queries.

## Benchmarks

As crud-bench is in active development, some benchmarking workloads are already implemented, while others will be
implemented in future releases. The list below details which benchmarks are implemented for the supporting datastores
and lists those which are planned in the future.

**CRUD**

- [x] Creating single records in individual transactions
- [x] Reading single records in individual transactions
- [x] Updating single records in individual transactions
- [x] Deleting single records in individual transactions
- [ ] Batch creating multiple records in a transaction
- [ ] Batch reading multiple records in a transactions
- [ ] Batch updating multiple records in a transactions
- [ ] Batch deleting multiple records in a transactions

**Scans**

- [x] Full table scans, projecting all fields
- [x] Full table scans, projecting id field
- [x] Full table count queries
- [x] Scans with a limit, projecting all fields
- [x] Scans with a limit, projecting id field
- [x] Scans with a limit, counting results
- [x] Scans with a limit and offset, projecting all fields
- [x] Scans with a limit and offset, projecting id field
- [x] Scans with a limit and offset, counting results

**Filters**

- [ ] Full table query, using filter condition, projecting all fields
- [ ] Full table query, using filter condition, projecting id field
- [ ] Full table query, using filter condition, counting rows

**Indexes**

- [ ] Indexed table query, using filter condition, projecting all fields
- [ ] Indexed table query, using filter condition, projecting id field
- [ ] Indexed table query, using filter condition, counting rows

**Relationships**

- [ ] Fetching or traversing 1-level, one-to-one relationships or joins
- [ ] Fetching or traversing 1-level, one-to-many relationships or joins
- [ ] Fetching or traversing 1-level, many-to-many relationships or joins
- [ ] Fetching or traversing n-level, one-to-one relationships or joins
- [ ] Fetching or traversing n-level, one-to-many relationships or joins
- [ ] Fetching or traversing n-level, many-to-many relationships or joins

**Workloads**

- [ ] Workload support for creating, updating, and reading records concurrently

## Requirements

- [Docker](https://www.docker.com/) - required when running automated tests
- [Rust](https://www.rust-lang.org/) - required when building crud-bench from source
- [Cargo](https://github.com/rust-lang/cargo) - required when building crud-bench from source

## Usage

```bash
cargo run -r -- -h
```

```bash
Usage: crud-bench [OPTIONS] --database <DATABASE> --samples <SAMPLES>

Options:
  -n, --name <NAME>          An optional name for the test, used as a suffix for the JSON result file name
  -d, --database <DATABASE>  The database to benchmark [possible values: dry, map, arangodb, dragonfly, fjall, keydb, lmdb, mongodb, mysql, neo4j, postgres, redb, redis, rocksdb, scylladb, sqlite, surrealkv, surrealmx, surrealdb, surrealdb-memory, surrealdb-rocksdb, surrealdb-surrealkv]
  -i, --image <IMAGE>        Specify a custom Docker image
  -p, --privileged           Whether to run Docker in privileged mode
  -e, --endpoint <ENDPOINT>  Specify a custom endpoint to connect to
  -b, --blocking <BLOCKING>  Maximum number of blocking threads (default is the number of CPU cores) [default: 12]
  -w, --workers <WORKERS>    Number of async runtime workers (default is the number of CPU cores) [default: 12]
  -c, --clients <CLIENTS>    Number of concurrent clients [default: 1]
  -t, --threads <THREADS>    Number of concurrent threads per client [default: 1]
  -s, --samples <SAMPLES>    Number of samples to be created, read, updated, and deleted
  -r, --random               Generate the keys in a pseudo-randomized order
  -k, --key <KEY>            The type of the key [default: integer] [possible values: integer, string26, string90, string250, string506, uuid]
  -v, --value <VALUE>        Size of the text value [env: CRUD_BENCH_VALUE=] [default: "{\n\t\t\t\"text\": \"string:50\",\n\t\t\t\"age\": \"int:1..99\",\n\t\t\t\"integer\": \"int\"\n\t\t}"]
      --show-sample          Print-out an example of a generated value
      --pid <PID>            Collect system information for a given pid
  -a, --scans <SCANS>        An array of scan specifications [env: CRUD_BENCH_SCANS=] [default: "[\n\t\t\t{ \"name\": \"count_all\", \"samples\": 100, \"projection\": \"COUNT\" },\n\t\t\t{ \"name\": \"limit_id\", \"samples\": 100, \"projection\": \"ID\", \"limit\": 100, \"expect\": 100 },\n\t\t\t{ \"name\": \"limit_all\", \"samples\": 100, \"projection\": \"FULL\", \"limit\": 100, \"expect\": 100 },\n\t\t\t{ \"name\": \"limit_count\", \"samples\": 100, \"projection\": \"COUNT\", \"limit\": 100, \"expect\": 100 },\n\t\t\t{ \"name\": \"limit_start_id\", \"samples\": 100, \"projection\": \"ID\", \"start\": 5000, \"limit\": 100, \"expect\": 100 },\n\t\t\t{ \"name\": \"limit_start_all\", \"samples\": 100, \"projection\": \"FULL\", \"start\": 5000, \"limit\": 100, \"expect\": 100 },\n\t\t\t{ \"name\": \"limit_start_count\", \"samples\": 100, \"projection\": \"COUNT\", \"start\": 5000, \"limit\": 100, \"expect\": 100 }\n\t\t]"]
  -h, --help                 Print help (see more with '--help')```

For more detailed help information run the following command:

```bash
cargo run -r -- --help
```

### Value

You can use the argument `-v` or `--value` (or the environment variable `CRUD_BENCH_VALUE`) to customize the row,
document, or record value which should be used in the benchmark tests. Pass a JSON structure that will serve as a
template for generating a randomized value.

> [!NOTE]
> For tabular, or column-oriented databases (e.g. Postgres, MySQL, ScyllaDB), the first-level fields of the JSON
> structure are translated as columns, and any nested structures will be stored in a JSON column where possible.

Within the JSON structure, the following values are replaced by randomly generated data:

- Every occurrence of `string:X` will be replaced by a random string with `X` characters.
- Every occurrence of `text:X` will be replaced by a random string made of words of 2 to 10 characters, for a total of
  `X` characters.
- Every occurrence of `string:X..Y` will be replaced by a random string between `X` and `Y` characters.
- Every occurrence of `text:X..Y` will be replaced by a random string made of words of 2 to 10 characters, for a total
  between `X` and `Y` characters.
- Every `int` will be replaced by a random integer (i32).
- Every `int:X..Y` will be replaced by a random integer (i32) between `X` and `Y`.
- Every `float` will be replaced by a random float (f32).
- Every `float:X..Y` will be replaced by a random float (f32) between `X` and `Y`.
- Every `uuid` will be replaced by a random UUID (v4).
- Every `bool` will be replaced by a `true` or `false`.
- Every `string_enum:A,B,C` will be replaced by a string from `A` `B` or `C`.
- Every `int_enum:A,B,C` will be replaced by a i32 from `A` `B` or `C`.
- Every `float_enum:A,B,C` will be replaced by a f32 from `A` `B` or `C`.
- Every `datetime` will be replaced by a datetime (ISO 8601).

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

### Scans

You can use the argument `-a` or `--scans` (or the environment variable `CRUD_BENCH_SCANS`) to customise the range,
table, or scan queries that are performed in the benchmark. This parameter accepts a JSON array, where each item
represents a different scan test. Each test is defined as a JSON object specifying the scan parameters and the test
name.

> [!NOTE]
> Not every database benchmark adapter supports scans or range queries. In such cases, the benchmark will not fail but
> the associated tests will indicate that the benchmark was `skipped`.

Each scan object can make use of the following values:

- `name`: A descriptive name for the test.
- `projection`: The projection type of the scan:
    - `"ID"`: only the ID is returned.
    - `"FULL"`: the whole record is returned.
    - `"COUNT"`: count the number of records.
- `start`: Skips the specified number of rows before starting to return rows.
- `limit`: Specifies the maximum number of rows to return.
- `expect`: (optional) Asserts the expected number of rows returned.

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

## Databases

### Dry

This benchmark does not interact with any datastore, allowing the overhead of the benchmark implementation, written in
Rust, to be measured.

```bash
cargo run -r -- -d dry -s 100000 -c 12 -t 24 -r
```

### [ArangoDB](https://arangodb.com/)

ArangoDB is a multi-model database with flexible data modeling and efficient querying.

```bash
cargo run -r -- -d arangodb -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to an already-running ArangoDB instance use the following command:

```bash
cargo run -r -- -d arangodb -e http://127.0.0.1:8529 -s 100000 -c 12 -t 24 -r
```

### [Dragonfly](https://www.dragonflydb.io/)

Dragonfly is an in-memory, networked, datastore which is fully-compatible with Redis and Memcached APIs.

```bash
cargo run -r -- -d dragonfly -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to an already-running Dragonfly instance use the
following command:

```bash
cargo run -r -- -d dragonfly -e redis://:root@127.0.0.1:6379 -s 100000 -c 12 -t 24 -r
```

### [Fjall](https://fjall-rs.github.io/)

Fjall is a transactional, ACID-compliant, embedded, key-value datastore, written in safe Rust, and based on LSM-trees.

```bash
cargo run -r -- -d fjall -s 100000 -c 12 -t 24 -r
```

### [KeyDB](https://docs.keydb.dev/)

KeyDB is an in-memory, networked, datastore which is a high-performance fork of Redis, with a focus on multithreading.

```bash
cargo run -r -- -d keydb -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to an already-running KeyDB instance use the
following command:

```bash
cargo run -r -- -d keydb -e redis://:root@127.0.0.1:6379 -s 100000 -c 12 -t 24 -r
```

### [LMDB](http://www.lmdb.tech/doc/)

LMDB is a transactional, ACID-compliant, embedded, key-value datastore, based on B-trees.

```bash
cargo run -r -- -d lmdb -s 100000 -c 12 -t 24 -r
```

### [Map](https://github.com/xacrimon/dashmap)

An in-memory concurrent, associative HashMap in Rust.

```bash
cargo run -r -- -d map -s 100000 -c 12 -t 24 -r
```

### [MongoDB](https://www.mongodb.com/)

MongoDB is a NoSQL, networked, ACID-compliant, document-oriented database, with support for unstructured data storage.

```bash
cargo run -r -- -d mongodb -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to an already-running MongoDB instance use the
following command:

```bash
cargo run -r -- -d mongodb -e mongodb://root:root@127.0.0.1:27017 -s 100000 -c 12 -t 24 -r
```

### [MySQL](https://www.mysql.com/)

MySQL is a networked, relational, ACID-compliant, SQL-based database.

```bash
cargo run -r -- -d mysql -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to an already-running MySQL instance use the
following command:

```bash
cargo run -r -- -d mysql -e mysql://root:mysql@127.0.0.1:3306/bench -s 100000 -c 12 -t 24 -r
```

### [Neo4j](https://neo4j.com/)

Neo4j is a graph database management system for connected data.

```bash
cargo run -r -- -d neo4j -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to an already-running Neo4j instance use the
following command:

```bash
cargo run -r -- -d neo4j -e '127.0.0.1:7687' -s 100000 -c 12 -t 24 -r
```

### [Postgres](https://www.postgresql.org/)

Postgres is a networked, object-relational, ACID-compliant, SQL-based database.

```bash
cargo run -r -- -d postgres -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to an already-running Postgres instance use the
following command:

```bash
cargo run -r -- -d postgres -e 'host=127.0.0.1 user=postgres password=postgres' -s 100000 -c 12 -t 24 -r
```

### [ReDB](https://www.redb.org/)

ReDB is a transactional, ACID-compliant, embedded, key-value datastore, written in Rust, and based on B-trees.

```bash
cargo run -r -- -d redb -s 100000 -c 12 -t 24 -r
```

### [Redis](https://redis.io/)

Redis is an in-memory, networked, datastore that can be used as a cache, message broker, or datastore.

```bash
cargo run -r -- -d redis -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to an already-running Redis instance use the
following command:

```bash
cargo run -r -- -d redis -e redis://:root@127.0.0.1:6379 -s 100000 -c 12 -t 24 -r
```

### [RocksDB](https://rocksdb.org/)

RocksDB is a transactional, ACID-compliant, embedded, key-value datastore, based on LSM-trees.

```bash
cargo run -r -- -d rocksdb -s 100000 -c 12 -t 24 -r
```

### [ScyllaDB](https://www.scylladb.com/)

ScyllaDB is a distributed, NoSQL, wide-column datastore, designed to be compatible with Cassandra.

```bash
cargo run -r -- -d scylladb -s 100000 -c 12 -t 24 -r
```

The above command starts a Docker container automatically. To connect to a already-running ScyllaDB cluster use the
following command:

```bash
cargo run -r -- -d scylladb -e 127.0.0.1:9042 -s 100000 -c 12 -t 24 -r
```

### [SQLite](https://www.sqlite.org/)

SQLite is an embedded, relational, ACID-compliant, SQL-based database.

```bash
cargo run -r -- -d sqlite -s 100000 -c 12 -t 24 -r
```

### [SurrealDB](https://surrealdb.com) (in-memory storage engine)

```bash
cargo run -r -- -d surrealdb-memory -s 100000 -c 12 -t 24 -r
```

### [SurrealDB](https://surrealdb.com) (RocksDB storage engine)

```bash
cargo run -r -- -d surrealdb-rocksdb -s 100000 -c 12 -t 24 -r
```

### [SurrealDB](https://surrealdb.com) (SurrealKV storage engine)

```bash
cargo run -r -- -d surrealdb-surrealkv -s 100000 -c 12 -t 24 -r
```

### [SurrealDB](https://surrealdb.com) embedded (in-memory storage engine)

```bash
cargo run -r -- -d surrealdb -e memory -s 100000 -c 12 -t 24 -r
```

### [SurrealDB](https://surrealdb.com) embedded (RocksDB storage engine)

```bash
cargo run -r -- -d surrealdb -e rocksdb:/tmp/db -s 100000 -c 12 -t 24 -r
```

### [SurrealDB](https://surrealdb.com) embedded (SurrealKV storage engine)

```bash
cargo run -r -- -d surrealdb -e surrealkv:/tmp/db -s 100000 -c 12 -t 24 -r
```

### [SurrealKV](https://surrealkv.org)

SurrealKV is a versioned, transactional, ACID-compliant, embedded key-value database implemented in Rust using an LSM (Log-Structured Merge) tree and B+tree architecture.

```bash
cargo run -r -- -d surrealkv -s 100000 -c 12 -t 24 -r
```

### [SurrealMX](https://surrealmx.org)

SurrealKV is an embedded, in-memory, lock-free and wait-free, transactional, embedded key-value database engine implemented in Rust.

```bash
cargo run -r -- -d surrealmx -s 100000 -c 12 -t 24 -r
```

## SurrealDB local benchmark

To run the benchmark against an already running SurrealDB instance, follow the steps below.

Start a SurrealDB server:

```bash
surreal start --allow-all -u root -p root rocksdb:/tmp/db
```

Then run crud-bench with the `surrealdb` database option:

```bash
cargo run -r -- -d surrealdb -e ws://127.0.0.1:8000 -s 100000 -c 12 -t 24 -r
```
