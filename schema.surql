-- ----------------------------------------
-- SurrealDB schema for crud-bench result storage
-- This file defines the schema used to store benchmark
-- results in SurrealDB. This schema is automatically
-- applied to the database when crud-bench is run.
-- ----------------------------------------

-- Define the result table
DEFINE TABLE IF NOT EXISTS result SCHEMAFULL;

-- Define fields
DEFINE FIELD IF NOT EXISTS database ON result TYPE option<string>;
DEFINE FIELD IF NOT EXISTS system_info ON result TYPE option<object>;
DEFINE FIELD IF NOT EXISTS benchmark_metadata ON result TYPE option<object>;
DEFINE FIELD IF NOT EXISTS creates ON result TYPE option<object>;
DEFINE FIELD IF NOT EXISTS reads ON result TYPE option<object>;
DEFINE FIELD IF NOT EXISTS updates ON result TYPE option<object>;
DEFINE FIELD IF NOT EXISTS deletes ON result TYPE option<object>;
DEFINE FIELD IF NOT EXISTS scans ON result TYPE array;
DEFINE FIELD IF NOT EXISTS batches ON result TYPE array;
DEFINE FIELD IF NOT EXISTS sample ON result TYPE object;
DEFINE FIELD IF NOT EXISTS timestamp ON result TYPE datetime DEFAULT time::now();

-- Define indexes for efficient querying
DEFINE INDEX IF NOT EXISTS idx_database ON result FIELDS database;
DEFINE INDEX IF NOT EXISTS idx_timestamp ON result FIELDS timestamp;

-- ----------------------------------------
-- Get all results for a specific database
-- ----------------------------------------

-- SELECT * FROM result WHERE database = 'Redis' ORDER BY timestamp DESC;

-- ----------------------------------------
-- Get latest results for each database
-- ----------------------------------------

-- SELECT database, creates.ops as create_ops, reads.ops as read_ops
-- FROM result
-- ORDER BY timestamp DESC
-- GROUP BY database;

-- ----------------------------------------
-- Compare results by system specs
-- ----------------------------------------

-- SELECT * FROM result
-- WHERE system_info.cpu_cores = 8
-- ORDER BY creates.ops DESC;

-- ----------------------------------------
-- Get historical trend for a database
-- ----------------------------------------

-- SELECT timestamp, creates.ops, reads.ops, updates.ops, deletes.ops
-- FROM result
-- WHERE database = 'SurrealDB (RocksDB)'
-- ORDER BY timestamp ASC;
