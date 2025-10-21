use crate::result::BenchmarkResult;
use anyhow::Result;
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::Client;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;

pub struct StorageClient {
	db: Surreal<Client>,
}

impl StorageClient {
	pub async fn connect(endpoint: &str) -> Result<Self> {
		// Create a new client
		let db = Surreal::new::<Ws>(endpoint).await?;
		// Sign in as root user
		db.signin(Root {
			username: "root".to_string(),
			password: "root".to_string(),
		})
		.await?;
		// Use namespace and database
		db.use_ns("surrealdb").use_db("crud-bench").await?;
		Ok(Self {
			db,
		})
	}

	pub async fn store_result(&self, result: &BenchmarkResult) -> Result<()> {
		// Create the schema if it doesn't exist
		self.db
			.query(
				r#"
				DEFINE TABLE IF NOT EXISTS result SCHEMAFULL;
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
				DEFINE INDEX IF NOT EXISTS idx_database ON result FIELDS database;
				DEFINE INDEX IF NOT EXISTS idx_timestamp ON result FIELDS timestamp;
			"#,
			)
			.await?;
		// Convert to serde_json::Value for insertion
		let result = serde_json::to_value(result)?;
		// Insert the result using a query
		self.db.query("CREATE result CONTENT $result").bind(("result", result)).await?;
		// All ok
		Ok(())
	}
}
