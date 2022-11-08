use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use iceberg_rs::{
    catalog::{namespace::Namespace, table_identifier::TableIdentifier, Catalog},
    model::schema::SchemaV2,
    object_store::ObjectStore,
    table::{table_builder::TableBuilder, Table},
};

use crate::apis::{catalog_api_api, configuration};

pub struct RestCatalog {
    name: String,
    configuration: configuration::Configuration,
    base_path: String,
    object_store: Arc<dyn ObjectStore>,
}

impl RestCatalog {
    pub fn new(
        name: String,
        configuration: configuration::Configuration,
        base_path: String,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        RestCatalog {
            name,
            configuration,
            base_path,
            object_store,
        }
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    /// Lists all tables in the given namespace.
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableIdentifier>> {
        let tables =
            catalog_api_api::list_tables(&self.configuration, &self.name, &namespace.to_string())
                .await?;
        tables
            .identifiers
            .ok_or_else(|| anyhow!("No tables found"))?
            .into_iter()
            .map(|x| {
                let mut vec = x.namespace;
                vec.push(x.name);
                TableIdentifier::try_new(&vec)
            })
            .collect::<Result<Vec<TableIdentifier>>>()
    }
    /// Create a table from an identifier and a schema
    async fn create_table(
        self: Arc<Self>,
        identifier: TableIdentifier,
        schema: SchemaV2,
    ) -> Result<Table> {
        unimplemented!()
    }
    /// Check if a table exists
    async fn table_exists(&self, identifier: &TableIdentifier) -> Result<bool> {
        unimplemented!()
    }
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &TableIdentifier) -> Result<()> {
        unimplemented!()
    }
    /// Load a table.
    async fn load_table(self: Arc<Self>, identifier: TableIdentifier) -> Result<Table> {
        unimplemented!()
    }
    /// Invalidate cached table metadata from current catalog.
    async fn invalidate_table(&self, identifier: &TableIdentifier) -> Result<()> {
        unimplemented!()
    }
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        identifier: TableIdentifier,
        metadata_file_location: &str,
    ) -> Result<Table> {
        unimplemented!()
    }
    /// Update a table by atomically changing the pointer to the metadata file
    async fn update_table(
        self: Arc<Self>,
        identifier: TableIdentifier,
        metadata_file_location: &str,
        previous_metadata_file_location: &str,
    ) -> Result<Table> {
        unimplemented!()
    }
    /// Instantiate a builder to either create a table or start a create/replace transaction.
    async fn build_table(
        self: Arc<Self>,
        identifier: TableIdentifier,
        schema: SchemaV2,
    ) -> Result<TableBuilder> {
        unimplemented!()
    }
    /// Initialize a catalog given a custom name and a map of catalog properties.
    /// A custom Catalog implementation must have a no-arg constructor. A compute engine like Spark
    /// or Flink will first initialize the catalog without any arguments, and then call this method to
    /// complete catalog initialization with properties passed into the engine.
    async fn initialize(self: Arc<Self>, properties: &HashMap<String, String>) -> Result<()> {
        unimplemented!()
    }
    /// Return the associated object store to the catalog
    fn object_store(&self) -> Arc<dyn ObjectStore> {
        unimplemented!()
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use iceberg_rs::{
        catalog::{namespace::Namespace, Catalog},
        object_store::{memory::InMemory, ObjectStore},
    };

    use crate::{
        apis::{self, configuration::Configuration},
        catalog::RestCatalog,
        models::{self, schema, Schema},
    };

    fn configuration() -> Configuration {
        Configuration {
            base_path: "http://localhost:8080".to_string(),
            user_agent: None,
            client: reqwest::Client::new(),
            basic_auth: None,
            oauth_access_token: None,
            bearer_access_token: None,
            api_key: None,
        }
    }

    #[tokio::test]
    async fn list_tables() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = RestCatalog::new(
            "my_catalog".to_owned(),
            configuration(),
            "/".to_owned(),
            object_store,
        );
        let namespace_request1 = models::CreateNamespaceRequest {
            namespace: vec!["list_tables".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request1),
        )
        .await;

        let mut request1 = models::CreateTableRequest::new(
            "list_tables1".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request1.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "list_tables",
            Some(request1),
        )
        .await
        .expect("Failed to create table");
        let mut request2 = models::CreateTableRequest::new(
            "list_tables2".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request2.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "list_tables",
            Some(request2),
        )
        .await
        .expect("Failed to create table");

        let response = catalog
            .list_tables(
                &Namespace::try_new(&vec!["list_tables".to_owned()])
                    .expect("Failed to create Namespace"),
            )
            .await
            .expect("Failed to get tables.");
        assert_eq!(
            response
                .into_iter()
                .map(|x| x.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["list_tables1", "list_tables2"]
        );
    }
}
