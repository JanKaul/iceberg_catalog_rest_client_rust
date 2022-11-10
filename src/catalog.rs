use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use iceberg_rs::{
    catalog::{namespace::Namespace, table_identifier::TableIdentifier, Catalog},
    model::{schema::SchemaV2, table_metadata::TableMetadata},
    object_store::{path::Path, ObjectStore},
    table::{table_builder::TableBuilder, Table},
};

use crate::{
    apis::{catalog_api_api, configuration},
    models::{self},
};

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
    /// Lists all namespaces in the catalog.
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>> {
        let namespaces =
            catalog_api_api::list_namespaces(&self.configuration, &self.name, parent).await?;
        namespaces
            .namespaces
            .ok_or_else(|| anyhow!("No tables found"))?
            .into_iter()
            .map(|x| Namespace::try_new(&x))
            .collect::<Result<Vec<Namespace>>>()
    }
    /// Create a table from an identifier and a schema
    async fn create_table(
        self: Arc<Self>,
        identifier: TableIdentifier,
        schema: SchemaV2,
    ) -> Result<Table> {
        let builder = self.build_table(identifier, schema).await?;
        builder.commit().await
    }
    /// Check if a table exists
    async fn table_exists(&self, identifier: &TableIdentifier) -> Result<bool> {
        catalog_api_api::table_exists(
            &self.configuration,
            &self.name,
            &identifier.namespace().to_string(),
            identifier.name(),
        )
        .await
        .map(|_| true)
        .map_err(anyhow::Error::msg)
    }
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &TableIdentifier) -> Result<()> {
        catalog_api_api::drop_table(
            &self.configuration,
            &self.name,
            &identifier.namespace().to_string(),
            identifier.name(),
            None,
        )
        .await
        .map_err(anyhow::Error::msg)
    }
    /// Load a table.
    async fn load_table(self: Arc<Self>, identifier: TableIdentifier) -> Result<Table> {
        let path: Path = catalog_api_api::load_table(
            &self.configuration,
            &self.name,
            &identifier.namespace().to_string(),
            identifier.name(),
        )
        .await
        .map(|x| x.metadata_location)?
        .ok_or(anyhow!("No metadata location provided."))
        .and_then(|y| url::Url::parse(&y).map_err(anyhow::Error::msg))?
        .path()
        .into();
        let bytes = &self
            .object_store
            .get(&path)
            .await
            .map_err(|err| anyhow!(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        let metadata: TableMetadata = serde_json::from_str(
            std::str::from_utf8(bytes).map_err(|err| anyhow!(err.to_string()))?,
        )
        .map_err(|err| anyhow!(err.to_string()))?;
        let catalog: Arc<dyn Catalog> = self;
        Ok(Table::new_metastore_table(
            identifier,
            Arc::clone(&catalog),
            metadata,
            &path.to_string(),
        )
        .await?)
    }
    /// Invalidate cached table metadata from current catalog.
    async fn invalidate_table(&self, _identifier: &TableIdentifier) -> Result<()> {
        unimplemented!()
    }
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        identifier: TableIdentifier,
        metadata_file_location: &str,
    ) -> Result<Table> {
        let mut request = models::CreateTableRequest::new(
            identifier.name().to_owned(),
            models::Schema::default(),
        );
        request.location = Some(metadata_file_location.to_owned());
        catalog_api_api::create_table(
            &self.configuration,
            &self.name,
            &identifier.namespace().to_string(),
            Some(request),
        )
        .await?;
        self.load_table(identifier).await
    }
    /// Update a table by atomically changing the pointer to the metadata file
    async fn update_table(
        self: Arc<Self>,
        identifier: TableIdentifier,
        metadata_file_location: &str,
        _previous_metadata_file_location: &str,
    ) -> Result<Table> {
        let mut update = models::TableUpdate::default();
        update.location = metadata_file_location.to_owned();
        let request = models::CommitTableRequest::new(vec![], vec![update]);
        catalog_api_api::update_table(
            &self.configuration,
            &self.name,
            &identifier.namespace().to_string(),
            identifier.name(),
            Some(request),
        )
        .await?;
        self.load_table(identifier).await
    }
    /// Instantiate a builder to either create a table or start a create/replace transaction.
    async fn build_table(
        self: Arc<Self>,
        identifier: TableIdentifier,
        schema: SchemaV2,
    ) -> Result<TableBuilder> {
        let location = self.base_path.clone() + &format!("{}", identifier).replace(".", "/");
        let catalog: Arc<dyn Catalog> = self;
        TableBuilder::new_metastore_table(&location, schema, identifier, Arc::clone(&catalog))
    }
    /// Initialize a catalog given a custom name and a map of catalog properties.
    /// A custom Catalog implementation must have a no-arg constructor. A compute engine like Spark
    /// or Flink will first initialize the catalog without any arguments, and then call this method to
    /// complete catalog initialization with properties passed into the engine.
    async fn initialize(self: Arc<Self>, _properties: &HashMap<String, String>) -> Result<()> {
        unimplemented!()
    }
    /// Return the associated object store to the catalog
    fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use iceberg_rs::{
        catalog::{table_identifier::TableIdentifier, Catalog},
        model::schema::{AllType, PrimitiveType, SchemaStruct, SchemaV2, StructField},
        object_store::{memory::InMemory, ObjectStore},
    };

    use crate::{apis::configuration::Configuration, catalog::RestCatalog};

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
    async fn test_create_update_drop_table() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> = Arc::new(RestCatalog::new(
            "my_catalog".to_owned(),
            configuration(),
            "/".to_owned(),
            object_store,
        ));
        let identifier = TableIdentifier::parse("load_table.table3").unwrap();
        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            name_mapping: None,
            struct_fields: SchemaStruct {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "one".to_string(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                    StructField {
                        id: 2,
                        name: "two".to_string(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                ],
            },
        };

        let mut table = Arc::clone(&catalog)
            .create_table(identifier.clone(), schema)
            .await
            .expect("Failed to create table");

        let exists = Arc::clone(&catalog)
            .table_exists(&identifier)
            .await
            .expect("Table doesn't exist");
        assert_eq!(exists, true);

        let metadata_location = table.metadata_location().to_string();

        let transaction = table.new_transaction();
        transaction.commit().await.expect("Transaction failed.");

        let new_metadata_location = table.metadata_location().to_string();

        assert_ne!(metadata_location, new_metadata_location);

        let _ = catalog
            .drop_table(&identifier)
            .await
            .expect("Failed to drop table.");

        Arc::clone(&catalog)
            .table_exists(&identifier)
            .await
            .expect_err("Table still exists");
    }
}
