/*
 * Apache Iceberg REST Catalog API
 *
 * Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.
 *
 * The version of the OpenAPI document: 0.0.1
 * 
 * Generated by: https://openapi-generator.tech
 */




#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct RenameTableRequest {
    #[serde(rename = "source")]
    pub source: Box<crate::models::TableIdentifier>,
    #[serde(rename = "destination")]
    pub destination: Box<crate::models::TableIdentifier>,
}

impl RenameTableRequest {
    pub fn new(source: crate::models::TableIdentifier, destination: crate::models::TableIdentifier) -> RenameTableRequest {
        RenameTableRequest {
            source: Box::new(source),
            destination: Box::new(destination),
        }
    }
}


