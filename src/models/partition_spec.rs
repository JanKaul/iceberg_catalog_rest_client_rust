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
pub struct PartitionSpec {
    #[serde(rename = "spec-id", skip_serializing_if = "Option::is_none")]
    pub spec_id: Option<i32>,
    #[serde(rename = "fields")]
    pub fields: Vec<crate::models::PartitionField>,
}

impl PartitionSpec {
    pub fn new(fields: Vec<crate::models::PartitionField>) -> PartitionSpec {
        PartitionSpec {
            spec_id: None,
            fields,
        }
    }
}


