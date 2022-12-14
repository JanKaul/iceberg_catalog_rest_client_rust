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
pub struct StructType {
    #[serde(rename = "type")]
    pub r#type: RHashType,
    #[serde(rename = "fields")]
    pub fields: Vec<crate::models::StructField>,
}

impl StructType {
    pub fn new(r#type: RHashType, fields: Vec<crate::models::StructField>) -> StructType {
        StructType {
            r#type,
            fields,
        }
    }
}

/// 
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum RHashType {
    #[serde(rename = "struct")]
    Struct,
}

impl Default for RHashType {
    fn default() -> RHashType {
        Self::Struct
    }
}

