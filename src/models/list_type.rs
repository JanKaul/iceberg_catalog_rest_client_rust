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
pub struct ListType {
    #[serde(rename = "type")]
    pub r#type: RHashType,
    #[serde(rename = "element-id")]
    pub element_id: i32,
    #[serde(rename = "element")]
    pub element: Box<crate::models::Type>,
    #[serde(rename = "element-required")]
    pub element_required: bool,
}

impl ListType {
    pub fn new(r#type: RHashType, element_id: i32, element: crate::models::Type, element_required: bool) -> ListType {
        ListType {
            r#type,
            element_id,
            element: Box::new(element),
            element_required,
        }
    }
}

/// 
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum RHashType {
    #[serde(rename = "list")]
    List,
}

impl Default for RHashType {
    fn default() -> RHashType {
        Self::List
    }
}
