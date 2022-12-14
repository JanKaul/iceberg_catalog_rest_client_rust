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
pub struct AddSnapshotUpdateAllOf {
    #[serde(rename = "snapshot")]
    pub snapshot: Box<crate::models::Snapshot>,
}

impl AddSnapshotUpdateAllOf {
    pub fn new(snapshot: crate::models::Snapshot) -> AddSnapshotUpdateAllOf {
        AddSnapshotUpdateAllOf {
            snapshot: Box::new(snapshot),
        }
    }
}


