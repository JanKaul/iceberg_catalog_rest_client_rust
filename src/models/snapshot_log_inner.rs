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
pub struct SnapshotLogInner {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i32,
}

impl SnapshotLogInner {
    pub fn new(snapshot_id: i32, timestamp_ms: i32) -> SnapshotLogInner {
        SnapshotLogInner {
            snapshot_id,
            timestamp_ms,
        }
    }
}


