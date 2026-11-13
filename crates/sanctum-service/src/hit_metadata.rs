use sanctum_core::Result;

use crate::{
    ChunkRef, MetadataEntry,
    db::CollectionDb,
    metadata_filter::{Filter, Level},
};

const MAX_HIT_METADATA_BYTES: usize = 4096;

#[derive(Default, Debug)]
pub struct HitMetadata {
    pub entries: Vec<HitMetadataEntry>,
    pub omitted: bool,
}

#[derive(Debug)]
pub struct HitMetadataEntry {
    pub level: Level,
    pub namespace: String,
    pub key: String,
    pub json: String,
}

pub(crate) fn metadata_for_hit(
    db: &CollectionDb,
    reference: &ChunkRef,
    filter: Option<&Filter>,
    catalog: &[MetadataEntry],
) -> Result<HitMetadata> {
    let mut metadata = HitMetadata::default();
    let Some(filter) = filter else {
        return Ok(metadata);
    };
    let mut fields = Vec::new();
    filter.visit_fields(&mut |level, namespace, key| {
        let field = (level, namespace.to_owned(), key.to_owned());
        if !fields.contains(&field) {
            fields.push(field);
        }
    });
    let mut bytes = 0;
    for (level, namespace, key) in fields {
        let value = match level {
            Level::Collection => catalog
                .iter()
                .find(|entry| entry.namespace == namespace && entry.key == key)
                .map(|entry| entry.json.clone()),
            _ => db.read_chunk_metadata_field(reference, level, &namespace, &key)?,
        };
        let Some(json) = value else {
            continue;
        };
        let size = namespace.len() + key.len() + json.len();
        if size > MAX_HIT_METADATA_BYTES - bytes {
            metadata.omitted = true;
            continue;
        }
        bytes += size;
        metadata.entries.push(HitMetadataEntry {
            level,
            namespace,
            key,
            json,
        });
    }
    Ok(metadata)
}
