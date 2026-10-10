use uuid::Uuid;

pub(crate) fn chunk_name(id: Uuid) -> String {
    let id = id.simple().to_string();
    format!("{}/{id}", &id[..2])
}

pub(crate) fn parse_chunk_name(name: &str) -> Option<Uuid> {
    let (shard, id) = name.split_once('/')?;
    if shard.len() != 2
        || id.len() != 32
        || !id.starts_with(shard)
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    Uuid::parse_str(id).ok()
}
pub(crate) fn parse_cache_name(name: &str) -> Option<(Uuid, bool)> {
    let (name, extension) = name.rsplit_once('.')?;
    let compressed = match extension {
        "raw" => false,
        "zst" => true,
        _ => return None,
    };
    Some((parse_chunk_name(name)?, compressed))
}

pub(crate) fn parse_upload_cache_name(name: &str) -> Option<Uuid> {
    let id = name
        .strip_suffix(".raw")
        .or_else(|| name.strip_suffix(".zst"))?;
    Uuid::parse_str(id).ok()
}
