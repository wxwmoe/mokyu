use uuid::Uuid;

pub(crate) fn chunk_name(id: Uuid) -> String {
    let id = id.simple().to_string();
    format!("{}/{id}", &id[30..])
}

pub(crate) fn parse_chunk_name(name: &str) -> Option<Uuid> {
    let (shard, id) = name.split_once('/')?;
    if shard.len() != 2
        || id.len() != 32
        || !id.ends_with(shard)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_shards_use_random_suffix_and_reject_other_paths() {
        let id = Uuid::parse_str("019a00ab-cdef-7123-8abc-001122334455").unwrap();
        let name = chunk_name(id);
        assert_eq!(name, "55/019a00abcdef71238abc001122334455");
        assert_eq!(parse_chunk_name(&name), Some(id));
        assert_eq!(parse_cache_name(&format!("{name}.raw")), Some((id, false)));
        assert_eq!(
            parse_chunk_name("01/019a00abcdef71238abc001122334455"),
            None
        );
        assert_eq!(
            parse_chunk_name("../019a00abcdef71238abc001122334455"),
            None
        );
    }
}
