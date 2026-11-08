use object_store::path::Path;
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(crate) enum Layout {
    Local,
    S3,
}

impl Layout {
    pub(crate) fn for_local(is_local: bool) -> Self {
        if is_local { Self::Local } else { Self::S3 }
    }

    fn name(self, id: Uuid) -> String {
        let hex = id.simple().to_string();
        let count = match self {
            Self::Local => 3,
            Self::S3 => 9,
        };
        let mut name = String::with_capacity(count + count / 3 + hex.len());
        for (i, ch) in hex.bytes().rev().take(count).enumerate() {
            name.push(char::from(ch));
            if i % 3 == 2 {
                name.push('/');
            }
        }
        name.push_str(&hex);
        name
    }

    pub(crate) fn chunk(self, id: Uuid) -> Path {
        Path::from(format!("chunks/{}", self.name(id)))
    }

    pub(crate) fn pack(self, id: Uuid) -> Path {
        Path::from(format!("packs/{}", self.name(id)))
    }

    pub(crate) fn object(self, id: Uuid) -> Path {
        Path::from(format!("objects/{}", self.name(id)))
    }

    pub(crate) fn parse_name(self, name: &str) -> Option<Uuid> {
        let (_, file) = name.rsplit_once('/')?;
        let id = Uuid::parse_str(file).ok()?;
        (self.name(id) == name).then_some(id)
    }

    pub(crate) fn parse_chunk_or_pack(self, path: &str) -> Option<Uuid> {
        let (directory, name) = path.split_once('/')?;
        match directory {
            "chunks" | "packs" => self.parse_name(name),
            _ => None,
        }
    }
}

pub(crate) fn chunk_name(id: Uuid) -> String {
    Layout::Local.name(id)
}

pub(crate) fn parse_chunk_name(name: &str) -> Option<Uuid> {
    Layout::Local.parse_name(name)
}

pub(crate) fn owns_node_file(name: &str, objects: bool) -> bool {
    let (base, extension) = name.split_once('.').unwrap_or((name, ""));
    parse_chunk_name(base).is_some()
        && if objects {
            matches!(extension, "" | "part" | "map" | "tmp" | "map.tmp")
        } else {
            matches!(extension, "raw" | "zst" | "raw.tmp" | "zst.tmp")
        }
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
    fn carrier_paths_use_reversed_uuid_characters() {
        for (hex, local, s3) in [
            ("019a111122227abc8def000123456789", "987", "987/654/321"),
            ("019a111122227abc8def0000a0b0c0d0", "0d0", "0d0/c0b/0a0"),
        ] {
            let id = Uuid::parse_str(hex).unwrap();
            for (layout, shard) in [(Layout::Local, local), (Layout::S3, s3)] {
                let name = format!("{shard}/{hex}");
                assert_eq!(layout.chunk(id).as_ref(), format!("chunks/{name}"));
                assert_eq!(layout.pack(id).as_ref(), format!("packs/{name}"));
                assert_eq!(layout.object(id).as_ref(), format!("objects/{name}"));
                assert_eq!(layout.parse_name(&name), Some(id));
                assert_eq!(layout.parse_name(&name.to_uppercase()), None);
                assert_eq!(layout.parse_name(&format!("../{name}")), None);
            }
            assert!(Layout::Local.parse_name(&Layout::S3.name(id)).is_none());
            assert!(Layout::S3.parse_name(&Layout::Local.name(id)).is_none());
            assert_eq!(
                parse_cache_name(&format!("{local}/{hex}.raw")),
                Some((id, false))
            );
        }
    }

    #[test]
    fn node_scan_only_owns_canonical_files() {
        let id = Uuid::from_u128(0x123456789);
        let name = chunk_name(id);
        for suffix in ["", ".part", ".map", ".map.tmp"] {
            assert!(owns_node_file(&format!("{name}{suffix}"), true));
        }
        for suffix in [".raw", ".zst", ".raw.tmp"] {
            assert!(owns_node_file(&format!("{name}{suffix}"), false));
        }
        for file in [
            format!("{name}.partial"),
            format!("89/{}.part", id.simple()),
            format!("{name}.unknown"),
        ] {
            assert!(!owns_node_file(&file, true));
            assert!(!owns_node_file(&file, false));
        }
        assert!(crate::storage::is_fragment_key(
            &crate::storage::fragment_key(id)
        ));
        for key in [
            "multipart/fragment/123",
            "multipart/fragment/../mokyu.json",
            "multipart/pending/00000000000000000000000000000000",
        ] {
            assert!(!crate::storage::is_fragment_key(key));
        }
    }
}
