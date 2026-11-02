pub(crate) mod client;
mod metrics;
mod multipart;
mod native;
mod notifications;
mod object;
pub(crate) mod read;
mod request;
pub(crate) mod server;
mod session;
mod work;
mod write;

pub(crate) mod wire {
    tonic::include_proto!("mokyu.node.v1");
}

pub(crate) const MESSAGE_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const REPORT_LIMIT: usize = 96 * 1024;
pub(crate) const SESSION_LIMIT: usize = 64 * 1024;
pub(crate) const OBSERVATION_LIMIT: usize = 2 * 1024 * 1024;
pub(crate) const PROTOCOL: u32 = 1;

fn database_id(value: &str) -> Result<crate::model::Id, tonic::Status> {
    if value.len() == 36 {
        return uuid(value).map(crate::model::Id::from);
    }
    let invalid = || tonic::Status::invalid_argument("invalid database identity");
    let id: crate::model::Id = value.parse().map_err(|_| invalid())?;
    if !id.is_valid() || id.to_string() != value {
        return Err(invalid());
    }
    Ok(id)
}

fn uuid(value: &str) -> Result<uuid::Uuid, tonic::Status> {
    if value.len() != 36 {
        return Err(tonic::Status::invalid_argument("invalid UUID"));
    }
    uuid::Uuid::parse_str(value)
        .ok()
        .filter(|id| !id.is_nil())
        .ok_or_else(|| tonic::Status::invalid_argument("invalid UUID"))
}

#[cfg(test)]
mod identity_tests {
    use super::database_id;
    use crate::model::Id;
    use uuid::Uuid;

    #[test]
    fn wire_database_ids_keep_their_native_type() {
        let id = Uuid::new_v4();
        assert_eq!(
            database_id(&id.to_string().to_uppercase()).unwrap(),
            Id::Uuid(id)
        );
        assert_eq!(database_id("42").unwrap(), Id::Integer(42));
        for invalid in [
            "0",
            "01",
            "-1",
            "bad",
            "9223372036854775808",
            "00000000-0000-0000-0000-000000000000",
        ] {
            assert!(database_id(invalid).is_err(), "{invalid}");
        }
    }
}

fn section(name: &str) -> Option<wire::Section> {
    Some(match name {
        "requests" => wire::Section::Requests,
        "uploads" => wire::Section::Uploads,
        "metrics" => wire::Section::Metrics,
        "maintenance" => wire::Section::Maintenance,
        "io" => wire::Section::Io,
        _ => return None,
    })
}
