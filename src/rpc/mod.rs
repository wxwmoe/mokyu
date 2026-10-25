pub(crate) mod client;
mod multipart;
mod native;
mod object;
pub(crate) mod read;
mod request;
pub(crate) mod server;
mod work;
mod write;

pub(crate) mod wire {
    tonic::include_proto!("mokyu.node.v1");
}

pub(crate) const MESSAGE_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const SESSION_LIMIT: usize = 64 * 1024;
pub(crate) const OBSERVATION_LIMIT: usize = 16 * 1024;
pub(crate) const PROTOCOL: u32 = 1;

fn uuid(value: &str) -> Result<uuid::Uuid, tonic::Status> {
    if value.len() != 36 {
        return Err(tonic::Status::invalid_argument("invalid UUID"));
    }
    uuid::Uuid::parse_str(value)
        .ok()
        .filter(|id| !id.is_nil())
        .ok_or_else(|| tonic::Status::invalid_argument("invalid UUID"))
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
