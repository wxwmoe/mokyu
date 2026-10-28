use super::{node_read as read, session::Run};

pub(crate) const LIMIT: usize = 32;

pub(crate) struct Request {
    pub(crate) run: Run,
    pub(crate) reads: Vec<read::Query>,
    pub(crate) cached: Vec<read::Version>,
}

pub(crate) struct Reply {
    pub(crate) reads: Vec<read::Reply>,
    pub(crate) materials: Vec<read::Material>,
}
