use super::{REPORT_LIMIT, wire};
use crate::model::{node::NodeReport, session::Run};
use tonic::Status;

pub(super) fn report(input: wire::NodeReportRequest) -> Result<(Run, NodeReport, u64), Status> {
    let invalid = || Status::invalid_argument("invalid node report");
    if prost::Message::encoded_len(&input) > REPORT_LIMIT || input.sample_interval_seconds > 86_400
    {
        return Err(invalid());
    }
    let run = super::read::run(input.run)?;
    let report: NodeReport = serde_json::from_slice(&input.report_json).map_err(|_| invalid())?;
    report.validate().map_err(|_| invalid())?;
    if report.boot_id != run.boot {
        return Err(invalid());
    }
    if input.sample_interval_seconds > 0 {
        crate::model::metrics::NodeSample::from_report(&report, input.sample_interval_seconds)
            .and_then(|sample| sample.validate())
            .map_err(|_| invalid())?;
    }
    Ok((run, report, input.sample_interval_seconds))
}
