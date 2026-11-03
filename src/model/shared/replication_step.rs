use super::*;

pub(crate) async fn step_in(db: &mut Tx<'_>, task: Execution, run: &mut q::Run) -> Result<Step> {
    if run.redundancy_id.is_some() && super::super::redundancy::prepare(db, run).await? {
        return Ok(Step::Continue);
    }
    if run.options.0.external() {
        if let Some(step) = super::super::replication_remote::preflight(db, run).await? {
            return Ok(step);
        }
        return Ok(
            match super::super::replication_remote::prepare(db, task, run).await? {
                Some(plan) => Step::External {
                    plan: Box::new(plan),
                    connections: q::connections(task.id).fetch_all(&mut **db).await?,
                },
                None => Step::Continue,
            },
        );
    }
    Ok(match run.phase.as_str() {
        "scan" => {
            scan(db, run).await?;
            Step::Continue
        }
        "plan" => {
            plan(db, run).await?;
            Step::Continue
        }
        "verify" => verification(db, task, run).await?,
        "publish" => {
            publish(db, task, run).await?;
            Step::Continue
        }
        "cancel" => {
            cancel(db, run).await?;
            Step::Continue
        }
        _ => return Err(Error::OperationAborted.into()),
    })
}
