use crate::{
    app::App,
    authorization::{Action, Permit},
    model::{
        StreamPurpose,
        time_recovery::{REMOVE, Work},
    },
};
use anyhow::Result;
use std::{sync::Arc, time::Duration};

async fn perform(app: &App, w: &Work) -> Result<()> {
    app.writable()?;
    anyhow::ensure!(!w.item.target_changed, s3s::s3_error!(PreconditionFailed));
    #[cfg(feature = "fault-injection")]
    crate::faults::point("time-restore-before-item").await;
    if w.item.state == "remove" {
        return REMOVE
            .scope(
                w.step.clone(),
                app.change_object(
                    &w.actor,
                    w.target,
                    &w.item.target,
                    w.item.expected,
                    None,
                    None,
                ),
            )
            .await;
    }
    let source = w
        .stream
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("recovery source missing"))?;
    let mut authority = Permit::for_action(w.actor.clone(), w.target, Action::Write);
    authority.add(w.source, Action::RestoreVersion);
    authority.time_step = Some(w.step.clone());
    let (out, _write) = app
        .new_stream(
            &authority,
            w.target,
            &w.item.target,
            StreamPurpose::Object,
            source.metadata.clone(),
            false,
            None,
            None,
        )
        .await?;
    let result = async {
        app.reserve_quota(out, source.size, false, None).await?;
        app.copy_extents(out, source).await?;
        app.finish_stream(out, source.size, &source.etag, source.checksums.clone())
            .await?;
        app.publish(out, (w.target, &w.item.target), None, None)
            .await
    }
    .await;
    if result.is_err() {
        let _ = app.objects.abandon_copy(out).await;
    }
    result
}
async fn leased(app: &App, w: &Work) -> Result<()> {
    let op = perform(app, w);
    tokio::pin!(op);
    loop {
        tokio::select! {
            result=&mut op=>return result,
            _=tokio::time::sleep(Duration::from_secs(8))=>{
                anyhow::ensure!(app.objects.time_renew(&w.step).await?,s3s::s3_error!(PreconditionFailed));
            }
        }
    }
}

pub(super) async fn run(app: Arc<App>) -> Result<()> {
    let mut last = tokio::time::Instant::now() - Duration::from_secs(60);
    loop {
        let wake = app.wake_maintenance.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        if let Some(_control) = app.control_admission.enter()
            && app.writable().is_ok()
        {
            if last.elapsed() >= Duration::from_secs(10) {
                if let Err(e) = app.objects.time_expire().await {
                    tracing::warn!(error=%e,"time recovery cleanup failed");
                }
                last = tokio::time::Instant::now();
            }
            match app.objects.time_work().await {
                Ok(Some(work)) => match leased(&app, &work).await {
                    Ok(()) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(e)
                        if e.downcast_ref::<s3s::S3Error>()
                            .is_some_and(|e| e.code() == &s3s::S3ErrorCode::PreconditionFailed) =>
                    {
                        app.objects.time_conflict(&work.step).await?;
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(plan=%work.step.plan,error=%e,"time recovery item is pending")
                    }
                },
                Ok(None) => {}
                Err(e) => tracing::warn!(error=%e,"time recovery could not load a plan"),
            }
        }
        tokio::select! {_=wake=>(),_=tokio::time::sleep(Duration::from_secs(2))=>()}
    }
}
