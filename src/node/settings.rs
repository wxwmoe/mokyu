use crate::{
    config::{Instance, RuntimeSettings},
    rpc::wire,
};
use anyhow::{Context, Result, ensure};
use std::collections::{BTreeMap, HashSet};

#[derive(Clone, Default)]
pub(super) struct Settings {
    pub(super) runtime: RuntimeSettings,
    revisions: BTreeMap<i32, u64>,
}

impl Settings {
    pub(super) fn cached(&self) -> Vec<wire::Revision> {
        self.revisions
            .iter()
            .map(|(&section, &revision)| wire::Revision { section, revision })
            .collect()
    }

    pub(super) fn matches(&self, state: &wire::NodeState) -> Result<bool> {
        Ok(self.revisions == revisions(&state.revisions)?)
    }

    pub(super) fn update(&self, instance: &Instance, reply: &wire::SyncReply) -> Result<Self> {
        let expected = revisions(
            &reply
                .state
                .as_ref()
                .context("control omitted node state")?
                .revisions,
        )?;
        ensure!(
            reply.settings.len() <= 5,
            "too many runtime settings sections"
        );
        let mut next = self.clone();
        let mut changed = HashSet::new();
        for setting in &reply.settings {
            let version = setting
                .version
                .as_ref()
                .context("control omitted settings revision")?;
            ensure!(
                expected.get(&version.section) == Some(&version.revision)
                    && changed.insert(version.section),
                "invalid settings revision or duplicate section"
            );
            let name = name(version.section)?;
            let value: serde_json::Value = serde_json::from_slice(&setting.json)
                .map_err(|_| anyhow::anyhow!("invalid runtime settings JSON"))?;
            ensure!(
                name != "io" || value.get("sqlite_synchronous").is_none(),
                "node settings cannot configure SQLite"
            );
            next.runtime.apply_section(name, value)?;
            next.revisions.insert(version.section, version.revision);
        }
        ensure!(
            next.revisions == expected,
            "control omitted uncached runtime settings"
        );
        for (section, previous) in &self.revisions {
            ensure!(
                expected.get(section).is_some_and(|value| value >= previous),
                "runtime settings revision regressed"
            );
        }
        crate::config::limits::validate_runtime(instance, &next.runtime)?;
        Ok(next)
    }
}

fn name(section: i32) -> Result<&'static str> {
    Ok(match wire::Section::try_from(section) {
        Ok(wire::Section::Requests) => "requests",
        Ok(wire::Section::Uploads) => "uploads",
        Ok(wire::Section::Metrics) => "metrics",
        Ok(wire::Section::Maintenance) => "maintenance",
        Ok(wire::Section::Io) => "io",
        _ => anyhow::bail!("unknown node settings section"),
    })
}

fn revisions(values: &[wire::Revision]) -> Result<BTreeMap<i32, u64>> {
    ensure!(
        values.len() == 5,
        "control must advertise all node settings revisions"
    );
    let mut result = BTreeMap::new();
    for value in values {
        name(value.section)?;
        ensure!(
            value.revision > 0
                && value.revision <= i64::MAX as u64
                && result.insert(value.section, value.revision).is_none(),
            "invalid or duplicate settings revision"
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reply() -> wire::SyncReply {
        let revisions: Vec<_> = (1..=5)
            .map(|section| wire::Revision {
                section,
                revision: 1,
            })
            .collect();
        wire::SyncReply {
            valid_for_ms: 90_000,
            state: Some(wire::NodeState {
                native_pending: false,
                maintenance: false,
                draining: false,
                revisions: revisions.clone(),
            }),
            settings: revisions
                .into_iter()
                .map(|version| wire::Settings {
                    version: Some(version),
                    json: b"{}".to_vec(),
                })
                .collect(),
        }
    }

    #[test]
    fn settings_are_complete_atomic_and_revision_checked() {
        let instance = Instance::default();
        let empty = Settings::default();
        let initial = reply();
        let held = empty.update(&instance, &initial).unwrap();
        assert_eq!(held.cached().len(), 5);
        let mut cached = initial.clone();
        cached.settings.clear();
        assert!(empty.update(&instance, &cached).is_err());
        assert!(held.update(&instance, &cached).is_ok());

        let mut changed = cached.clone();
        changed.state.as_mut().unwrap().revisions[0].revision = 3;
        assert!(!held.matches(changed.state.as_ref().unwrap()).unwrap());
        assert!(held.update(&instance, &changed).is_err());
        changed.settings.push(wire::Settings {
            version: Some(wire::Revision {
                section: 1,
                revision: 3,
            }),
            json: serde_json::to_vec(&json!({"read_idle_timeout":"31s"})).unwrap(),
        });
        let next = held.update(&instance, &changed).unwrap();
        assert_eq!(next.runtime.requests.read_idle_timeout, "31s");
        assert_ne!(held.runtime.requests.read_idle_timeout, "31s");
        assert!(next.update(&instance, &initial).is_err());
        assert!(next.matches(changed.state.as_ref().unwrap()).unwrap());

        for section in [0, 6, -1] {
            let mut invalid = initial.clone();
            invalid.state.as_mut().unwrap().revisions[0].section = section;
            assert!(empty.update(&instance, &invalid).is_err());
        }
        let mut duplicate = initial.clone();
        duplicate.state.as_mut().unwrap().revisions[0].section = 2;
        assert!(empty.update(&instance, &duplicate).is_err());
        let mut duplicate = initial.clone();
        duplicate.settings[0] = duplicate.settings[1].clone();
        assert!(empty.update(&instance, &duplicate).is_err());
        for patch in [
            json!({"read_idle_timeout":"0s"}),
            json!({"unknown_secret":"hidden"}),
        ] {
            let mut invalid = initial.clone();
            invalid.settings[0].json = serde_json::to_vec(&patch).unwrap();
            assert!(held.update(&instance, &invalid).is_err());
        }
        let mut invalid = initial;
        invalid.settings[4].json = br#"{"sqlite_synchronous":"off"}"#.to_vec();
        assert!(held.update(&instance, &invalid).is_err());
        assert_eq!(held.cached()[0].revision, 1);
    }
}
