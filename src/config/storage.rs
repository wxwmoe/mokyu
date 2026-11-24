use super::{Budget, Instance, limits::Environment};
use anyhow::{Context, Result, ensure};
use std::net::{IpAddr, SocketAddr};

pub(crate) fn label(value: &str, field: &str) -> Result<()> {
    ensure!(
        (1..=128).contains(&value.len())
            && value.trim() == value
            && !value.chars().any(char::is_control),
        "{field} must contain 1..128 nonblank bytes"
    );
    Ok(())
}

pub(crate) fn url(value: &str) -> Result<()> {
    let address = reqwest::Url::parse(value).context("invalid instance.storage_url")?;
    ensure!(
        value.len() <= 2048
            && value.trim() == value
            && address.scheme() == "https"
            && address.host_str().is_some()
            && address.username().is_empty()
            && address.password().is_none()
            && address.path() == "/"
            && address.query().is_none()
            && address.fragment().is_none()
            && address.port_or_known_default().is_some_and(|port| port > 0)
            && address.host_str().is_some_and(|host| {
                host.trim_matches(['[', ']'])
                    .parse::<IpAddr>()
                    .map_or(true, |ip| !ip.is_unspecified() && !ip.is_multicast())
            }),
        "instance.storage_url must be an HTTPS origin without credentials, path, query or fragment"
    );
    Ok(())
}

pub(crate) fn validate(instance: &Instance, role: &str) -> Result<()> {
    if let Some(region) = &instance.region {
        label(region, "instance.region")?;
    }
    if !matches!(role, "storage" | "storage-recovery") {
        for (key, present) in [
            ("storage_url", instance.storage_url.is_some()),
            ("storage_group", instance.storage_group.is_some()),
            ("storage_purpose", instance.storage_purpose.is_some()),
            ("listen_storage", instance.listen_storage.is_some()),
            ("storage_transport", instance.storage_transport.is_some()),
            ("storage_tls_cert", instance.storage_tls_cert.is_some()),
            ("storage_tls_key", instance.storage_tls_key.is_some()),
        ] {
            ensure!(!present, "instance.{key} requires the storage role");
        }
        return Ok(());
    }
    url(instance
        .storage_url
        .as_deref()
        .context("instance.storage_url is required")?)?;
    if role == "storage" || instance.storage_group.is_some() {
        label(
            instance
                .storage_group
                .as_deref()
                .context("instance.storage_group is required")?,
            "instance.storage_group",
        )?;
    }
    ensure!(
        matches!(
            instance.storage_purpose.as_deref().unwrap_or("primary"),
            "primary" | "backup"
        ),
        "instance.storage_purpose must be primary or backup"
    );
    let listen: SocketAddr = instance
        .listen_storage
        .as_deref()
        .unwrap_or("0.0.0.0:9003")
        .parse()
        .context("invalid instance.listen_storage")?;
    ensure!(
        listen.port() != 0,
        "instance.listen_storage requires a fixed port"
    );
    match instance.storage_transport.as_deref().unwrap_or("tls") {
        "tls" => {
            for (name, value) in [
                ("storage_tls_cert", &instance.storage_tls_cert),
                ("storage_tls_key", &instance.storage_tls_key),
            ] {
                ensure!(
                    value
                        .as_ref()
                        .is_some_and(|path| !path.as_os_str().is_empty()),
                    "instance.{name} is required for direct TLS"
                );
            }
        }
        "proxy" => {
            ensure!(
                instance.storage_tls_cert.is_none() && instance.storage_tls_key.is_none(),
                "storage_tls_cert and storage_tls_key require storage_transport=tls"
            );
            let private = match listen.ip() {
                IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
                IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
            };
            ensure!(
                private,
                "instance.listen_storage must bind a loopback or private address in proxy mode"
            );
        }
        _ => anyhow::bail!("instance.storage_transport must be tls or proxy"),
    }
    Ok(())
}

impl Budget {
    pub(crate) fn for_storage(instance: &Instance) -> Result<Self> {
        let (environment, cpus, memory) = Self::environment();
        Self::storage(instance, environment, cpus, memory)
    }

    fn storage(
        instance: &Instance,
        environment: Environment,
        cpus: usize,
        memory: u64,
    ) -> Result<Self> {
        let inflight = instance
            .stream_memory_budget
            .as_deref()
            .map(super::limits::bytes)
            .transpose()?
            .unwrap_or(memory / 4);
        ensure!(
            inflight <= memory / 2,
            "storage stream_memory_budget must leave half of memory for runtime and OS"
        );
        let connection_bytes = (crate::model::storage_transfer::GRANT_LIMIT
            * crate::model::storage_transfer::STREAMS_PER_CONNECTION)
            as u64;
        let connections = instance
            .request_connections
            .unwrap_or((inflight / (connection_bytes * 2)).clamp(2, 32) as usize);
        let wire_bytes = (connections as u64)
            .checked_mul(connection_bytes)
            .context("storage connection budget overflow")?;
        // Each active operation has room for its checksum tree and bounded wire/file buffers.
        let slot_bytes = 2 * 1024 * 1024;
        let slots = (inflight.saturating_sub(wire_bytes) / slot_bytes) as usize;
        ensure!(
            slots >= 3 && connections > 0,
            "storage stream_memory_budget must cover connections and read, write and control slots"
        );
        let reads = instance
            .storage_read_slots
            .unwrap_or((cpus * 2).min(slots / 3).max(1));
        let writes = instance
            .storage_upload_slots
            .unwrap_or((cpus * 2).min(slots / 3).max(1));
        let controls = instance
            .storage_control_slots
            .unwrap_or(cpus.min(8).min(slots / 3).max(1));
        let cpu_jobs = instance.cpu_jobs.unwrap_or(cpus.min(slots));
        ensure!(
            [reads, writes, controls, cpu_jobs]
                .into_iter()
                .all(|v| v > 0)
                && cpu_jobs <= slots
                && reads
                    .checked_add(writes)
                    .and_then(|v| v.checked_add(controls))
                    .is_some_and(|v| v <= slots),
            "storage resource budgets exceed stream_memory_budget"
        );
        Ok(Self {
            environment,
            available_cpus: cpus,
            memory_bytes: memory,
            cpu_jobs,
            data_slots: slots,
            slot_bytes,
            compression_workspace: 0,
            upload_concurrency: writes,
            read_concurrency: reads,
            storage_concurrency: reads,
            storage_read_slots: reads,
            storage_upload_slots: writes,
            storage_control_slots: controls,
            request_connections: connections,
            database_connections: 0,
            database_read_connections: 0,
            cache_entries: 0,
            pack_memory_units: 0,
            response_cache_bytes: 0,
            write_memory_bytes: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_budget_covers_streams_without_data_codec_buffers() -> Result<()> {
        let mut instance = Instance {
            stream_memory_budget: Some("128MiB".into()),
            request_connections: Some(16),
            storage_read_slots: Some(2),
            storage_upload_slots: Some(2),
            storage_control_slots: Some(2),
            cpu_jobs: Some(2),
            ..Default::default()
        };
        let budget = Budget::storage(&instance, Environment::default(), 2, 256 << 20)?;
        assert_eq!(
            (budget.pack_memory_units, budget.compression_workspace),
            (0, 0)
        );
        assert_eq!(
            budget.storage_read_slots + budget.storage_upload_slots + budget.storage_control_slots,
            6
        );
        instance.request_connections = Some(usize::MAX);
        assert!(Budget::storage(&instance, Environment::default(), 2, 256 << 20).is_err());
        instance.request_connections = Some(16);
        instance.storage_read_slots = Some(64);
        assert!(Budget::storage(&instance, Environment::default(), 2, 256 << 20).is_err());
        assert!(Budget::storage(&Instance::default(), Environment::default(), 2, 64 << 20).is_ok());
        Ok(())
    }

    #[test]
    fn topology_and_transport_are_role_scoped() {
        let base = "[instance]\ncontrol_url='https://control.example'\n";
        for role in ["data", "worker", "node"] {
            assert!(
                crate::config::node::NodeConfig::parse(&format!("{base}region='dc-1'"), role)
                    .is_ok()
            );
            for key in [
                "storage_url",
                "storage_group",
                "storage_purpose",
                "listen_storage",
                "storage_transport",
                "storage_tls_cert",
                "storage_tls_key",
            ] {
                let error =
                    crate::config::node::NodeConfig::parse(&format!("{base}{key}='unused'"), role)
                        .err()
                        .unwrap();
                assert!(error.to_string().contains(key));
            }
        }
        let mut instance = Instance {
            storage_url: Some("https://127.0.0.1:9443".into()),
            storage_group: Some("host-1".into()),
            storage_tls_cert: Some("cert.pem".into()),
            storage_tls_key: Some("key.pem".into()),
            ..Default::default()
        };
        assert!(validate(&instance, "storage").is_ok());
        for invalid in [
            "",
            "http://host",
            "https://user@host",
            "https://host/path",
            "https://host/?x=1",
            "https://host/#x",
            "https://0.0.0.0",
            "https://[::]",
            "https://host:0",
        ] {
            assert!(url(invalid).is_err(), "{invalid}");
        }
        for role in ["single", "control"] {
            assert!(validate(&instance, role).is_err());
        }
        instance.storage_transport = Some("proxy".into());
        assert!(validate(&instance, "storage").is_err());
        instance.storage_tls_cert = None;
        instance.storage_tls_key = None;
        assert!(validate(&instance, "storage").is_err());
        instance.listen_storage = Some("127.0.0.1:9003".into());
        assert!(validate(&instance, "storage").is_ok());
        instance.region = Some(" ".into());
        assert!(validate(&instance, "storage").is_err());
    }
}
