use super::Instance;
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
    if role != "storage" {
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
    label(
        instance
            .storage_group
            .as_deref()
            .context("instance.storage_group is required")?,
        "instance.storage_group",
    )?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
