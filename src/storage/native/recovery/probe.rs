use super::media::Source;
use crate::config::Instance;
use anyhow::{Context, Result};
use rustls::{
    DigitallySignedStruct, Error, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature},
    pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject},
};
use serde_json::{Value, json};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tonic::transport::{ClientTlsConfig, Endpoint};

pub(crate) async fn status(instance: &Instance, credential: &Path) -> Result<Value> {
    let mut address = crate::config::storage::listen_address(instance)?;
    if address.ip().is_unspecified() {
        address.set_ip(match address.ip() {
            IpAddr::V4(_) => Ipv4Addr::LOCALHOST.into(),
            IpAddr::V6(_) => Ipv6Addr::LOCALHOST.into(),
        });
    }
    let tls = instance.storage_transport.as_deref() != Some("proxy");
    let scheme = if tls { "https" } else { "http" };
    let mut endpoint = Endpoint::from_shared(format!("{scheme}://{address}"))?
        .local_address(Some(address.ip()))
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(2));
    if tls {
        let path = instance
            .storage_tls_cert
            .as_ref()
            .context("missing storage certificate")?;
        let pem = crate::storage::local::read_bounded(path, 256 * 1024).await?;
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let verifier = LocalCertificate {
            leaf: CertificateDer::from_pem_slice(&pem)?,
            algorithms: provider.signature_verification_algorithms,
        };
        let _ = provider.install_default();
        let tls = ClientTlsConfig::new().domain_name(address.ip().to_string());
        endpoint = endpoint.tls_config_with_verifier(tls, Arc::new(verifier))?;
    }
    let (identity, _) = Source::connect(endpoint, credential).await?;
    Ok(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "role": "storage",
        "work": "recovery",
        "ready": true,
        "media_id": identity.media.media_id,
        "cluster_id": identity.manifest.cluster_id,
    }))
}

// Only the probe on the bound local address trusts the storage listener's configured leaf.
// The TLS handshake must still prove possession of its private key.
#[derive(Debug)]
struct LocalCertificate {
    leaf: CertificateDer<'static>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for LocalCertificate {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        if end_entity != &self.leaf {
            return Err(Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, cert, signature, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, signature, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}
