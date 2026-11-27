//! Trust configuration for the remote wallet endpoint.

use std::{net::Ipv6Addr, sync::Arc, time::Duration};

use rustls::{
    CertificateError, DigitallySignedStruct, Error, RootCertStore, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject as _},
    server::ParsedCertificate,
};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use tonic::transport::{ClientTlsConfig, Endpoint, Uri};

use crate::IndexerError;

#[derive(Clone)]
pub(super) enum TlsRoots {
    Pem(Vec<u8>),
    Der(Vec<Vec<u8>>),
}

impl TlsRoots {
    fn trust_store(&self) -> Result<RootCertStore, IndexerError> {
        let invalid_cert = || IndexerError::invalid_request("invalid TLS CA certificate");
        let mut roots = RootCertStore::empty();
        match self {
            Self::Pem(bundle) => {
                validate_pem_framing(bundle)?;
                for certificate in CertificateDer::pem_slice_iter(bundle) {
                    roots
                        .add(certificate.map_err(|_| invalid_cert())?)
                        .map_err(|_| invalid_cert())?;
                }
            }
            Self::Der(certificates) => {
                for certificate in certificates {
                    roots
                        .add(CertificateDer::from(certificate.as_slice()))
                        .map_err(|_| invalid_cert())?;
                }
            }
        }
        if roots.is_empty() {
            return Err(IndexerError::invalid_request("TLS CA bundle is empty"));
        }
        Ok(roots)
    }
}

fn validate_pem_framing(bundle: &[u8]) -> Result<(), IndexerError> {
    let malformed = || IndexerError::invalid_request("invalid TLS CA PEM framing");
    let mut open_label = None;
    // Rustls skips non-certificate blocks and text outside blocks.
    // A second BEGIN can replace an open label.
    for line in bundle.split(|byte| matches!(byte, b'\r' | b'\n')) {
        let line = line.trim_ascii_end();
        if let Some(label) = line.strip_prefix(b"-----BEGIN ") {
            let label = label
                .strip_suffix(b"-----")
                .filter(|label| *label == b"CERTIFICATE")
                .ok_or_else(malformed)?;
            if open_label.replace(label).is_some() {
                return Err(malformed());
            }
        } else if let Some(label) = line.strip_prefix(b"-----END ") {
            let label = label.strip_suffix(b"-----").ok_or_else(malformed)?;
            if open_label.take() != Some(label) {
                return Err(malformed());
            }
        } else if open_label.is_none() && !line.is_empty() {
            return Err(malformed());
        }
    }
    if open_label.is_some() {
        return Err(malformed());
    }
    Ok(())
}

pub(super) fn configure(
    mut endpoint: Endpoint,
    roots: Option<&TlsRoots>,
    domain_name: Option<String>,
    spki_pin: Option<[u8; 32]>,
    timeout: Duration,
) -> Result<Endpoint, IndexerError> {
    if endpoint.uri().scheme_str() != Some("https") {
        if roots.is_some() || domain_name.is_some() || spki_pin.is_some() {
            return Err(IndexerError::invalid_request(
                "TLS options require an https:// endpoint",
            ));
        }
        return Ok(endpoint);
    }

    let mut tls = ClientTlsConfig::new().timeout(timeout);
    if let Some(domain_name) = domain_name {
        let origin = tls_origin(endpoint.uri(), &domain_name)?;
        endpoint = endpoint.origin(origin);
        tls = tls.domain_name(domain_name);
    }
    let roots = roots.map(TlsRoots::trust_store).transpose()?;
    let configured = if let Some(sha256) = spki_pin {
        let roots = roots.unwrap_or_else(|| {
            let mut roots = RootCertStore::empty();
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            roots
        });
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
        let webpki = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|_| IndexerError::invalid_request("no usable TLS trust roots"))?;
        endpoint.tls_config_with_verifier(tls, Arc::new(SpkiPinVerifier { webpki, sha256 }))
    } else {
        endpoint.tls_config(match roots {
            Some(roots) => tls.trust_anchors(roots.roots),
            None => tls.with_native_roots(),
        })
    };
    configured.map_err(|_| IndexerError::invalid_request("invalid endpoint TLS configuration"))
}

fn tls_origin(dialed: &Uri, domain_name: &str) -> Result<Uri, IndexerError> {
    let invalid_name = || IndexerError::invalid_request("invalid TLS domain name");
    ServerName::try_from(domain_name).map_err(|_| invalid_name())?;
    let host = if domain_name.parse::<Ipv6Addr>().is_ok() {
        format!("[{domain_name}]")
    } else {
        domain_name.to_owned()
    };
    let authority = match dialed.port_u16() {
        Some(port) if port != 443 => format!("{host}:{port}"),
        Some(_) | None => host,
    };
    Uri::builder()
        .scheme("https")
        .authority(authority)
        .path_and_query("/")
        .build()
        .map_err(|_| invalid_name())
}

#[derive(Debug)]
struct SpkiPinVerifier {
    webpki: Arc<WebPkiServerVerifier>,
    sha256: [u8; 32],
}

impl ServerCertVerifier for SpkiPinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let verified = self.webpki.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        let spki = ParsedCertificate::try_from(end_entity)?.subject_public_key_info();
        let observed: [u8; 32] = Sha256::digest(spki.as_ref()).into();
        if !bool::from(observed.ct_eq(&self.sha256)) {
            return Err(CertificateError::ApplicationVerificationFailure.into());
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.webpki.verify_tls12_signature(message, cert, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.webpki.verify_tls13_signature(message, cert, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.webpki.supported_verify_schemes()
    }
}
