use super::*;
use std::{fs, path::PathBuf};

struct IdentityFiles {
    _root: tempfile::TempDir,
    certificate: PathBuf,
    key: PathBuf,
}

impl IdentityFiles {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let certificate = root.path().join("server.crt");
        let key = root.path().join("server.key");
        fs::write(
            &certificate,
            include_bytes!("../../../tests/fixtures/postgres-tls/server.crt"),
        )
        .unwrap();
        fs::write(
            &key,
            include_bytes!("../../../tests/fixtures/postgres-tls/server.key"),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        }
        Self {
            _root: root,
            certificate,
            key,
        }
    }

    fn load(&self, alpn: &[&[u8]]) -> io::Result<LoadedTlsIdentity> {
        load_server_identity(&self.certificate, &self.key, "Test connector", alpn)
    }
}

#[test]
fn each_connector_selects_alpn_and_retains_exact_channel_binding_bytes() {
    let files = IdentityFiles::new();
    let postgres = files.load(&[b"postgresql"]).unwrap();
    let plain = files.load(&[]).unwrap();
    let other = files.load(&[b"other/1", b"other/2"]).unwrap();
    assert_eq!(postgres.config.alpn_protocols, [b"postgresql".to_vec()]);
    assert!(plain.config.alpn_protocols.is_empty());
    assert_eq!(
        other.config.alpn_protocols,
        [b"other/1".to_vec(), b"other/2".to_vec()]
    );
    assert!(!Arc::ptr_eq(&postgres.config, &plain.config));
    let expected = fs::read(&files.certificate).unwrap();
    for identity in [postgres, plain, other] {
        assert_eq!(identity.certificate_pem.as_ref(), expected);
    }
}

fn handshake(
    identity: &LoadedTlsIdentity,
    trust_fixture: bool,
    server_name: &'static str,
    alpn: &[&[u8]],
) -> Result<Option<Vec<u8>>, rustls::Error> {
    let mut roots = rustls::RootCertStore::empty();
    if trust_fixture {
        for certificate in rustls_pemfile::certs(&mut identity.certificate_pem.as_ref()) {
            roots.add(certificate.unwrap()).unwrap();
        }
    }
    let mut client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = alpn.iter().map(|value| value.to_vec()).collect();
    let mut client = rustls::ClientConnection::new(
        Arc::new(client_config),
        rustls::pki_types::ServerName::try_from(server_name).unwrap(),
    )?;
    let mut server = rustls::ServerConnection::new(identity.config.clone())?;
    for _ in 0..20 {
        let mut client_bytes = Vec::new();
        client.write_tls(&mut client_bytes).unwrap();
        server.read_tls(&mut client_bytes.as_slice()).unwrap();
        server.process_new_packets()?;
        let mut server_bytes = Vec::new();
        server.write_tls(&mut server_bytes).unwrap();
        client.read_tls(&mut server_bytes.as_slice()).unwrap();
        client.process_new_packets()?;
        if !client.is_handshaking() && !server.is_handshaking() {
            assert_eq!(client.alpn_protocol(), server.alpn_protocol());
            return Ok(client.alpn_protocol().map(<[u8]>::to_vec));
        }
    }
    panic!("in-memory TLS handshake did not complete");
}

#[test]
fn shared_identity_handshakes_without_postgres_and_preserves_peer_validation() {
    let files = IdentityFiles::new();
    let identity = files.load(&[b"test/1"]).unwrap();
    assert_eq!(
        handshake(&identity, true, "localhost", &[b"test/1"]).unwrap(),
        Some(b"test/1".to_vec())
    );
    assert!(matches!(
        handshake(&identity, false, "localhost", &[b"test/1"]),
        Err(rustls::Error::InvalidCertificate(_))
    ));
    assert!(matches!(
        handshake(&identity, true, "wrong.invalid", &[b"test/1"]),
        Err(rustls::Error::InvalidCertificate(_))
    ));
    assert!(matches!(
        handshake(&identity, true, "localhost", &[b"wrong/1"]),
        Err(rustls::Error::NoApplicationProtocol)
    ));
    let no_alpn = files.load(&[]).unwrap();
    assert_eq!(handshake(&no_alpn, true, "localhost", &[]).unwrap(), None);
}

#[test]
fn missing_and_non_regular_files_are_rejected_with_connector_context() {
    let files = IdentityFiles::new();
    for (certificate, key, label) in [
        (
            files._root.path().join("missing"),
            files.key.clone(),
            "certificate",
        ),
        (
            files.certificate.clone(),
            files._root.path().join("missing"),
            "private key",
        ),
        (
            files._root.path().to_path_buf(),
            files.key.clone(),
            "certificate",
        ),
        (
            files.certificate.clone(),
            files._root.path().to_path_buf(),
            "private key",
        ),
    ] {
        let error = load_server_identity(&certificate, &key, "Test connector", &[])
            .err()
            .unwrap();
        assert!(matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::InvalidInput | io::ErrorKind::PermissionDenied
        ));
        assert!(
            error
                .to_string()
                .contains(&format!("Test connector TLS {label}"))
        );
    }
}

#[test]
fn pem_file_size_limit_is_inclusive_and_applies_to_both_inputs() {
    for certificate in [true, false] {
        let files = IdentityFiles::new();
        let path = if certificate {
            &files.certificate
        } else {
            &files.key
        };
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_len(MAX_TLS_PEM_BYTES).unwrap();
        configuration::validate_opened_file(&file, path, "Test file", MAX_TLS_PEM_BYTES, false)
            .unwrap();
        file.set_len(MAX_TLS_PEM_BYTES + 1).unwrap();
        let error = files.load(&[]).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("1048576-byte limit"));
    }
}

#[test]
fn empty_pem_files_are_rejected() {
    for certificate in [true, false] {
        let files = IdentityFiles::new();
        fs::write(
            if certificate {
                &files.certificate
            } else {
                &files.key
            },
            [],
        )
        .unwrap();
        let error = files.load(&[]).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(if certificate {
            "contains no certificates"
        } else {
            "contains no supported private key"
        }));
    }
}

#[test]
fn malformed_pem_and_invalid_der_do_not_return_secret_contents() {
    const SECRET: &str = "not-a-real-secret-sentinel-do-not-log";
    for certificate in [true, false] {
        for invalid_base64 in [true, false] {
            let files = IdentityFiles::new();
            let label = if certificate {
                "CERTIFICATE"
            } else {
                "PRIVATE KEY"
            };
            let body = if invalid_base64 { SECRET } else { "AQID" };
            let pem = format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n");
            fs::write(
                if certificate {
                    &files.certificate
                } else {
                    &files.key
                },
                &pem,
            )
            .unwrap();
            let error = files.load(&[]).err().unwrap();
            assert!(error.to_string().contains("Test connector TLS"));
            assert!(!error.to_string().contains(SECRET));
            assert!(!error.to_string().contains(&pem));
        }
    }
}

#[cfg(unix)]
#[test]
fn private_keys_allow_owner_and_group_read_but_reject_other_access_and_group_write() {
    use std::os::unix::fs::PermissionsExt;
    let files = IdentityFiles::new();
    for mode in [0o400, 0o600, 0o640] {
        fs::set_permissions(&files.key, fs::Permissions::from_mode(mode)).unwrap();
        files.load(&[]).unwrap();
    }
    for mode in [0o644, 0o660, 0o601, 0o602, 0o610] {
        fs::set_permissions(&files.key, fs::Permissions::from_mode(mode)).unwrap();
        let error = files.load(&[]).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("Test connector TLS private key"));
    }
}

#[test]
fn contextual_errors_preserve_kind_without_changing_the_cause() {
    let error = contextual_io_error(
        io::Error::new(io::ErrorKind::PermissionDenied, "denied"),
        "identity load".into(),
    );
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "identity load: denied");
}
