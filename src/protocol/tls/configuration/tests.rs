use super::*;
use std::fs;

#[test]
fn reads_zero_and_exact_limits_but_never_accepts_one_extra_byte() {
    let path = Path::new("configuration");
    for maximum in [0, 1, 16, 1026] {
        let exact = vec![b'x'; maximum as usize];
        assert_eq!(
            read_limited(exact.as_slice(), path, "Test", maximum)
                .unwrap()
                .as_slice(),
            exact
        );
        let oversized = vec![b'x'; maximum as usize + 1];
        let error = read_limited(oversized.as_slice(), path, "Test", maximum)
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(&format!("{maximum}-byte limit")));
    }
    assert_eq!(
        read_limited(io::empty(), path, "Test", u64::MAX)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

struct EndlessReader {
    read_bytes: usize,
    interrupted: bool,
}

impl Read for EndlessReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if !self.interrupted {
            self.interrupted = true;
            return Err(io::ErrorKind::Interrupted.into());
        }
        // Short reads force the accumulation path without ever reaching EOF.
        buffer[0] = b'x';
        self.read_bytes += 1;
        Ok(1)
    }
}

#[test]
fn interrupted_and_endless_short_reads_stop_after_one_overflow_byte() {
    let mut reader = EndlessReader {
        read_bytes: 0,
        interrupted: false,
    };
    let error = read_limited(&mut reader, Path::new("configuration"), "Test", 32)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(reader.read_bytes, 33);
}

#[test]
fn partial_read_errors_preserve_kind_and_do_not_include_read_contents() {
    struct Fail;
    impl Read for Fail {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "read denied",
            ))
        }
    }
    let secret = b"secret-sentinel-do-not-log";
    let reader = secret.as_slice().chain(Fail);
    let error = read_limited(reader, Path::new("configuration"), "Test secret", 64)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(
        error
            .to_string()
            .contains("failed to read Test secret configuration")
    );
    assert!(
        !error
            .to_string()
            .contains(std::str::from_utf8(secret).unwrap())
    );
}

#[test]
fn growth_after_descriptor_validation_is_still_bounded() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("growing");
    fs::write(&path, b"small").unwrap();
    let file = File::open(&path).unwrap();
    validate_opened_file(&file, &path, "Test", 16, false).unwrap();
    let writer = OpenOptions::new().write(true).open(&path).unwrap();
    writer.set_len(1024 * 1024).unwrap();
    let error = read_limited(file, &path, "Test", 16).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("16-byte limit"));
}

#[cfg(unix)]
fn mode(path: &Path, bits: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(bits)).unwrap();
}

#[cfg(unix)]
#[test]
fn replaced_path_cannot_change_bytes_read_from_the_validated_descriptor() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("secret");
    fs::write(&path, b"original").unwrap();
    mode(&path, 0o600);
    let opened = File::open(&path).unwrap();
    fs::rename(&path, root.path().join("previous")).unwrap();
    fs::write(&path, b"substituted").unwrap();
    mode(&path, 0o644);
    let bytes = read_opened_file(opened, &path, "Test secret", 32, true).unwrap();
    assert_eq!(bytes.as_slice(), b"original");
    assert_eq!(
        read_configuration_file(&path, "Test secret", 32, true)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[cfg(unix)]
#[test]
fn safe_replacement_path_cannot_hide_permissions_of_the_opened_descriptor() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("secret");
    fs::write(&path, b"public-readable").unwrap();
    mode(&path, 0o644);
    let opened = File::open(&path).unwrap();
    fs::rename(&path, root.path().join("previous")).unwrap();
    fs::write(&path, b"private").unwrap();
    mode(&path, 0o600);
    assert_eq!(
        read_opened_file(opened, &path, "Test secret", 32, true)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        read_configuration_file(&path, "Test secret", 32, true)
            .unwrap()
            .as_slice(),
        b"private"
    );
}

#[cfg(unix)]
#[test]
fn symlinks_remain_supported_but_target_permissions_are_required() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("secret");
    let link = root.path().join("current");
    fs::write(&target, b"secret").unwrap();
    mode(&target, 0o600);
    symlink(&target, &link).unwrap();
    assert_eq!(
        read_configuration_file(&link, "Test secret", 16, true)
            .unwrap()
            .as_slice(),
        b"secret"
    );
    mode(&target, 0o644);
    assert_eq!(
        read_configuration_file(&link, "Test secret", 16, true)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[cfg(unix)]
#[test]
fn fifo_without_a_writer_is_rejected_without_blocking_open() {
    use std::{ffi::CString, os::unix::ffi::OsStrExt, sync::mpsc, time::Duration};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("fifo");
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: name is a valid, NUL-terminated path owned for the whole call.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let (sender, receiver) = mpsc::channel();
    let task = std::thread::spawn(move || {
        let kind = read_configuration_file(&path, "Test secret", 16, true)
            .err()
            .unwrap()
            .kind();
        let _ = sender.send(kind);
    });
    assert_eq!(
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("FIFO open blocked without a writer"),
        io::ErrorKind::InvalidInput
    );
    task.join().unwrap();
}
