use super::*;
use std::{
    os::unix::fs::{PermissionsExt, symlink},
    process::{Command, Stdio},
};

fn target() -> (tempfile::TempDir, Target) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("briskdb-efs-test-local");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let target = Target::open(&root, "local", "fs-deadbeef").unwrap();
    (temp, target)
}

fn mount(options: &str) -> String {
    format!("30 20 0:70 / /mnt/efs rw - nfs4 127.0.0.1:/ rw,{options}")
}

#[test]
fn mount_gate_checks_descriptor_identity_and_exact_options() {
    let good = "vers=4.1,hard,proto=tcp,local_lock=none";
    let root = Path::new("/mnt/efs/briskdb-efs-test-run");
    assert!(validate_mountinfo(&mount(good), "30", root).is_ok());
    for bad in [
        "vers=4.0,hard,proto=tcp,local_lock=none",
        "vers=4.1,soft,proto=tcp,local_lock=none",
        "vers=4.1,hard,proto=tcp,local_lock=all",
        "vers=4.1,hard,proto=udp,local_lock=none",
        "vers=4.1,hard,proto=tcp",
        "vers=4.1,hard,proto=tcp,local_lock=none,softreval",
        "vers=4.1,hard,proto=tcp,local_lock=none,nolock",
        "vers=4.1,vers=4.0,hard,proto=tcp,local_lock=none",
        "vers=4.1,hard,proto=tcp,proto=udp,local_lock=none",
    ] {
        assert!(
            validate_mountinfo(&mount(bad), "30", root).is_err(),
            "{bad}"
        );
    }
    assert!(validate_mountinfo(&mount(good), "31", root).is_err());
    assert!(validate_mountinfo(&mount(good), "30", Path::new("/mnt/efs")).is_err());
    assert!(validate_mountinfo(&mount(good), "30", Path::new("/mnt/efs-other/test")).is_err());
    let overlaid = format!("{}\n31 30 0:80 / /mnt/efs rw - tmpfs tmpfs rw", mount(good));
    assert!(validate_mountinfo(&overlaid, "31", root).is_err());
    assert!(validate_mountinfo(&format!("{}\n{}", mount(good), mount(good)), "30", root).is_err());
    let escaped = mount(good).replace("/mnt/efs", "/mnt/my\\040efs");
    assert!(validate_mountinfo(&escaped, "30", Path::new("/mnt/my efs/test")).is_ok());
}

#[test]
fn init_never_adopts_data_and_missing_or_wrong_marker_never_creates_a_lock() {
    let (_temp, target) = target();
    assert!(target.acquire(false, &control(10, true).unwrap()).is_err());
    assert!(!target.root.join(LOCK).exists());
    fs::write(target.root.join("existing-data"), b"retain").unwrap();
    assert!(target.acquire(true, &control(10, true).unwrap()).is_err());
    assert!(!target.root.join(LOCK).exists());
    assert_eq!(
        fs::read(target.root.join("existing-data")).unwrap(),
        b"retain"
    );
    fs::write(target.root.join(MARKER), b"wrong").unwrap();
    assert!(target.acquire(false, &control(10, true).unwrap()).is_err());
    assert!(!target.root.join(LOCK).exists());
}

#[test]
fn retained_lock_excludes_peers_and_unrelated_close_does_not_release_ownership() {
    let (_temp, target) = target();
    let guard = target.acquire(true, &control(10, true).unwrap()).unwrap();
    assert_eq!(
        target
            .acquire(false, &control(10, true).unwrap())
            .err()
            .unwrap()
            .kind(),
        EngineErrorKind::Busy
    );
    drop(File::open(target.root.join(LOCK)).unwrap());
    run(&target, "probe", "B", 2, 10, 1).unwrap();
    let started = Instant::now();
    assert_eq!(
        target
            .acquire(false, &control(20, false).unwrap())
            .err()
            .unwrap()
            .kind(),
        EngineErrorKind::Busy
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(guard);
    assert!(run(&target, "probe", "B", 2, 10, 1).is_err());
    assert!(target.root.join(LOCK).is_file());
    target.acquire(false, &control(10, true).unwrap()).unwrap();
}

#[test]
fn changed_lock_identity_and_symlinks_fail_closed() {
    let (_temp, target) = target();
    let guard = target.acquire(true, &control(10, true).unwrap()).unwrap();
    fs::rename(target.root.join(LOCK), target.root.join("old-lock")).unwrap();
    fs::write(target.root.join(LOCK), b"").unwrap();
    assert!(guard.check().is_err());
    assert!(
        target.acquire(false, &control(10, true).unwrap()).is_err(),
        "new openers must reject a replaced retained lock too"
    );
    drop(guard);
    fs::remove_file(target.root.join(LOCK)).unwrap();
    symlink(target.root.join("old-lock"), target.root.join(LOCK)).unwrap();
    assert!(target.acquire(false, &control(10, true).unwrap()).is_err());
}

#[test]
fn removed_manifest_and_replaced_root_are_not_adopted() {
    let (_temp, target) = target();
    run(&target, "init", "A", 1, 5000, 1).unwrap();
    fs::rename(
        target.root.join("manifest.sqlite"),
        target.root.join("retained-manifest.sqlite"),
    )
    .unwrap();
    assert!(run(&target, "write", "A", 1, 5000, 1).is_err());
    assert!(!target.root.join("manifest.sqlite").exists());
    let retained = target.root.with_file_name("retained-root");
    fs::rename(&target.root, &retained).unwrap();
    fs::create_dir(&target.root).unwrap();
    fs::set_permissions(&target.root, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(target.check_directory().is_err());
    assert!(target.acquire(true, &control(10, true).unwrap()).is_err());
    assert_eq!(fs::read_dir(&target.root).unwrap().count(), 0);
}

#[test]
fn local_mount_is_rejected_without_creating_files() {
    let (_temp, target) = target();
    assert!(inspect_mount(&target.directory, &target.root).is_err());
    assert_eq!(fs::read_dir(&target.root).unwrap().count(), 0);
}

#[test]
fn local_runner_preserves_sql_and_documents_across_owner_scopes() {
    let (_temp, target) = target();
    run(&target, "init", "A", 3, 5000, 1).unwrap();
    run(&target, "write", "A", 3, 5000, 1).unwrap();
    run(&target, "write", "B", 3, 5000, 1).unwrap();
    run(&target, "verify", "A", 3, 5000, 1).unwrap();
    assert!(
        run(&target, "write", "A", 3, 5000, 1).is_err(),
        "uncertain writes must not be blindly replayed"
    );
    run(&target, "verify", "B", 3, 5000, 1).unwrap();
}

#[test]
fn peer_process_probe() {
    let Ok(root) = std::env::var("BRISKDB_LOCAL_EFS_PROBE_ROOT") else {
        return;
    };
    let target = Target::open(Path::new(&root), "local", "fs-deadbeef").unwrap();
    run(&target, "probe", "B", 1, 10, 1).unwrap();
}

#[test]
fn independent_local_process_observes_ownership() {
    let (_temp, target) = target();
    let guard = target.acquire(true, &control(10, true).unwrap()).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::profile::efs_qualification::tests::peer_process_probe",
            "--nocapture",
        ])
        .env("BRISKDB_LOCAL_EFS_PROBE_ROOT", &target.root)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    drop(guard);
}

#[test]
fn local_crash_child() {
    let Ok(root) = std::env::var("BRISKDB_LOCAL_EFS_CRASH_ROOT") else {
        return;
    };
    let target = Target::open(Path::new(&root), "local", "fs-deadbeef").unwrap();
    run(
        &target,
        &std::env::var("BRISKDB_LOCAL_EFS_CRASH_ACTION").unwrap(),
        "A",
        2,
        5000,
        1,
    )
    .unwrap();
    panic!("crash boundary did not terminate the process");
}

#[test]
fn process_exits_release_locks_and_recover_exact_commit_outcomes() {
    for committed in [false, true] {
        let (_temp, target) = target();
        run(&target, "init", "A", 2, 5000, 1).unwrap();
        run(&target, "write", "A", 2, 5000, 1).unwrap();
        run(&target, "write", "B", 2, 5000, 1).unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::profile::efs_qualification::tests::local_crash_child",
                "--nocapture",
            ])
            .env("BRISKDB_LOCAL_EFS_CRASH_ROOT", &target.root)
            .env(
                "BRISKDB_LOCAL_EFS_CRASH_ACTION",
                if committed {
                    "crash-after-commit"
                } else {
                    "crash-before-commit"
                },
            )
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(73), "{output:?}");
        run(
            &target,
            if committed {
                "verify-committed"
            } else {
                "verify"
            },
            "B",
            2,
            5000,
            1,
        )
        .unwrap();
    }
}
