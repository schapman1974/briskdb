use std::process::Command;

fn assert_node_success(arguments: &[&str]) {
    let output = Command::new("node")
        .args(arguments)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("the supported CI and admin-browser development environment provides Node.js");

    assert!(
        output.status.success(),
        "node {} failed\nstdout:\n{}\nstderr:\n{}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn embedded_admin_browser_scripts_are_valid_and_logic_tests_pass() {
    assert_node_success(&["--check", "src/protocol/http/admin/logic.js"]);
    assert_node_success(&["--check", "src/protocol/http/admin/app.js"]);
    assert_node_success(&["--test", "tests/admin_browser_logic.test.js"]);
}
