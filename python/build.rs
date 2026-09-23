fn main() {
    println!("cargo:rerun-if-changed=src/remote_sqlite.c");
    // Headers only: the addon must dispatch every host operation through the
    // load-extension API, never through BriskDB's bundled SQLite symbols.
    cc::Build::new()
        .file("src/remote_sqlite.c")
        .include(std::env::var("DEP_SQLITE3_INCLUDE").expect("bundled SQLite headers"))
        .std("c11")
        .warnings_into_errors(true)
        .compile("briskdb_remote_sqlite");
}
