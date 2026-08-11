pub mod core;
pub mod import;
pub mod protocol;
pub mod server;
pub mod sql;
pub mod storage;

mod sqlite_error;

// Preserve the original public module path while frontends migrate to the
// explicit protocol namespace.
pub use protocol::http as api;
