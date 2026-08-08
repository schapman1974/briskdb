pub mod core;
pub mod protocol;
pub mod server;
pub mod sql;
pub mod storage;

// Preserve the original public module path while frontends migrate to the
// explicit protocol namespace.
pub use protocol::http as api;
