//! Network protocol adapters.

pub mod error;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "postgres")]
pub mod postgres;
