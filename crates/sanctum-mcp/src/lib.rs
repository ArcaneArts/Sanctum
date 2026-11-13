#[cfg(feature = "bridge")]
mod bridge;
#[cfg(feature = "http")]
mod filter;
#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
mod tools;

#[cfg(feature = "bridge")]
pub use bridge::run_stdio_bridge;
#[cfg(feature = "http")]
pub use http::{hosted_router, router};

#[cfg(any(feature = "bridge", feature = "http"))]
const INSTRUCTIONS: &str = "Search Sanctum collections and cite results by tenant, collection, source ID, revision ID, and byte range.";
