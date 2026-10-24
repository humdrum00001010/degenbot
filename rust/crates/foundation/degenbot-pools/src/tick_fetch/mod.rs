//! Fetch-callback seams for sparse tick-map reads.
//!
//! `TickWordFetcher` serves already registered sparse pools when simulation
//! encounters an uncached bitmap word.

pub mod miss;

pub use miss::{FetchTickWordError, FetchedTickWord, TickWordFetcher};
