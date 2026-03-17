//! Resumable download logic.
//!
//! The `download_bytes_resumable` functions in `barter-data` (Binance and
//! Hyperliquid bulk modules) use HTTP Range resume with retry. They remain
//! in `barter-data` for now because they are tightly coupled to the partial
//! buffer state (`Arc<Mutex<Vec<u8>>>`). A future step will unify them here
//! and eliminate the `Arc<Mutex>` by owning the buffer directly.
