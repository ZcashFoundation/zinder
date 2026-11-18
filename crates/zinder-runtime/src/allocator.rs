//! Process allocator that every Zinder service binary installs.
//!
//! On Linux this is jemalloc with unprefixed symbols. Linking this crate makes
//! jemalloc the C library `malloc` that `RocksDB` allocates through, and the
//! global allocator declaration sends Rust allocations to jemalloc's sized
//! allocation functions. Glibc malloc makes
//! concurrent threads share a few arena locks, and a deployment that caps the
//! arena count serializes Zinder's parallel storage reads on them. See
//! [ADR-0005](../../../docs/adrs/0005-bounded-process-memory.md).

/// Global allocator for a Zinder service binary.
///
/// Each binary entry point installs it:
///
/// ```ignore
/// #[global_allocator]
/// static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;
/// ```
#[cfg(target_os = "linux")]
pub use tikv_jemallocator::Jemalloc as RuntimeAllocator;

/// Global allocator for a Zinder service binary.
///
/// Platforms other than Linux keep the system allocator.
#[cfg(not(target_os = "linux"))]
pub use std::alloc::System as RuntimeAllocator;
