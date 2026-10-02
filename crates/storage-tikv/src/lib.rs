// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional TiKV storage for ExtendDB.
//!
//! The dependency direction is `engine -> kv -> transport`: DynamoDB modules
//! use the small [`kv`] contract, never client-rust types. [`codec`] owns the
//! versioned on-disk key format. This lets the same business logic run against
//! TiKV and a deterministic transactional reference store in contract tests.
//! See the crate README for the module contracts and operational limits.

#[cfg(feature = "client")]
pub mod backend;
pub mod backup;
pub mod bootstrap;
#[cfg(feature = "client")]
mod runtime;
mod staging;
#[cfg(feature = "client")]
pub use backend::backend;
pub mod catalog;
pub mod codec;
pub mod config;
pub mod data;
pub mod engine;
pub mod index;
pub mod kv;
pub mod maintenance;
pub mod metadata;
pub mod query;
pub mod stream;
pub mod table;
pub mod ttl;
pub mod vector;
pub use engine::TikvEngine;
