// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scheduling only; maintenance semantics live in explicit engine step methods.
//! Cancellation stops new ticks and shutdown joins an in-flight bounded tick.
use crate::TikvEngine;
use extenddb_storage::{
    DataEngine, MetadataEngine, StreamEngine,
    hooks::{ServerRuntimeHooks, WorkerContext, sleep_or_shutdown},
};
use std::{sync::Arc, time::Duration};
pub(crate) struct TikvRuntime {
    pub engine: Arc<TikvEngine>,
}
#[async_trait::async_trait]
impl ServerRuntimeHooks for TikvRuntime {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        let engine = self.engine.clone();
        let shutdown = ctx.shutdown.clone();
        let retention_engine = self.engine.clone();
        let retention_shutdown = ctx.shutdown.clone();
        let retention = tokio::spawn(async move {
            while sleep_or_shutdown(&retention_shutdown, Duration::from_secs(60)).await {
                if let Err(err) = retention_engine.pitr_step().await {
                    tracing::error!(%err,"TiKV PITR retention renewal failed");
                }
            }
        });
        vec![
            retention,
            tokio::spawn(async move {
                let mut tick = 0u64;
                while sleep_or_shutdown(&shutdown, Duration::from_secs(1)).await {
                    if let Err(err) = engine.lifecycle_step().await {
                        tracing::error!(%err,"TiKV maintenance failed; next tick will retry");
                    }
                    tick += 1;
                    if tick.is_multiple_of(5)
                        && let Err(err) = engine.ttl_step().await
                    {
                        tracing::error!(%err,"TiKV TTL cleanup failed");
                    }
                    if tick.is_multiple_of(60) {
                        if let Err(err) = engine.cleanup_expired_idempotency_tokens(600).await {
                            tracing::warn!(%err,"TiKV idempotency cleanup failed");
                        }
                        if let Err(err) = engine.cleanup_expired_stream_records(24).await {
                            tracing::warn!(%err,"TiKV stream cleanup failed");
                        }
                        match engine.all_active_tables().await {
                            Ok(tables) => {
                                for (a, n) in tables {
                                    if shutdown.is_cancelled() {
                                        return;
                                    }
                                    if let Err(err) = engine.refresh_table_size(&a, &n).await {
                                        tracing::warn!(%err,"TiKV table statistics refresh failed");
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::warn!(%err,"TiKV table statistics enumeration failed")
                            }
                        }
                    }
                }
            }),
        ]
    }
    fn backend_info(&self) -> Option<String> {
        Some(format!("storage=tikv namespace={}", self.engine.namespace))
    }
}
