// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Real-cluster harness: unique namespace, mandatory endpoint, cleanup even when
//! a contract panics. Only that namespace is destroyed; shared cluster data stays.
#[cfg(feature = "client")]
pub async fn real_contract<F, Fut>(body: F)
where
    F: FnOnce(extenddb_storage_tikv::TikvEngine) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    use extenddb_storage::bootstrapper::Bootstrapper;
    use extenddb_storage_tikv::{TikvEngine, bootstrap::TikvBootstrapper, config::TikvConfig};
    use futures::FutureExt;
    let mut config = TikvConfig::default();
    config.pd_endpoints = std::env::var("TIKV_PD_ENDPOINTS")
        .expect("set TIKV_PD_ENDPOINTS to a dedicated test cluster")
        .split(',')
        .map(str::to_owned)
        .collect();
    config.namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
    let config = config.validated().unwrap();
    let e = TikvEngine::new(
        config.connect().await.unwrap(),
        &config.namespace,
        "us-east-1",
    )
    .unwrap();
    let cleanup = TikvBootstrapper::new(e.clone(), config.clone());
    let result = std::panic::AssertUnwindSafe(body(e)).catch_unwind().await;
    cleanup
        .drop_databases(&config.namespace)
        .await
        .expect("test namespace cleanup");
    if let Err(panic) = result {
        std::panic::resume_unwind(panic)
    }
}
