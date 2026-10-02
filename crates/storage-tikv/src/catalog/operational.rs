// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings, administrators, historical metrics and login throttling.
//! Each setting/admin is an independent document; append-only operational rows
//! use unique keys. Clock and transaction factory are injected through the engine.
//! Password work runs outside the async executor and transaction retry closures.
use super::*;
use extenddb_storage::{
    CatalogStore,
    diagnostics::{DiagError, DiagResult, DiagnosticsStore},
};
use futures::future::BoxFuture;

#[derive(Clone, Serialize, Deserialize)]
struct Admin {
    name: String,
    hash: String,
    created: OffsetDateTime,
}
#[derive(Clone, Serialize, Deserialize)]
struct Attempt {
    principal: String,
    ip: Option<String>,
    at: i64,
}
#[derive(Clone, Serialize, Deserialize)]
struct Metric {
    bucket: OffsetDateTime,
    metric: String,
    table_name: Option<String>,
    index_name: Option<String>,
    operation: Option<String>,
    sum: f64,
    count: i64,
    min: f64,
    max: f64,
}
impl From<&MetricsRow> for Metric {
    fn from(r: &MetricsRow) -> Self {
        Self {
            bucket: r.bucket,
            metric: r.metric.clone(),
            table_name: r.table_name.clone(),
            index_name: r.index_name.clone(),
            operation: r.operation.clone(),
            sum: r.sum,
            count: r.count,
            min: r.min,
            max: r.max,
        }
    }
}
impl From<Metric> for MetricsRow {
    fn from(r: Metric) -> Self {
        Self {
            bucket: r.bucket,
            metric: r.metric,
            table_name: r.table_name,
            index_name: r.index_name,
            operation: r.operation,
            sum: r.sum,
            count: r.count,
            min: r.min,
            max: r.max,
        }
    }
}
pub(crate) async fn verify(password: String, hash: String) -> OpResult<bool> {
    tokio::task::spawn_blocking(move || bcrypt::verify(password, &hash))
        .await
        .map_err(|e| OpError::Internal(e.to_string()))?
        .map_err(|_| OpError::Internal("Invalid password hash".into()))
}
impl SettingsStore for TikvCatalog {
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let e = self.engine.clone();
        let k = e.key(&["setting", key]);
        Box::pin(async move {
            e.db.run(move |tx| {
                let k = k.clone();
                Box::pin(async move { kv::get(tx, k).await })
            })
            .await
            .map_err(op_error)
        })
    }
    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>> {
        let e = self.engine.clone();
        let k = e.key(&["setting", key]);
        let v = value.to_owned();
        Box::pin(async move {
            e.db.run(move |tx| {
                let k = k.clone();
                let v = v.clone();
                Box::pin(async move { kv::put(tx, k, &v).await })
            })
            .await
            .map_err(op_error)
        })
    }
    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let e = self.engine.clone();
        let p = e.key(&["setting"]);
        Box::pin(async move {
            e.db.run(move |tx| {
                let p = p.clone();
                Box::pin(async move {
                    let mut out = vec![];
                    for (k, v) in kv::all(tx, p.clone()).await? {
                        let name = crate::codec::decode_component(&k[p.len()..])?;
                        out.push((
                            String::from_utf8(name).map_err(|_| {
                                StorageError::Internal("Invalid setting name".into())
                            })?,
                            kv::decode(&v)?,
                        ));
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(op_error)
        })
    }
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(|k| k.as_str().to_owned())
    }
}
impl CatalogStore for TikvCatalog {
    fn cached_encryption_key(&self) -> Option<String> {
        SettingsStore::cached_encryption_key(self)
    }
}
impl AdminStore for TikvCatalog {
    fn create_admin(&self, name: &str, hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let e = self.engine.clone();
        let a = Admin {
            name: name.into(),
            hash: hash.into(),
            created: self.now(),
        };
        Box::pin(async move {
            e.db.clone()
                .run(move |tx| {
                    let e = e.clone();
                    let a = a.clone();
                    Box::pin(async move {
                        let k = e.key(&["admin", &a.name]);
                        if kv::get::<Admin>(tx, k.clone()).await?.is_some() {
                            return Ok(Err(duplicate("Admin")));
                        }
                        kv::put(tx, k, &a).await?;
                        Ok(Ok(()))
                    })
                })
                .await
                .map_err(op_error)?
        })
    }
    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
        Box::pin(async move {
            Ok(self
                .documents::<Admin>("admin")
                .await?
                .into_iter()
                .map(|a| AdminEntry {
                    admin_name: a.name,
                    created_at: a.created,
                })
                .collect())
        })
    }
    fn delete_admin(&self, name: &str) -> BoxFuture<'_, OpResult<()>> {
        let k = self.engine.key(&["admin", name]);
        Box::pin(async move {
            self.engine
                .db
                .run(move |tx| {
                    let k = k.clone();
                    Box::pin(async move {
                        if kv::get::<Admin>(tx, k.clone()).await?.is_none() {
                            return Ok(Err(missing("Admin")));
                        }
                        kv::delete(tx, k).await?;
                        Ok(Ok(()))
                    })
                })
                .await
                .map_err(op_error)?
        })
    }
    fn change_admin_password(&self, name: &str, hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let k = self.engine.key(&["admin", name]);
        let h = hash.to_owned();
        Box::pin(async move {
            self.engine
                .db
                .run(move |tx| {
                    let k = k.clone();
                    let h = h.clone();
                    Box::pin(async move {
                        let Some(mut a) = kv::get::<Admin>(tx, k.clone()).await? else {
                            return Ok(Err(missing("Admin")));
                        };
                        a.hash = h;
                        kv::put(tx, k, &a).await?;
                        Ok(Ok(()))
                    })
                })
                .await
                .map_err(op_error)?
        })
    }
    fn verify_admin_password(
        &self,
        name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<Option<bool>>> {
        let k = self.engine.key(&["admin", name]);
        let p = password.to_owned();
        Box::pin(async move {
            let a = self
                .engine
                .db
                .run(move |tx| {
                    let k = k.clone();
                    Box::pin(async move { kv::get::<Admin>(tx, k).await })
                })
                .await
                .map_err(op_error)?;
            match a {
                Some(a) => Ok(Some(verify(p, a.hash).await?)),
                None => Ok(None),
            }
        })
    }
}
impl TikvCatalog {
    async fn documents<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        kind: &str,
    ) -> OpResult<Vec<T>> {
        let p = self.engine.key(&[kind]);
        self.engine
            .db
            .run(move |tx| {
                let p = p.clone();
                Box::pin(async move {
                    kv::all(tx, p)
                        .await?
                        .into_iter()
                        .map(|(_, v)| kv::decode(&v))
                        .collect()
                })
            })
            .await
            .map_err(op_error)
    }
    async fn prune<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        kind: &str,
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> OpResult<()> {
        self.engine
            .prune_step::<T>(kind, f)
            .await
            .map(|_| ())
            .map_err(op_error)
    }
}
impl MetricsStore for TikvCatalog {
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        let e = self.engine.clone();
        let rows: Vec<_> = rows
            .iter()
            .map(|r| {
                (
                    e.key(&[
                        "metric",
                        &r.bucket.unix_timestamp_nanos().to_string(),
                        &r.metric,
                        r.table_name.as_deref().unwrap_or(""),
                        r.index_name.as_deref().unwrap_or(""),
                        r.operation.as_deref().unwrap_or(""),
                    ]),
                    Metric::from(r),
                )
            })
            .collect();
        Box::pin(async move {
            e.db.run(move |tx| {
                let rows = rows.clone();
                Box::pin(async move {
                    for (k, mut v) in rows {
                        if let Some(old) = kv::get::<Metric>(tx, k.clone()).await? {
                            v.sum += old.sum;
                            v.count += old.count;
                            v.min = v.min.min(old.min);
                            v.max = v.max.max(old.max);
                        }
                        kv::put(tx, k, &v).await?;
                    }
                    Ok(())
                })
            })
            .await
            .map_err(op_error)
        })
    }
    fn query_metrics(
        &self,
        start: OffsetDateTime,
        end: OffsetDateTime,
        table_name: Option<&str>,
        metric: Option<&str>,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>> {
        let table = table_name.map(str::to_owned);
        let metric = metric.map(str::to_owned);
        Box::pin(async move {
            let mut out: Vec<MetricsRow> = self
                .documents::<Metric>("metric")
                .await?
                .into_iter()
                .filter(|r| {
                    r.bucket >= start
                        && r.bucket <= end
                        && table
                            .as_ref()
                            .is_none_or(|t| r.table_name.as_ref() == Some(t))
                        && metric.as_ref().is_none_or(|m| r.metric == *m)
                })
                .map(Into::into)
                .collect();
            out.sort_by_key(|r| r.bucket);
            Ok(out)
        })
    }
    fn prune_metrics(&self, retention: std::time::Duration) -> BoxFuture<'_, OpResult<()>> {
        let cutoff =
            self.now() - time::Duration::try_from(retention).unwrap_or(time::Duration::MAX);
        Box::pin(async move {
            self.prune::<Metric>("metric", move |r| r.bucket < cutoff)
                .await
        })
    }
}
impl RateLimitStore for TikvCatalog {
    fn count_principal_failures(
        &self,
        principal: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let p = principal.to_owned();
        let cutoff = self
            .engine
            .clock
            .now_ms()
            .saturating_sub(window_seconds.saturating_mul(1000));
        Box::pin(async move {
            Ok(self
                .documents::<Attempt>("attempt")
                .await?
                .iter()
                .filter(|a| a.at >= cutoff && a.principal == p)
                .count() as i64)
        })
    }
    fn count_ip_failures(
        &self,
        source_ip: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let ip = source_ip.to_owned();
        let cutoff = self
            .engine
            .clock
            .now_ms()
            .saturating_sub(window_seconds.saturating_mul(1000));
        Box::pin(async move {
            Ok(self
                .documents::<Attempt>("attempt")
                .await?
                .iter()
                .filter(|a| a.at >= cutoff && a.ip.as_ref() == Some(&ip))
                .count() as i64)
        })
    }
    fn record_failed_login(&self, principal: &str, source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        let k = self
            .engine
            .key(&["attempt", &uuid::Uuid::new_v4().to_string()]);
        let a = Attempt {
            principal: principal.into(),
            ip: source_ip.map(str::to_owned),
            at: self.engine.clock.now_ms(),
        };
        Box::pin(async move {
            if let Err(err) = self
                .engine
                .db
                .run(move |tx| {
                    let k = k.clone();
                    let a = a.clone();
                    Box::pin(async move { kv::put(tx, k, &a).await })
                })
                .await
            {
                tracing::error!(%err,"TiKV login-attempt persistence failed");
            }
        })
    }
    fn cleanup_old_attempts(&self, max_age_seconds: i64) -> BoxFuture<'_, ()> {
        let cutoff = self
            .engine
            .clock
            .now_ms()
            .saturating_sub(max_age_seconds.saturating_mul(1000));
        Box::pin(async move {
            if let Err(err) = self
                .prune::<Attempt>("attempt", move |a| a.at < cutoff)
                .await
            {
                tracing::error!(?err, "TiKV login-attempt cleanup failed");
            }
        })
    }
}
impl DiagnosticsStore for TikvCatalog {
    fn count_tables(&self) -> BoxFuture<'_, DiagResult<i64>> {
        Box::pin(async move {
            self.engine
                .db
                .run(|tx| {
                    let e = self.engine.clone();
                    Box::pin(async move { e.tables(tx).await })
                })
                .await
                .map(|t| t.len() as i64)
                .map_err(|e| DiagError::QueryFailed(e.to_string()))
        })
    }
    fn count_indexes(&self) -> BoxFuture<'_, DiagResult<i64>> {
        Box::pin(async move {
            self.engine
                .db
                .run(|tx| {
                    let e = self.engine.clone();
                    Box::pin(async move { e.tables(tx).await })
                })
                .await
                .map(|t| t.iter().map(|t| t.indexes.len() as i64).sum())
                .map_err(|e| DiagError::QueryFailed(e.to_string()))
        })
    }
    fn test_data_database_connection(&self) -> BoxFuture<'_, DiagResult<String>> {
        Box::pin(async move {
            self.get_setting("catalog_version")
                .await
                .map_err(|e| DiagError::ConnectionFailed(format!("{e:?}")))?
                .ok_or_else(|| {
                    DiagError::QueryFailed("TiKV namespace is not initialized".into())
                })?;
            Ok(self.engine.namespace.to_string())
        })
    }
}
