// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Historical MVCC recovery with renewable PD retention barriers.
//!
//! Enabling pins history before publishing the setting. Ordinary writes need no
//! journal: TiKV's commit timestamps already order every item/schema transaction.
//! Retention keeps up to 35 days; a 24-hour PD lease tolerates worker downtime.
//! After a longer outage, reconciliation narrows the reported window to history
//! actually retained by PD. Recovery never substitutes the current snapshot.
use crate::{
    TikvEngine,
    backup::{restore::Writer, snapshot::Reader},
    catalog::Account,
    kv,
    table::Table,
};
use extenddb_core::types::*;
use extenddb_storage::{RestorePoint, error::StorageError};
use serde::{Deserialize, Serialize};
const DAY_MS: u64 = 86_400_000;
const WINDOW: u64 = (35 * DAY_MS) << 18;
const RETENTION_TTL: i64 = 86_400;
#[derive(Clone, Serialize, Deserialize)]
struct State {
    account: String,
    generation: String,
    table: String,
    service: String,
    start: u64,
    floor: u64,
    refreshed: u64,
    enabled: bool,
}
fn invalid(s: &str) -> StorageError {
    StorageError::Validation(s.into())
}
fn disabled() -> ContinuousBackupsDescription {
    ContinuousBackupsDescription {
        continuous_backups_status: "DISABLED".into(),
        point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
            point_in_time_recovery_status: "DISABLED".into(),
            earliest_restorable_date_time: None,
            latest_restorable_date_time: None,
        }),
    }
}
fn description(s: &State, now: u64) -> ContinuousBackupsDescription {
    // Public times have millisecond precision. Explicit requests select the end
    // of that physical millisecond, capped at the current TSO for the latest one.
    let earliest = (s.floor >> 18) as f64 / 1000.;
    ContinuousBackupsDescription {
        continuous_backups_status: "ENABLED".into(),
        point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
            point_in_time_recovery_status: "ENABLED".into(),
            earliest_restorable_date_time: Some(earliest),
            latest_restorable_date_time: Some((now >> 18) as f64 / 1000.),
        }),
    }
}
fn requested(point: RestorePoint, now: u64, floor: u64) -> Result<u64, StorageError> {
    let ts = match point {
        RestorePoint::Latest => now,
        RestorePoint::Timestamp(seconds) => {
            if !seconds.is_finite()
                || seconds < 0.
                || seconds * 1000. > ((i64::MAX as u64 >> 18) - 1) as f64
            {
                return Err(invalid("Invalid RestoreDateTime"));
            }
            let ms = (seconds * 1000.).floor() as u64;
            if ms > now >> 18 {
                return Err(invalid("RestoreDateTime is in the future"));
            }
            ((ms << 18) | ((1 << 18) - 1)).min(now)
        }
    };
    if ts < floor || ts > now {
        return Err(invalid(
            "RestoreDateTime is outside the available recovery window",
        ));
    }
    Ok(ts)
}
impl TikvEngine {
    async fn pitr_source(
        &self,
        a: &str,
        n: &str,
    ) -> Result<(Table, Account, Option<State>), StorageError> {
        self.db
            .run(|tx| {
                let e = self.clone();
                let a = a.to_owned();
                let n = n.to_owned();
                Box::pin(async move {
                    let t = e.table_by_name(tx, &a, &n).await?;
                    if t.description.table_status != TableStatus::Active {
                        return Err(StorageError::TableNotActive(n));
                    }
                    let owner = kv::get::<Account>(tx, e.key(&["account", &a]))
                        .await?
                        .ok_or_else(|| invalid("Account does not exist"))?;
                    let s = kv::get(tx, e.key(&["pitr", &t.description.table_id])).await?;
                    Ok((t, owner, s))
                })
            })
            .await
    }
    async fn retire_pitr(&self, s: &State) -> Result<(), StorageError> {
        self.db.retain(s.service.clone(), 0, 0).await?;
        self.db
            .run(|tx| {
                let e = self.clone();
                let s = s.clone();
                Box::pin(async move {
                    let k = e.key(&["pitr", &s.table]);
                    if kv::get::<State>(tx, k.clone())
                        .await?
                        .is_some_and(|v| v.service == s.service)
                    {
                        kv::delete(tx, k).await?;
                    }
                    Ok(())
                })
            })
            .await
    }
    async fn refresh_pitr(&self, mut s: State) -> Result<Option<State>, StorageError> {
        let alive = self
            .db
            .run(|tx| {
                let e = self.clone();
                let s = s.clone();
                Box::pin(async move {
                    let t = kv::get::<Table>(tx, e.key(&["table", &s.table])).await?;
                    let a = kv::get::<Account>(tx, e.key(&["account", &s.account])).await?;
                    Ok(s.enabled
                        && t.is_some_and(|t| t.description.table_status != TableStatus::Deleting)
                        && a.is_some_and(|a| a.generation == s.generation))
                })
            })
            .await?;
        if !alive {
            self.retire_pitr(&s).await?;
            return Ok(None);
        }
        let now = self.db.timestamp().await?;
        s.floor = s
            .floor
            .max(s.start)
            .max(now.saturating_sub(WINDOW))
            .max(self.db.gc_floor().await?.saturating_add(1));
        let minimum = self
            .db
            .retain(s.service.clone(), s.floor - 1, RETENTION_TTL)
            .await?;
        if minimum >= s.floor {
            s.floor = minimum
                .checked_add(1)
                .ok_or_else(|| invalid("Invalid retention floor"))?;
            if self
                .db
                .retain(s.service.clone(), s.floor - 1, RETENTION_TTL)
                .await?
                > s.floor - 1
            {
                return Err(invalid("Recovery retention moved concurrently; retry"));
            }
        }
        if s.floor > now {
            return Err(invalid(
                "No retained recovery window is currently available",
            ));
        }
        s.refreshed = now;
        let updated = self
            .db
            .run(|tx| {
                let e = self.clone();
                let s = s.clone();
                Box::pin(async move {
                    let k = e.key(&["pitr", &s.table]);
                    let Some(mut current) = kv::get::<State>(tx, k.clone()).await? else {
                        return Ok(None);
                    };
                    if current.service != s.service || !current.enabled {
                        return Ok(None);
                    }
                    current.floor = current.floor.max(s.floor);
                    current.refreshed = current.refreshed.max(s.refreshed);
                    kv::put(tx, k, &current).await?;
                    Ok(Some(current))
                })
            })
            .await?;
        if updated.is_none() {
            let _ = self.db.retain(s.service, 0, 0).await;
        }
        Ok(updated)
    }
    pub(crate) async fn describe_pitr(
        &self,
        a: &str,
        n: &str,
    ) -> Result<ContinuousBackupsDescription, StorageError> {
        let (_, _, s) = self.pitr_source(a, n).await?;
        let Some(s) = s.filter(|s| s.enabled) else {
            return Ok(disabled());
        };
        match self.refresh_pitr(s).await? {
            Some(s) => Ok(description(&s, self.db.timestamp().await?)),
            None => Ok(disabled()),
        }
    }
    pub(crate) async fn set_pitr(
        &self,
        a: &str,
        n: &str,
        enabled: bool,
    ) -> Result<ContinuousBackupsDescription, StorageError> {
        let (t, owner, previous) = self.pitr_source(a, n).await?;
        if !enabled {
            let removed = self
                .db
                .run(|tx| {
                    let e = self.clone();
                    let id = t.description.table_id.clone();
                    Box::pin(async move {
                        let k = e.key(&["pitr", &id]);
                        let Some(mut s) = kv::get::<State>(tx, k.clone()).await? else {
                            return Ok(None);
                        };
                        s.enabled = false;
                        kv::put(tx, k, &s).await?;
                        Ok(Some(s))
                    })
                })
                .await?;
            if let Some(s) = removed
                && let Err(err) = self.retire_pitr(&s).await
            {
                tracing::warn!(%err,"PITR disabled; retention lease cleanup will retry");
            }
            return Ok(disabled());
        }
        if previous.as_ref().is_some_and(|s| s.enabled) {
            return self.describe_pitr(a, n).await;
        }
        let now = self.db.timestamp().await?;
        let s = State {
            account: a.into(),
            generation: owner.generation,
            table: t.description.table_id,
            service: format!("extenddb-pitr-{}", uuid::Uuid::new_v4()),
            start: now,
            floor: now,
            refreshed: 0,
            enabled: true,
        };
        // Short provisional lease bounds orphan retention if publication fails or
        // its result is unknown. A confirmed setting receives the normal lease.
        if self.db.retain(s.service.clone(), now - 1, 300).await? > now - 1 {
            return Err(invalid(
                "Cannot retain history at the enable timestamp; retry",
            ));
        }
        self.db
            .run(|tx| {
                let e = self.clone();
                let s = s.clone();
                let n = n.to_owned();
                Box::pin(async move {
                    let t = e.table_by_name(tx, &s.account, &n).await?;
                    if t.description.table_id != s.table
                        || t.description.table_status != TableStatus::Active
                    {
                        return Err(invalid("Source table changed while enabling recovery"));
                    }
                    let k = e.key(&["pitr", &s.table]);
                    if kv::get::<State>(tx, k.clone())
                        .await?
                        .is_some_and(|v| v.enabled)
                    {
                        return Err(invalid("Recovery enablement changed concurrently; retry"));
                    }
                    kv::put(tx, k, &s).await
                })
            })
            .await?;
        if let Some(old) = previous {
            let _ = self.db.retain(old.service, 0, 0).await;
        }
        let s = self
            .refresh_pitr(s)
            .await?
            .ok_or_else(|| invalid("Recovery disabled concurrently"))?;
        Ok(description(&s, self.db.timestamp().await?))
    }
    pub(crate) async fn restore_pitr(
        &self,
        a: &str,
        source: &str,
        target: &str,
        point: RestorePoint,
    ) -> Result<TableDescription, StorageError> {
        let (t, owner, state) = self.pitr_source(a, source).await?;
        let state = state
            .filter(|s| s.enabled)
            .ok_or_else(|| invalid("Point-in-time recovery is not enabled"))?;
        let state = self
            .refresh_pitr(state)
            .await?
            .ok_or_else(|| invalid("Point-in-time recovery is not enabled"))?;
        let ts = requested(point, self.db.timestamp().await?, state.floor)?;
        let mut r = Reader::from_snapshot(self, a, source, self.db.snapshot_at(ts).await?).await?;
        if r.table.description.table_id != t.description.table_id
            || r.account_generation != owner.generation
        {
            return Err(invalid(
                "Source generation did not exist at RestoreDateTime",
            ));
        }
        let mut w = Writer::begin(self, &t, target, owner.generation).await?;
        let result = async {
            loop {
                let items = r.next().await?;
                if items.is_empty() {
                    break;
                }
                w.write(&items).await?;
            }
            w.finish().await
        }
        .await;
        if result.is_err() {
            w.abandon().await;
        }
        result
    }
    /// Called only by namespace destruction, after all writers have stopped.
    pub(crate) async fn destroy_pitr(&self) -> Result<(), StorageError> {
        loop {
            let states = self
                .db
                .run(|tx| {
                    let e = self.clone();
                    Box::pin(async move {
                        kv::prefix(tx, e.key(&["pitr"]), 64)
                            .await?
                            .into_iter()
                            .map(|(_, v)| kv::decode::<State>(&v))
                            .collect::<Result<Vec<_>, _>>()
                    })
                })
                .await?;
            if states.is_empty() {
                return Ok(());
            }
            for s in states {
                self.retire_pitr(&s).await?;
            }
        }
    }
    /// Renew/reclaim one persisted page, independently of data/index maintenance.
    pub async fn pitr_step(&self) -> Result<(), StorageError> {
        let rows = self
            .db
            .run(|tx| {
                let e = self.clone();
                Box::pin(async move {
                    let p = e.key(&["pitr"]);
                    let ck = e.key(&["maintenance_cursor", "pitr"]);
                    let start = kv::get::<Vec<u8>>(tx, ck.clone())
                        .await?
                        .filter(|v| !v.is_empty())
                        .unwrap_or_else(|| p.clone());
                    let rows = tx
                        .scan(start, crate::codec::prefix_end(&p), 64, false)
                        .await
                        .map_err(kv::storage_error)?;
                    let next = if rows.len() == 64 {
                        let mut k = rows.last().unwrap().0.clone();
                        k.push(0);
                        k
                    } else {
                        vec![]
                    };
                    kv::put(tx, ck, &next).await?;
                    rows.into_iter()
                        .map(|(_, v)| kv::decode::<State>(&v))
                        .collect::<Result<Vec<_>, _>>()
                })
            })
            .await?;
        let mut failure = None;
        for s in rows {
            if let Err(err) = self.refresh_pitr(s).await {
                failure.get_or_insert(err);
            }
        }
        match failure {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn time_selection_never_falls_back_to_current_state() {
        let now = 10_000u64 << 18;
        let floor = 5_000u64 << 18;
        assert_eq!(
            requested(RestorePoint::Timestamp(7.0), now, floor).unwrap(),
            (7_000 << 18) | ((1 << 18) - 1)
        );
        assert!(requested(RestorePoint::Timestamp(4.999), now, floor).is_err());
        for t in [f64::NAN, f64::INFINITY, -1., 11.] {
            assert!(requested(RestorePoint::Timestamp(t), now, floor).is_err());
        }
        assert_eq!(requested(RestorePoint::Latest, now, floor).unwrap(), now);
    }
}
