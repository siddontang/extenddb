// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Normalized IAM records with an account header as the transaction fence.
//!
//! The logical aggregate remains convenient for pure management transformations,
//! but no account-sized value is persisted. Principals, policies, access keys,
//! memberships and sessions occupy separate keys. Changes write only their diff.
//! The account header participates in every mutation, so snapshot scans need no
//! predicate locks. Legacy inline aggregates are read and converted on first edit
//! in the same transaction as the requested mutation. Generation prefixes prevent
//! deleted accounts' records from becoming visible after numeric-ID reuse.
use super::*;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "record")]
enum Record {
    Principal {
        kind: String,
        name: String,
        value: Principal,
    },
    Policy {
        kind: String,
        name: String,
        policy: String,
        value: Value,
        created: OffsetDateTime,
    },
    Key {
        user: String,
        id: String,
        value: AccessKey,
    },
    Member {
        group: String,
        user: String,
    },
    Session {
        id: String,
        value: Session,
    },
}
fn key(e: &TikvEngine, a: &Account, parts: &[&str]) -> Vec<u8> {
    let mut key = e.key(&["iam", &a.id, &a.generation]);
    key.extend(crate::codec::tuple(parts));
    key
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(value).map_err(|e| StorageError::Internal(e.to_string()))
}
fn flattened(e: &TikvEngine, a: &Account) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, StorageError> {
    let mut rows = BTreeMap::new();
    for (kind, principals) in [("user", &a.users), ("group", &a.groups), ("role", &a.roles)] {
        for (name, p) in principals {
            let mut base = p.clone();
            base.policies.clear();
            base.members.clear();
            base.keys.clear();
            rows.insert(
                key(e, a, &[kind, name, "0"]),
                encode(&Record::Principal {
                    kind: kind.into(),
                    name: name.clone(),
                    value: base,
                })?,
            );
            for (policy, (value, created)) in &p.policies {
                rows.insert(
                    key(e, a, &[kind, name, "policy", policy]),
                    encode(&Record::Policy {
                        kind: kind.into(),
                        name: name.clone(),
                        policy: policy.clone(),
                        value: value.clone(),
                        created: *created,
                    })?,
                );
            }
            for (id, value) in &p.keys {
                rows.insert(
                    key(e, a, &[kind, name, "key", id]),
                    encode(&Record::Key {
                        user: name.clone(),
                        id: id.clone(),
                        value: value.clone(),
                    })?,
                );
            }
            for user in &p.members {
                let encoded = encode(&Record::Member {
                    group: name.clone(),
                    user: user.clone(),
                })?;
                rows.insert(key(e, a, &[kind, name, "member", user]), encoded.clone());
                rows.insert(key(e, a, &["member_of", user, name]), encoded);
            }
        }
    }
    for (id, value) in &a.sessions {
        let encoded = encode(&Record::Session {
            id: id.clone(),
            value: value.clone(),
        })?;
        rows.insert(key(e, a, &["session", id]), encoded.clone());
        rows.insert(
            key(e, a, &["session_name", &value.role, &value.name, id]),
            encoded,
        );
    }
    Ok(rows)
}
async fn scan(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    a: &Account,
    parts: &[&str],
) -> Result<Vec<Record>, StorageError> {
    let prefix = key(e, a, parts);
    let end = crate::codec::prefix_end(&prefix);
    let mut start = prefix;
    let mut out = vec![];
    loop {
        let rows = tx
            .scan_snapshot(start, end.clone(), 128, false)
            .await
            .map_err(kv::storage_error)?;
        let short = rows.len() < 128;
        start = rows.last().map_or_else(Vec::new, |(k, _)| {
            let mut k = k.clone();
            k.push(0);
            k
        });
        for (_, bytes) in rows {
            out.push(kv::decode(&bytes)?);
        }
        if short {
            break;
        }
    }
    Ok(out)
}
fn add(a: &mut Account, record: Record) -> Result<(), StorageError> {
    let missing = || StorageError::Internal("IAM child record without principal".into());
    match record {
        Record::Principal { kind, name, value } => {
            match kind.as_str() {
                "user" => &mut a.users,
                "group" => &mut a.groups,
                "role" => &mut a.roles,
                _ => return Err(missing()),
            }
            .insert(name, value);
        }
        Record::Policy {
            kind,
            name,
            policy,
            value,
            created,
        } => {
            a.principal_mut(&kind, &name)
                .map_err(|_| missing())?
                .policies
                .insert(policy, (value, created));
        }
        Record::Key { user, id, value } => {
            a.users
                .get_mut(&user)
                .ok_or_else(missing)?
                .keys
                .insert(id, value);
        }
        Record::Member { group, user } => {
            a.groups
                .get_mut(&group)
                .ok_or_else(missing)?
                .members
                .insert(user);
        }
        Record::Session { id, value } => {
            a.sessions.insert(id, value);
        }
    }
    Ok(())
}
/// Protected header plus snapshot children: a writing caller must save/delete
/// that header before commit, preserving the aggregate relationship invariant.
pub(crate) async fn load(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    id: &str,
) -> Result<Option<Account>, StorageError> {
    let Some(mut a) = kv::get::<Account>(tx, e.key(&["account", id])).await? else {
        return Ok(None);
    };
    if a.layout == 1 {
        return Ok(Some(a));
    }
    if a.layout != 2 {
        return Err(StorageError::Internal("Unknown IAM record layout".into()));
    }
    let records = scan(e, tx, &a, &[]).await?;
    // Secondary indexes may sort before their principal. Materialize roots first.
    for r in records
        .iter()
        .filter(|r| matches!(r, Record::Principal { .. }))
    {
        add(&mut a, r.clone())?;
    }
    for r in records
        .into_iter()
        .filter(|r| !matches!(r, Record::Principal { .. }))
    {
        add(&mut a, r)?;
    }
    Ok(Some(a))
}
/// Save a diff and compact header in the same caller-owned transaction.
pub(crate) async fn save(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    before: &Account,
    after: &Account,
) -> Result<(), StorageError> {
    let old = if before.layout == 1 {
        BTreeMap::new()
    } else {
        flattened(e, before)?
    };
    let new = flattened(e, after)?;
    // This bounds individual physical objects, never total account size. Public
    // IAM request validation has much smaller policy/tag/trust-document limits.
    if new.values().any(|v| v.len() > 4 * 1024 * 1024) {
        return Err(StorageError::Validation(
            "One IAM object exceeds the 4 MiB physical-record limit".into(),
        ));
    }
    for k in old.keys().filter(|k| !new.contains_key(*k)) {
        kv::delete(tx, k.clone()).await?;
    }
    for (k, v) in new {
        if old.get(&k) != Some(&v) {
            tx.put(k, v).await.map_err(kv::storage_error)?;
        }
    }
    let mut header = after.clone();
    header.layout = 2;
    header.users.clear();
    header.groups.clear();
    header.roles.clear();
    header.sessions.clear();
    kv::put(tx, e.key(&["account", &after.id]), &header).await
}
/// Enqueue unreachable records for bounded deletion after removing the header.
pub(crate) async fn retire(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    a: &Account,
) -> Result<(), StorageError> {
    if a.layout == 2 {
        kv::put(
            tx,
            e.key(&["garbage", "iam", &a.id, &a.generation]),
            &key(e, a, &[]),
        )
        .await?;
    }
    Ok(())
}
/// Fetch just one principal, without reading other tenants or principals.
pub(crate) async fn principal(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    a: &Account,
    kind: &str,
    name: &str,
) -> Result<Option<Principal>, StorageError> {
    if a.layout == 1 {
        return Ok(a.principal(kind, name).ok().cloned());
    }
    let rows = scan(e, tx, a, &[kind, name]).await?;
    let mut view = a.clone();
    for r in rows {
        add(&mut view, r)?;
    }
    Ok(view.principal(kind, name).ok().cloned())
}
pub(crate) async fn session(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    a: &Account,
    id: &str,
) -> Result<Option<Session>, StorageError> {
    if a.layout == 1 {
        return Ok(a.sessions.get(id).cloned());
    }
    match kv::get::<Record>(tx, key(e, a, &["session", id])).await? {
        Some(Record::Session { value, .. }) => Ok(Some(value)),
        None => Ok(None),
        _ => Err(StorageError::Internal("Invalid session record".into())),
    }
}
pub(crate) async fn named_sessions(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    a: &Account,
    role: &str,
    name: &str,
) -> Result<Vec<Session>, StorageError> {
    if a.layout == 1 {
        return Ok(a
            .sessions
            .values()
            .filter(|s| s.role == role && s.name == name)
            .cloned()
            .collect());
    }
    Ok(scan(e, tx, a, &["session_name", role, name])
        .await?
        .into_iter()
        .filter_map(|r| match r {
            Record::Session { value, .. } => Some(value),
            _ => None,
        })
        .collect())
}
pub(crate) async fn groups(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    a: &Account,
    user: &str,
) -> Result<Vec<Principal>, StorageError> {
    if a.layout == 1 {
        return Ok(a
            .groups
            .values()
            .filter(|g| g.members.contains(user))
            .cloned()
            .collect());
    }
    let mut out = vec![];
    for r in scan(e, tx, a, &["member_of", user]).await? {
        if let Record::Member { group, .. } = r
            && let Some(p) = principal(e, tx, a, "group", &group).await?
        {
            out.push(p);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::memory::MemoryStore;

    #[tokio::test]
    async fn legacy_migration_is_atomic_and_oversized_records_roll_back() {
        let e =
            TikvEngine::new(Arc::new(MemoryStore::default()), "legacy-iam", "us-east-1").unwrap();
        let c = TikvCatalog::new(e.clone());
        let mut old = Account::new("a".into(), "old".into(), c.now());
        old.layout = 1;
        let mut p = Principal::new(c.now());
        p.policies.insert(
            "old".into(),
            (serde_json::json!({"Statement": []}), c.now()),
        );
        old.users.insert("alice".into(), p);
        let mut encoded = serde_json::to_value(&old).unwrap();
        encoded.as_object_mut().unwrap().remove("layout");
        e.db.run(|tx| {
            let e = e.clone();
            let encoded = encoded.clone();
            Box::pin(async move { kv::put(tx, e.key(&["account", "a"]), &encoded).await })
        })
        .await
        .unwrap();
        c.tag_user("a", "alice", &[("env".into(), "dev".into())])
            .await
            .unwrap();
        e.db.run(|tx| {
            let e = e.clone();
            Box::pin(async move {
                let a: Account = kv::get(tx, e.key(&["account", "a"])).await?.unwrap();
                assert_eq!(a.layout, 2);
                assert!(a.users.is_empty());
                assert_eq!(
                    principal(&e, tx, &a, "user", "alice")
                        .await?
                        .unwrap()
                        .policies
                        .len(),
                    1
                );
                Ok(())
            })
        })
        .await
        .unwrap();
        assert!(
            c.edit("a", |a| {
                let p = a.users.get_mut("alice").unwrap();
                p.keys.insert(
                    "AKIA_MUST_ROLL_BACK".into(),
                    AccessKey {
                        encrypted: vec![],
                        active: true,
                        created: p.created,
                    },
                );
                p.policies.insert(
                    "too-large".into(),
                    (Value::String("x".repeat(4 * 1024 * 1024)), p.created),
                );
                Ok(())
            })
            .await
            .is_err()
        );
        assert!(c.list_access_keys("a", "alice").await.unwrap().is_empty());
        e.db.run(|tx| {
            let e = e.clone();
            Box::pin(async move {
                assert!(
                    kv::get::<Locator>(tx, e.key(&["credential", "AKIA_MUST_ROLL_BACK"]))
                        .await?
                        .is_none()
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
