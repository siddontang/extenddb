// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional catalog independent of transport and HTTP handlers.
//!
//! IAM mutations use a protected account header and independently stored records.
//! Pure management transformations operate on a snapshot and persist only their
//! changed records. Authorization reads just the relevant principal/session.
//! See `records` for layout migration and transaction invariants.

pub mod authorization;
pub mod credentials;
pub mod crypto;
pub mod management;
pub mod operational;
mod records;

use crate::{TikvEngine, kv};
use extenddb_storage::{error::StorageError, management_store::*};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use time::OffsetDateTime;
use zeroize::Zeroizing;

#[derive(Clone)]
pub struct TikvCatalog {
    pub(crate) engine: TikvEngine,
    pub(crate) encryption_key: Option<Arc<Zeroizing<String>>>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Account {
    #[serde(default = "legacy_layout")]
    pub layout: u8,
    pub id: String,
    #[serde(default)]
    pub generation: String,
    pub name: String,
    pub created: OffsetDateTime,
    pub users: BTreeMap<String, Principal>,
    pub groups: BTreeMap<String, Principal>,
    pub roles: BTreeMap<String, Principal>,
    pub sessions: BTreeMap<String, Session>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Principal {
    pub created: OffsetDateTime,
    pub password: Option<String>,
    pub trust: Value,
    pub tags: BTreeMap<String, String>,
    pub policies: BTreeMap<String, (Value, OffsetDateTime)>,
    pub boundary: Option<Value>,
    pub members: BTreeSet<String>,
    pub keys: BTreeMap<String, AccessKey>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct AccessKey {
    pub encrypted: Vec<u8>,
    pub active: bool,
    pub created: OffsetDateTime,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Session {
    pub role: String,
    pub name: String,
    pub token: String,
    pub encrypted: Vec<u8>,
    pub tags: Option<Value>,
    pub policy: Option<Value>,
    pub expires: OffsetDateTime,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Locator {
    pub account: String,
    pub principal: String,
    pub session: bool,
}

pub(crate) fn op_error(e: StorageError) -> OpError {
    OpError::Internal(e.to_string())
}
pub(crate) fn missing(s: &str) -> OpError {
    OpError::NotFound(format!("{s} not found"))
}
pub(crate) fn duplicate(s: &str) -> OpError {
    OpError::AlreadyExists(format!("{s} already exists"))
}
pub(crate) fn arn(id: &str, kind: &str, name: &str) -> String {
    format!("arn:aws:iam::{id}:{kind}/{name}")
}
pub(crate) fn pairs(m: &BTreeMap<String, String>) -> Vec<(String, String)> {
    m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}
fn legacy_layout() -> u8 {
    1
}
impl Account {
    pub(crate) fn new(id: String, name: String, created: OffsetDateTime) -> Self {
        Self {
            layout: 2,
            id,
            generation: uuid::Uuid::new_v4().to_string(),
            name,
            created,
            users: BTreeMap::new(),
            groups: BTreeMap::new(),
            roles: BTreeMap::new(),
            sessions: BTreeMap::new(),
        }
    }
    pub(crate) fn principal(&self, kind: &str, name: &str) -> OpResult<&Principal> {
        match kind {
            "user" => self.users.get(name),
            "group" => self.groups.get(name),
            "role" => self.roles.get(name),
            _ => return Err(OpError::Validation("Invalid principal type".into())),
        }
        .ok_or_else(|| missing(kind))
    }
    pub(crate) fn principal_mut(&mut self, kind: &str, name: &str) -> OpResult<&mut Principal> {
        match kind {
            "user" => self.users.get_mut(name),
            "group" => self.groups.get_mut(name),
            "role" => self.roles.get_mut(name),
            _ => return Err(OpError::Validation("Invalid principal type".into())),
        }
        .ok_or_else(|| missing(kind))
    }
    fn locators(&self) -> BTreeMap<String, Locator> {
        self.users
            .iter()
            .flat_map(|(name, p)| {
                p.keys.keys().map(move |id| {
                    (
                        id.clone(),
                        Locator {
                            account: self.id.clone(),
                            principal: name.clone(),
                            session: false,
                        },
                    )
                })
            })
            .chain(self.sessions.iter().map(|(id, s)| {
                (
                    id.clone(),
                    Locator {
                        account: self.id.clone(),
                        principal: s.role.clone(),
                        session: true,
                    },
                )
            }))
            .collect()
    }
}
impl Principal {
    fn new(created: OffsetDateTime) -> Self {
        Self {
            created,
            password: None,
            trust: Value::Null,
            tags: BTreeMap::new(),
            policies: BTreeMap::new(),
            boundary: None,
            members: BTreeSet::new(),
            keys: BTreeMap::new(),
        }
    }
}
impl TikvCatalog {
    pub fn new(engine: TikvEngine) -> Self {
        Self {
            engine,
            encryption_key: None,
        }
    }
    pub fn with_encryption_key(mut self, key: String) -> OpResult<Self> {
        crypto::validate_key(&key)?;
        self.encryption_key = Some(Arc::new(Zeroizing::new(key)));
        Ok(self)
    }
    pub(crate) fn now(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(self.engine.clock.now_ms()) * 1_000_000,
        )
        .expect("clock in supported epoch range")
    }
    pub(crate) async fn encryption_key(&self) -> OpResult<String> {
        if let Some(k) = &self.encryption_key {
            return Ok(k.as_str().to_owned());
        }
        self.get_setting("encryption_key")
            .await?
            .ok_or_else(|| missing("Encryption key"))
    }
    pub(crate) async fn read<R: Send + 'static>(
        &self,
        id: &str,
        f: impl Fn(Option<Account>) -> OpResult<R> + Send + Sync + 'static,
    ) -> OpResult<R> {
        let e = self.engine.clone();
        let id = id.to_owned();
        let f = Arc::new(f);
        e.db.clone()
            .run(move |tx| {
                let e = e.clone();
                let id = id.clone();
                let f = f.clone();
                Box::pin(async move { Ok(f(records::load(&e, tx, &id).await?)) })
            })
            .await
            .map_err(op_error)?
    }
    pub(crate) async fn read_principal<R: Send + 'static>(
        &self,
        id: &str,
        kind: &'static str,
        name: &str,
        f: impl Fn(Option<Principal>) -> OpResult<R> + Send + Sync + 'static,
    ) -> OpResult<R> {
        let e = self.engine.clone();
        let id = id.to_owned();
        let name = name.to_owned();
        let f = Arc::new(f);
        e.db.clone()
            .run(move |tx| {
                let e = e.clone();
                let id = id.clone();
                let name = name.clone();
                let f = f.clone();
                Box::pin(async move {
                    let p =
                        if let Some(a) = kv::get::<Account>(tx, e.key(&["account", &id])).await? {
                            records::principal(&e, tx, &a, kind, &name).await?
                        } else {
                            None
                        };
                    Ok(f(p))
                })
            })
            .await
            .map_err(op_error)?
    }
    pub(crate) async fn edit<R: Send + 'static>(
        &self,
        id: &str,
        f: impl Fn(&mut Account) -> OpResult<R> + Send + Sync + 'static,
    ) -> OpResult<R> {
        let e = self.engine.clone();
        let id = id.to_owned();
        let f = Arc::new(f);
        e.db.clone()
            .run(move |tx| {
                let id = id.clone();
                let f = f.clone();
                let e = e.clone();
                Box::pin(async move {
                    let Some(mut account) = records::load(&e, tx, &id).await? else {
                        return Ok(Err(missing("Account")));
                    };
                    let original = account.clone();
                    let before = account.locators();
                    let result = match f(&mut account) {
                        Ok(v) => v,
                        Err(e) => return Ok(Err(e)),
                    };
                    let after = account.locators();
                    // Validate every absent locator before staging any mutation.
                    for (id, locator) in &after {
                        if before.get(id) != Some(locator)
                            && kv::get::<Locator>(tx, e.key(&["credential", id]))
                                .await?
                                .is_some()
                        {
                            return Ok(Err(duplicate("Access key")));
                        }
                    }
                    for id in before.keys().filter(|id| !after.contains_key(*id)) {
                        kv::delete(tx, e.key(&["credential", id])).await?;
                    }
                    for (id, locator) in &after {
                        if before.get(id) != Some(locator) {
                            kv::put(tx, e.key(&["credential", id]), locator).await?;
                        }
                    }
                    records::save(&e, tx, &original, &account).await?;
                    Ok(Ok(result))
                })
            })
            .await
            .map_err(op_error)?
    }
}
