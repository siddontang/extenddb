// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Read-only policy projections from a consistent IAM aggregate snapshot.
//! Missing principals yield no authority. Session policies/tags are returned only
//! for live sessions, and resource tags are stored under the full account ARN.
use super::*;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use futures::future::BoxFuture;
impl AuthorizationStore for TikvCatalog {
    fn fetch_user_policies(
        &self,
        account_id: &str,
        name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let id = account_id.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            self.read(&id, move |a| {
                Ok(a.and_then(|a| {
                    a.users
                        .get(&name)
                        .map(|p| p.policies.values().map(|(d, _)| d.to_string()).collect())
                })
                .unwrap_or_default())
            })
            .await
        })
    }
    fn fetch_user_boundary(
        &self,
        account_id: &str,
        name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let id = account_id.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            self.read(&id, move |a| {
                Ok(a.and_then(|a| {
                    a.users
                        .get(&name)
                        .map(|p| p.boundary.as_ref().map(Value::to_string))
                })
                .unwrap_or_default())
            })
            .await
        })
    }
    fn fetch_user_tags(
        &self,
        account_id: &str,
        name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let id = account_id.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            self.read(&id, move |a| {
                Ok(a.and_then(|a| a.users.get(&name).map(|p| pairs(&p.tags)))
                    .unwrap_or_default())
            })
            .await
        })
    }
    fn fetch_role_policies(
        &self,
        account_id: &str,
        name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let id = account_id.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            self.read(&id, move |a| {
                Ok(a.and_then(|a| {
                    a.roles
                        .get(&name)
                        .map(|p| p.policies.values().map(|(d, _)| d.to_string()).collect())
                })
                .unwrap_or_default())
            })
            .await
        })
    }
    fn fetch_role_boundary(
        &self,
        account_id: &str,
        name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let id = account_id.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            self.read(&id, move |a| {
                Ok(a.and_then(|a| {
                    a.roles
                        .get(&name)
                        .map(|p| p.boundary.as_ref().map(Value::to_string))
                })
                .unwrap_or_default())
            })
            .await
        })
    }
    fn fetch_role_tags(
        &self,
        account_id: &str,
        name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let id = account_id.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            self.read(&id, move |a| {
                Ok(a.and_then(|a| a.roles.get(&name).map(|p| pairs(&p.tags)))
                    .unwrap_or_default())
            })
            .await
        })
    }
    fn fetch_user_group_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let id = account_id.to_owned();
        let name = user_name.to_owned();
        Box::pin(async move {
            self.read(&id, move |a| {
                Ok(a.map(|a| {
                    if !a.users.contains_key(&name) {
                        return vec![];
                    }
                    a.groups
                        .values()
                        .filter(|g| g.members.contains(&name))
                        .flat_map(|g| g.policies.values().map(|(d, _)| d.to_string()))
                        .collect()
                })
                .unwrap_or_default())
            })
            .await
        })
    }
    fn fetch_session_data(
        &self,
        account_id: &str,
        role_name: &str,
        session_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
        let id = account_id.to_owned();
        let role = role_name.to_owned();
        let name = session_name.to_owned();
        let now = self.now();
        Box::pin(async move {
            self.read(&id, move |a| {
                Ok(a.and_then(|a| {
                    a.sessions
                        .values()
                        .filter(|s| s.role == role && s.name == name && s.expires > now)
                        .max_by_key(|s| s.expires)
                        .map(|s| SessionData {
                            session_policy: s.policy.as_ref().map(Value::to_string),
                            session_tags: session_tags(&s.tags),
                        })
                }))
            })
            .await
        })
    }
    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let k = self.engine.key(&["tags", arn]);
        Box::pin(async move {
            self.engine
                .db
                .run(move |tx| {
                    let k = k.clone();
                    Box::pin(async move {
                        let tags: Vec<extenddb_core::types::Tag> =
                            kv::get(tx, k).await?.unwrap_or_default();
                        Ok(tags.into_iter().map(|t| (t.key, t.value)).collect())
                    })
                })
                .await
                .map_err(op_error)
        })
    }
}
pub(crate) fn session_tags(value: &Option<Value>) -> Vec<(String, String)> {
    match value {
        Some(Value::Object(m)) => m
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_owned())))
            .collect(),
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|v| {
                Some((
                    v.get("Key")?.as_str()?.to_owned(),
                    v.get("Value")?.as_str()?.to_owned(),
                ))
            })
            .collect(),
        _ => vec![],
    }
}
