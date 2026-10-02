// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM operations as pure aggregate transformations inside catalog transactions.
//! No SQL, network clients, encryption or clock calls occur in retry closures.
//! Credential locators and the account document commit atomically in `edit`.
use super::*;
use extenddb_storage::management_store::{GroupListEntry, RoleListEntry, UserListEntry};
use futures::future::BoxFuture;
impl ManagementStore for TikvCatalog {
    fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let account_name = account_name.to_owned();
        Box::pin(async move {
            let e = self.engine.clone();
            let now = self.now();
            e.db.clone()
                .run(move |tx| {
                    let e = e.clone();
                    let id = account_id.clone();
                    let name = account_name.clone();
                    Box::pin(async move {
                        let key = e.key(&["account", &id]);
                        let nk = e.key(&["account_name", &name]);
                        if kv::get::<Account>(tx, key.clone()).await?.is_some()
                            || kv::get::<String>(tx, nk.clone()).await?.is_some()
                        {
                            return Ok(Err(duplicate("Account")));
                        }
                        kv::put(tx, key, &Account::new(id.clone(), name, now)).await?;
                        kv::put(tx, nk, &id).await?;
                        Ok(Ok(()))
                    })
                })
                .await
                .map_err(op_error)?
        })
    }
    fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let e = self.engine.clone();
            e.db.clone()
                .run(move |tx| {
                    let e = e.clone();
                    let id = account_id.clone();
                    Box::pin(async move {
                        let key = e.key(&["account", &id]);
                        let Some(a) = kv::get::<Account>(tx, key.clone()).await? else {
                            return Ok(Err(missing("Account")));
                        };
                        let count = kv::get::<u64>(tx, e.key(&["account_tables", &id]))
                            .await?
                            .unwrap_or(0);
                        if count > 0 {
                            return Ok(Err(OpError::HasDependents(
                                "Cannot delete account with existing tables".into(),
                            )));
                        }
                        for id in a.locators().keys() {
                            kv::delete(tx, e.key(&["credential", id])).await?;
                        }
                        kv::delete(tx, e.key(&["account_name", &a.name])).await?;
                        kv::delete(tx, key).await?;
                        Ok(Ok(()))
                    })
                })
                .await
                .map_err(op_error)?
        })
    }
    fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async move {
            Ok(self
                .accounts()
                .await?
                .into_iter()
                .map(|a| (a.id, a.name))
                .collect())
        })
    }
    fn default_account_id(&self) -> BoxFuture<'_, OpResult<Option<String>>> {
        Box::pin(async move { self.get_setting("default_account_id").await })
    }
    fn list_all_accounts_full(
        &self,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, OffsetDateTime)>>> {
        Box::pin(async move {
            Ok(self
                .accounts()
                .await?
                .into_iter()
                .map(|a| (a.id, a.name, a.created))
                .collect())
        })
    }
    fn list_accounts_for(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(vec![]) };
                Ok(vec![(a.id, a.name)])
            })
            .await
        })
    }
    fn get_account_detail(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<AccountDetail>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(None) };
                Ok(Some(AccountDetail {
                    account_name: a.name,
                    users: a.users.into_keys().collect(),
                    groups: a.groups.into_keys().collect(),
                    roles: a.roles.into_keys().collect(),
                }))
            })
            .await
        })
    }
    fn dashboard_counts(&self) -> BoxFuture<'_, OpResult<(i64, i64)>> {
        Box::pin(async move {
            Ok((
                self.accounts().await?.len() as i64,
                self.list_admins().await?.len() as i64,
            ))
        })
    }
    fn create_user(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password_hash = password_hash.map(str::to_owned);
        Box::pin(async move {
            let now = self.now();
            self.edit(&account_id,move |a|{if a.users.contains_key(&user_name){return Err(duplicate("User"))}let mut p=Principal::new(now);p.password=password_hash.clone();p.policies.insert("SelfServicePolicy".into(),(serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:CreateAccessKey","iam:DeleteAccessKey","iam:ListAccessKeys","iam:ChangePassword"],"Resource":arn(&a.id,"user",&user_name)}]}),now));a.users.insert(user_name.clone(),p);Ok(())}).await
        })
    }
    fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.users.remove(&user_name).ok_or_else(|| missing("User"))?;
                for g in a.groups.values_mut() {
                    g.members.remove(&user_name);
                }
                Ok(())
            })
            .await
        })
    }
    fn list_users(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<UserListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(vec![]) };
                Ok(a.users
                    .iter()
                    .map(|(n, p)| {
                        (
                            a.id.clone(),
                            n.clone(),
                            arn(&a.id, "user", n),
                            p.password.is_some(),
                            p.created,
                        )
                    })
                    .collect())
            })
            .await
        })
    }
    fn get_user_detail(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<UserDetail>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(None) };
                Ok(a.users.get(&user_name).map(|p| UserDetail {
                    keys: p.keys.iter().map(|(n, k)| (n.clone(), k.active)).collect(),
                    policies: p.policies.keys().cloned().collect(),
                    tags: pairs(&p.tags),
                    groups: a
                        .groups
                        .iter()
                        .filter(|(_, p)| p.members.contains(&user_name))
                        .map(|(n, _)| n.clone())
                        .collect(),
                }))
            })
            .await
        })
    }
    fn verify_iam_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<bool>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password = password.to_owned();
        Box::pin(async move {
            let hash = self
                .read(&account_id, move |a| {
                    Ok(a.and_then(|a| a.users.get(&user_name).and_then(|p| p.password.clone())))
                })
                .await?;
            match hash {
                Some(h) => super::operational::verify(password, h).await,
                None => Ok(false),
            }
        })
    }
    fn change_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut("user", &user_name)?.password = Some(password_hash.clone());
                Ok(())
            })
            .await
        })
    }
    fn tag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let tags = tags.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut("user", &user_name)?
                    .tags
                    .extend(tags.clone());
                Ok(())
            })
            .await
        })
    }
    fn untag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let tag_keys = tag_keys.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                let p = a.principal_mut("user", &user_name)?;
                for k in &tag_keys {
                    p.tags.remove(k);
                }
                Ok(())
            })
            .await
        })
    }
    fn list_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let a = a.ok_or_else(|| missing("Account"))?;
                Ok(pairs(&a.principal("user", &user_name)?.tags))
            })
            .await
        })
    }
    fn set_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
        document: &Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let document = document.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut("user", &user_name)?.boundary = Some(document.clone());
                Ok(())
            })
            .await
        })
    }
    fn get_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<Value>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let a = a.ok_or_else(|| missing("Account"))?;
                Ok(a.principal("user", &user_name)?.boundary.clone())
            })
            .await
        })
    }
    fn delete_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut("user", &user_name)?.boundary = None;
                Ok(())
            })
            .await
        })
    }
    fn tag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let tags = tags.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut("role", &role_name)?
                    .tags
                    .extend(tags.clone());
                Ok(())
            })
            .await
        })
    }
    fn untag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let tag_keys = tag_keys.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                let p = a.principal_mut("role", &role_name)?;
                for k in &tag_keys {
                    p.tags.remove(k);
                }
                Ok(())
            })
            .await
        })
    }
    fn list_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let a = a.ok_or_else(|| missing("Account"))?;
                Ok(pairs(&a.principal("role", &role_name)?.tags))
            })
            .await
        })
    }
    fn set_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
        document: &Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let document = document.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut("role", &role_name)?.boundary = Some(document.clone());
                Ok(())
            })
            .await
        })
    }
    fn get_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<Value>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let a = a.ok_or_else(|| missing("Account"))?;
                Ok(a.principal("role", &role_name)?.boundary.clone())
            })
            .await
        })
    }
    fn delete_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut("role", &role_name)?.boundary = None;
                Ok(())
            })
            .await
        })
    }
    fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move {
            let now = self.now();
            self.edit(&account_id, move |a| {
                if a.groups.contains_key(&group_name) {
                    return Err(duplicate("Group"));
                }
                a.groups.insert(group_name.clone(), Principal::new(now));
                Ok(())
            })
            .await
        })
    }
    fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.groups
                    .remove(&group_name)
                    .ok_or_else(|| missing("Group"))?;
                Ok(())
            })
            .await
        })
    }
    fn list_groups(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<GroupListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(vec![]) };
                Ok(a.groups
                    .iter()
                    .map(|(n, p)| (a.id.clone(), n.clone(), arn(&a.id, "group", n), p.created))
                    .collect())
            })
            .await
        })
    }
    fn get_group_detail(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<GroupDetail>>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(None) };
                Ok(a.groups.get(&group_name).map(|p| GroupDetail {
                    members: p.members.iter().cloned().collect(),
                    policies: p.policies.keys().cloned().collect(),
                    all_users: a.users.keys().cloned().collect(),
                }))
            })
            .await
        })
    }
    fn add_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal("user", &user_name)?;
                a.principal_mut("group", &group_name)?
                    .members
                    .insert(user_name.clone());
                Ok(())
            })
            .await
        })
    }
    fn remove_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                if !a
                    .principal_mut("group", &group_name)?
                    .members
                    .remove(&user_name)
                {
                    return Err(missing("Group membership"));
                }
                Ok(())
            })
            .await
        })
    }
    fn create_role(
        &self,
        account_id: &str,
        role_name: &str,
        trust_policy: &Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let trust_policy = trust_policy.to_owned();
        Box::pin(async move {
            let now = self.now();
            self.edit(&account_id, move |a| {
                if a.roles.contains_key(&role_name) {
                    return Err(duplicate("Role"));
                }
                let mut p = Principal::new(now);
                p.trust = trust_policy.clone();
                a.roles.insert(role_name.clone(), p);
                Ok(())
            })
            .await
        })
    }
    fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.roles.remove(&role_name).ok_or_else(|| missing("Role"))?;
                a.sessions.retain(|_, s| s.role != role_name);
                Ok(())
            })
            .await
        })
    }
    fn list_roles(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<RoleListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(vec![]) };
                Ok(a.roles
                    .iter()
                    .map(|(n, p)| {
                        (
                            a.id.clone(),
                            n.clone(),
                            arn(&a.id, "role", n),
                            p.trust.clone(),
                            p.created,
                        )
                    })
                    .collect())
            })
            .await
        })
    }
    fn get_role_detail(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<RoleDetail>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(None) };
                Ok(a.roles.get(&role_name).map(|p| RoleDetail {
                    trust_policy: p.trust.clone(),
                    policies: p.policies.keys().cloned().collect(),
                    tags: pairs(&p.tags),
                }))
            })
            .await
        })
    }
    fn get_role_trust_policy(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<Value>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let Some(a) = a else { return Ok(None) };
                Ok(a.roles.get(&role_name).map(|p| p.trust.clone()))
            })
            .await
        })
    }
    fn put_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        let policy_name = policy_name.to_owned();
        let document = document.to_owned();
        Box::pin(async move {
            let now = self.now();
            self.edit(&account_id, move |a| {
                a.principal_mut(&principal_type, &principal_name)?
                    .policies
                    .insert(policy_name.clone(), (document.clone(), now));
                Ok(())
            })
            .await
        })
    }
    fn delete_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        let policy_name = policy_name.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut(&principal_type, &principal_name)?
                    .policies
                    .remove(&policy_name)
                    .ok_or_else(|| missing("Policy"))?;
                Ok(())
            })
            .await
        })
    }
    fn list_policies(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, Value, OffsetDateTime)>>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let a = a.ok_or_else(|| missing("Account"))?;
                Ok(a.principal(&principal_type, &principal_name)?
                    .policies
                    .iter()
                    .map(|(n, (d, t))| (n.clone(), d.clone(), *t))
                    .collect())
            })
            .await
        })
    }
    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let id = format!(
                "AKIAEXTENDDB{}",
                random_chars(8, b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
            );
            let secret = format!(
                "extenddb{}",
                random_chars(
                    32,
                    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
                )
            );
            self.import_access_key(&account_id, &user_name, &id, &secret)
                .await?;
            Ok(AccessKeyCreated {
                access_key_id: id,
                secret_access_key: secret,
            })
        })
    }
    fn delete_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        key_id: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let key_id = key_id.to_owned();
        Box::pin(async move {
            self.edit(&account_id, move |a| {
                a.principal_mut("user", &user_name)?
                    .keys
                    .remove(&key_id)
                    .ok_or_else(|| missing("Access key"))?;
                Ok(())
            })
            .await
        })
    }
    fn list_access_keys(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, bool, OffsetDateTime)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.read(&account_id, move |a| {
                let a = a.ok_or_else(|| missing("Account"))?;
                Ok(a.principal("user", &user_name)?
                    .keys
                    .iter()
                    .map(|(n, k)| (n.clone(), k.active, k.created))
                    .collect())
            })
            .await
        })
    }
    fn import_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let access_key_id = access_key_id.to_owned();
        let secret_access_key = secret_access_key.to_owned();
        Box::pin(async move {
            let encrypted = crypto::encrypt(
                &secret_access_key,
                &self.encryption_key().await?,
                &access_key_id,
            )?;
            let now = self.now();
            self.edit(&account_id, move |a| {
                let p = a.principal_mut("user", &user_name)?;
                if p.keys.contains_key(&access_key_id) {
                    return Err(duplicate("Access key"));
                }
                p.keys.insert(
                    access_key_id.clone(),
                    AccessKey {
                        encrypted: encrypted.clone(),
                        active: true,
                        created: now,
                    },
                );
                Ok(())
            })
            .await
        })
    }
    fn store_session(
        &self,
        session_token: &str,
        access_key_id: &str,
        secret_key_encrypted: &[u8],
        account_id: &str,
        role_name: &str,
        session_name: &str,
        session_tags: &Option<Value>,
        session_policy: &Option<Value>,
        expires_at: OffsetDateTime,
    ) -> BoxFuture<'_, OpResult<()>> {
        let session_token = session_token.to_owned();
        let access_key_id = access_key_id.to_owned();
        let secret_key_encrypted = secret_key_encrypted.to_owned();
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        let session_tags = session_tags.to_owned();
        let session_policy = session_policy.to_owned();
        Box::pin(async move {
            let now = self.now();
            self.edit(&account_id, move |a| {
                a.principal("role", &role_name)?;
                a.sessions.retain(|_, s| s.expires > now);
                if a.sessions.values().any(|s|s.role==role_name && s.name==session_name && (s.tags!=session_tags || s.policy!=session_policy)) {return Err(OpError::Validation("A live role session with this name has different tags or policy; use a distinct session name".into()))}
                if a.sessions.contains_key(&access_key_id) {
                    return Err(duplicate("Session"));
                }
                a.sessions.insert(
                    access_key_id.clone(),
                    Session {
                        role: role_name.clone(),
                        name: session_name.clone(),
                        token: session_token.clone(),
                        encrypted: secret_key_encrypted.clone(),
                        tags: session_tags.clone(),
                        policy: session_policy.clone(),
                        expires: expires_at,
                    },
                );
                Ok(())
            })
            .await
        })
    }
    fn fetch_caller_tags(
        &self,
        account_id: &str,
        resource: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let resource = resource.to_owned();
        Box::pin(async move {
            let Some((kind, name)) = resource.split_once('/') else {
                return Ok(vec![]);
            };
            match kind {
                "user" => self.list_user_tags(&account_id, name).await,
                "role" => self.list_role_tags(&account_id, name).await,
                _ => Ok(vec![]),
            }
        })
    }
}
impl TikvCatalog {
    async fn accounts(&self) -> OpResult<Vec<Account>> {
        let e = self.engine.clone();
        let k = e.key(&["account"]);
        e.db.run(move |tx| {
            let k = k.clone();
            Box::pin(async move {
                kv::all(tx, k)
                    .await?
                    .into_iter()
                    .map(|(_, v)| kv::decode(&v))
                    .collect()
            })
        })
        .await
        .map_err(op_error)
    }
}
fn random_chars(len: usize, charset: &[u8]) -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..len)
        .map(|_| charset[rng.random_range(0..charset.len())] as char)
        .collect()
}
