// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Credential resolution in one snapshot: locator + owner + credential.
//! Revocation/deletion changes both records atomically. Expiry is checked against
//! the injected clock; the shared auth cache also checks the returned deadline.
use super::*;
use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
fn auth_error(_: impl std::fmt::Debug) -> DynamoDbError {
    DynamoDbError::InternalServerError("Internal error during authentication".into())
}
#[async_trait::async_trait]
impl CredentialStore for TikvCatalog {
    async fn lookup_credential(&self, id: &str) -> Result<Option<StoredCredential>, DynamoDbError> {
        if !id.starts_with("AKIA") && !id.starts_with("ASIA") {
            return Ok(None);
        }
        let e = self.engine.clone();
        let id = id.to_owned();
        let enc = Zeroizing::new(self.encryption_key().await.map_err(auth_error)?);
        let now = self.now();
        e.db.clone()
            .run(move |tx| {
                let e = e.clone();
                let id = id.clone();
                let enc = enc.clone();
                Box::pin(async move {
                    let Some(loc) = kv::get::<Locator>(tx, e.key(&["credential", &id])).await?
                    else {
                        return Ok(Ok(None));
                    };
                    let Some(a) = kv::get::<Account>(tx, e.key(&["account", &loc.account])).await?
                    else {
                        return Ok(Ok(None));
                    };
                    let (bytes, session_name, token, active, expires) = if loc.session {
                        let Some(s) = a.sessions.get(&id) else {
                            return Ok(Ok(None));
                        };
                        if !a.roles.contains_key(&s.role) {
                            return Ok(Ok(None));
                        }
                        if s.expires <= now {
                            return Ok(Err(DynamoDbError::ExpiredTokenException(
                                "The security token included in the request is expired".into(),
                            )));
                        }
                        (
                            s.encrypted.clone(),
                            Some(s.name.clone()),
                            Some(s.token.clone()),
                            true,
                            Some(s.expires),
                        )
                    } else {
                        let Some(k) = a.users.get(&loc.principal).and_then(|u| u.keys.get(&id))
                        else {
                            return Ok(Ok(None));
                        };
                        (k.encrypted.clone(), None, None, k.active, None)
                    };
                    let secret = match crypto::decrypt(&bytes, &enc, &id) {
                        Ok(s) => s,
                        Err(err) => return Ok(Err(auth_error(err))),
                    };
                    Ok(Ok(Some(StoredCredential {
                        secret_key: secret,
                        account_id: loc.account,
                        principal_name: loc.principal,
                        session_name,
                        is_session: loc.session,
                        session_token: token,
                        is_active: active,
                        expires_at: expires,
                    })))
                })
            })
            .await
            .map_err(auth_error)?
    }
}
