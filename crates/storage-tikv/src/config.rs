// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Connection configuration shared by serve, bootstrap and diagnostics.
//!
//! `connection_config()` carries a serialized descriptor, preserving endpoint,
//! namespace and TLS settings across the existing string-based factory seam.
//! No passwords are accepted in PD endpoints. TLS paths must be supplied as a
//! complete triple. Namespace changes select a different deployment.

use extenddb_storage::{config::StorageConfig, error::StorageError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TikvConfig {
    pub pd_endpoints: Vec<String>,
    pub namespace: String,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub request_timeout_seconds: u64,
    #[serde(skip)]
    pub(crate) descriptor: String,
}
impl Default for TikvConfig {
    fn default() -> Self {
        Self {
            pd_endpoints: vec!["127.0.0.1:2379".into()],
            namespace: "extenddb".into(),
            ca_path: None,
            cert_path: None,
            key_path: None,
            request_timeout_seconds: 10,
            descriptor: String::new(),
        }
    }
}
pub fn validate_namespace(s: &str) -> Result<(), StorageError> {
    if s.is_empty()
        || s.len() > 128
        || !s
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err(StorageError::Validation(
            "TiKV namespace must contain 1..128 ASCII letters, digits, '_' or '-'".into(),
        ));
    }
    Ok(())
}
impl TikvConfig {
    pub fn validated(mut self) -> Result<Self, StorageError> {
        validate_namespace(&self.namespace)?;
        if self.pd_endpoints.is_empty()
            || self
                .pd_endpoints
                .iter()
                .any(|p| p.is_empty() || p.contains('@') || p.contains('?') || p.contains('#'))
        {
            return Err(StorageError::Validation("Invalid TiKV PD endpoints".into()));
        }
        let tls = [
            self.ca_path.is_some(),
            self.cert_path.is_some(),
            self.key_path.is_some(),
        ];
        if tls.iter().any(|x| *x) && !tls.iter().all(|x| *x) {
            return Err(StorageError::Validation(
                "TiKV TLS requires ca_path, cert_path and key_path".into(),
            ));
        }
        if self.request_timeout_seconds == 0 {
            return Err(StorageError::Validation(
                "TiKV request timeout must be positive".into(),
            ));
        }
        self.descriptor =
            serde_json::to_string(&self).map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(self)
    }
    pub fn from_descriptor(s: &str) -> Result<Self, StorageError> {
        serde_json::from_str::<Self>(s)
            .map_err(|e| StorageError::Validation(e.to_string()))?
            .validated()
    }
    #[cfg(feature = "client")]
    pub async fn connect(&self) -> Result<std::sync::Arc<dyn crate::kv::Store>, StorageError> {
        let validated = self.clone().validated()?;
        let mut config = tikv_client::Config::default()
            .with_timeout(std::time::Duration::from_secs(
                validated.request_timeout_seconds,
            ))
            .with_grpc_max_decoding_message_size(128 * 1024 * 1024);
        if let (Some(ca), Some(cert), Some(key)) = (
            &validated.ca_path,
            &validated.cert_path,
            &validated.key_path,
        ) {
            config = config.with_security(ca, cert, key);
        }
        Ok(std::sync::Arc::new(
            crate::kv::client::TikvStore::connect(validated.pd_endpoints, config)
                .await
                .map_err(crate::kv::storage_error)?,
        ))
    }
}
impl StorageConfig for TikvConfig {
    fn connection_config(&self) -> &str {
        &self.descriptor
    }
    fn max_connections(&self) -> u32 {
        1
    }
    fn max_catalog_connections(&self) -> u32 {
        1
    }
    fn clone_box(&self) -> Box<dyn StorageConfig> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn descriptor_roundtrip_preserves_namespace_and_tls() {
        let c = TikvConfig {
            namespace: "test_2".into(),
            ca_path: Some("ca".into()),
            cert_path: Some("cert".into()),
            key_path: Some("key".into()),
            ..Default::default()
        }
        .validated()
        .unwrap();
        let d = TikvConfig::from_descriptor(c.connection_config()).unwrap();
        assert_eq!(c.namespace, d.namespace);
        assert_eq!(c.key_path, d.key_path);
    }
    #[test]
    fn rejects_unsafe_or_partial_config() {
        assert!(validate_namespace("").is_err());
        assert!(validate_namespace("a/b").is_err());
        assert!(
            TikvConfig {
                ca_path: Some("ca".into()),
                ..Default::default()
            }
            .validated()
            .is_err()
        );
        assert!(
            TikvConfig {
                pd_endpoints: vec![],
                ..Default::default()
            }
            .validated()
            .is_err()
        );
    }
}
