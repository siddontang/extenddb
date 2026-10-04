// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Version-one credential envelope: AES-256-GCM nonce(12) || ciphertext+tag.
//! Access-key ID is mandatory AAD. No unauthenticated legacy fallback exists.
//! This format matches the shared AssumeRole writer. Keys are zeroized on drop.
use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, AeadCore, OsRng, Payload},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use extenddb_storage::management_store::{OpError, OpResult};
use zeroize::Zeroizing;
fn cipher(key: &str) -> OpResult<Aes256Gcm> {
    let bytes = Zeroizing::new(
        STANDARD
            .decode(key)
            .map_err(|_| OpError::Internal("Invalid encryption key encoding".into()))?,
    );
    Aes256Gcm::new_from_slice(&bytes)
        .map_err(|_| OpError::Internal("Encryption key must contain 32 bytes".into()))
}
pub fn validate_key(key: &str) -> OpResult<()> {
    cipher(key).map(|_| ())
}
pub fn encrypt(secret: &str, key: &str, id: &str) -> OpResult<Vec<u8>> {
    let cipher = cipher(key)?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let encrypted = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: secret.as_bytes(),
                aad: id.as_bytes(),
            },
        )
        .map_err(|_| OpError::Internal("Credential encryption failed".into()))?;
    let mut bytes = nonce.to_vec();
    bytes.extend(encrypted);
    Ok(bytes)
}
pub fn decrypt(bytes: &[u8], key: &str, id: &str) -> OpResult<String> {
    if bytes.len() < 28 {
        return Err(OpError::Internal("Truncated credential envelope".into()));
    }
    let plain = cipher(key)?
        .decrypt(
            Nonce::from_slice(&bytes[..12]),
            Payload {
                msg: &bytes[12..],
                aad: id.as_bytes(),
            },
        )
        .map_err(|_| OpError::Internal("Credential authentication failed".into()))?;
    String::from_utf8(plain).map_err(|_| OpError::Internal("Invalid credential encoding".into()))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn envelope_authenticates_key_identity_and_bytes() {
        let key = STANDARD.encode([9; 32]);
        let mut b = encrypt("secret", &key, "AKIA123").unwrap();
        assert_eq!(decrypt(&b, &key, "AKIA123").unwrap(), "secret");
        assert!(decrypt(&b, &key, "AKIA124").is_err());
        assert!(decrypt(&b, &STANDARD.encode([8; 32]), "AKIA123").is_err());
        b[12] ^= 1;
        assert!(decrypt(&b, &key, "AKIA123").is_err());
        assert!(decrypt(&b[..12], &key, "AKIA123").is_err());
        assert!(validate_key("aGVsbG8=").is_err());
        assert_ne!(
            encrypt("secret", &key, "AKIA123").unwrap(),
            encrypt("secret", &key, "AKIA123").unwrap()
        );
    }
}
