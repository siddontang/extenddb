// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pure DynamoDB resource-tag validation.
//!
//! Character limits count Unicode scalar values; the aggregate 10 KiB budget
//! counts UTF-8 bytes. Call `validate_tags` for request members before mutation
//! and for the final merged set inside the storage transaction. The latter is
//! necessary to enforce limits when concurrent requests add different keys.
use crate::{error::DynamoDbError, types::Tag};
use regex::Regex;
use std::{collections::HashMap, sync::LazyLock};

fn invalid(message: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(message.into())
}

/// Check tag keys and values, then limits on the unique set (last value wins).
pub fn validate_tags(tags: &[Tag]) -> Result<(), DynamoDbError> {
    static CHARACTERS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\A[\p{L}\p{N}\p{Z}_.:/=+\-]*\z").expect("constant tag expression")
    });
    let mut merged = HashMap::new();
    for tag in tags {
        let key_len = tag.key.chars().count();
        if !(1..=128).contains(&key_len) {
            return Err(invalid(
                "Tag Key length must be between 1 and 128 characters",
            ));
        }
        if tag.value.chars().count() > 256 {
            return Err(invalid("Tag Value length must not exceed 256 characters"));
        }
        if !CHARACTERS.is_match(&tag.key) || !CHARACTERS.is_match(&tag.value) {
            return Err(invalid("Tag keys and values contain invalid characters"));
        }
        if tag.key.starts_with("aws:") {
            return Err(invalid("Tag keys beginning with aws: are reserved"));
        }
        merged.insert(tag.key.as_str(), tag.value.as_str());
    }
    if merged.len() > 50 {
        return Err(invalid("A resource may have at most 50 tags"));
    }
    if merged.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() > 10 * 1024 {
        return Err(invalid("The total size of tags exceeds 10 KB"));
    }
    Ok(())
}

/// Validate required tag members before typed deserialization. This preserves
/// ValidationException for missing members without hiding wrong JSON types.
pub fn validate_tag_members(body: &serde_json::Value, required: bool) -> Result<(), DynamoDbError> {
    let tags = body.get("Tags");
    if required && tags.is_none_or(serde_json::Value::is_null) {
        return Err(invalid("Tags is required"));
    }
    if let Some(tags) = tags.and_then(serde_json::Value::as_array) {
        for tag in tags {
            if let Some(tag) = tag.as_object() {
                for member in ["Key", "Value"] {
                    if tag.get(member).is_none_or(serde_json::Value::is_null) {
                        return Err(invalid(&format!("Tag {member} is required")));
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tag(key: &str, value: &str) -> Tag {
        Tag {
            key: key.into(),
            value: value.into(),
        }
    }
    #[test]
    fn unicode_boundaries_and_empty_values() {
        assert!(
            validate_tags(&[
                tag(&"字".repeat(128), &"א".repeat(256)),
                tag("space +-=._:/", "")
            ])
            .is_ok()
        );
        for t in [
            tag("", "value"),
            tag(&"字".repeat(129), ""),
            tag("key", &"א".repeat(257)),
            tag("aws:reserved", ""),
            tag("emoji😀", ""),
            tag("tab\t", ""),
            tag("key", "no!"),
        ] {
            assert!(validate_tags(&[t]).is_err());
        }
    }
    #[test]
    fn aggregate_limits_apply_after_overwrites() {
        let mut tags: Vec<_> = (0..50).map(|i| tag(&format!("k{i}"), "v")).collect();
        assert!(validate_tags(&tags).is_ok());
        tags.push(tag("k0", "updated"));
        assert!(validate_tags(&tags).is_ok());
        tags.push(tag("extra", "v"));
        assert!(validate_tags(&tags).is_err());
        let tags: Vec<_> = (0..50)
            .map(|i| tag(&format!("k{i}"), &"x".repeat(256)))
            .collect();
        assert!(validate_tags(&tags).is_err());
    }
    #[test]
    fn missing_members_remain_distinct_from_wrong_types() {
        use serde_json::json;
        assert!(validate_tag_members(&json!({}), false).is_ok());
        for body in [
            json!({}),
            json!({"Tags":null}),
            json!({"Tags":[{"Key":"k"}]}),
            json!({"Tags":[{"Value":"v"}]}),
        ] {
            assert!(validate_tag_members(&body, true).is_err());
        }
        assert!(validate_tag_members(&json!({"Tags":[{"Key":123,"Value":"v"}]}), true).is_ok());
    }
}
