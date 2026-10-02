// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned, prefix-free tuple encoding for an ordered byte-key store.
//!
//! A zero byte is escaped as `00 ff`; `00 00` terminates a component. This
//! preserves unsigned byte ordering, distinguishes prefixes, and cannot
//! collide when names contain separators or NUL. Prefix scans operate on
//! *complete* tuple components; [`prefix_end`] returns their exclusive bound.
//! Numeric components use canonical arbitrary-precision decimal encodings,
//! adapted from the SQLite backend; numbers never pass through an `f64`.

use base64::Engine;
use bigdecimal::BigDecimal;
use extenddb_core::types::{AttributeValue, Item, KeySchemaElement};
use extenddb_storage::error::StorageError;

/// Append an order-preserving, self-delimiting binary component.
pub fn component(out: &mut Vec<u8>, value: &[u8]) {
    for &b in value {
        out.push(b);
        if b == 0 {
            out.push(255);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// Encode string components; suitable for metadata and namespace keys.
pub fn tuple(parts: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        component(&mut out, p.as_bytes());
    }
    out
}

/// Exclusive upper bound of a prefix, or `None` for an unbounded suffix.
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 255 {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Encode a scalar key. Numeric equality is canonical (`1 == 1.0`).
pub fn scalar(value: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    match value {
        AttributeValue::S(s) => Ok(s.as_bytes().to_vec()),
        AttributeValue::B(s) => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|_| StorageError::Validation("Invalid binary key".into())),
        AttributeValue::N(s) => {
            let n: BigDecimal = s
                .parse()
                .map_err(|_| StorageError::Validation("Invalid numeric key".into()))?;
            Ok(number(&n).into_bytes())
        }
        _ => Err(StorageError::Validation(
            "Key must be a string, number or binary".into(),
        )),
    }
}

/// Append key attributes in schema order. Missing keys are rejected.
pub fn item_key(
    out: &mut Vec<u8>,
    item: &Item,
    schema: &[KeySchemaElement],
) -> Result<(), StorageError> {
    for k in schema {
        let v = item
            .get(&k.attribute_name)
            .ok_or_else(|| StorageError::Validation(format!("Missing key {}", k.attribute_name)))?;
        component(out, &scalar(v)?);
    }
    Ok(())
}

/// Exact, order-preserving decimal representation for validated DynamoDB numbers.
pub fn number(value: &BigDecimal) -> String {
    use bigdecimal::Zero;
    if value.is_zero() {
        return "1".into();
    }
    let negative = value < &BigDecimal::zero();
    let (mantissa, scale) = value.abs().normalized().as_bigint_and_exponent();
    let digits = mantissa.to_string();
    let exponent = digits.len() as i64 - scale;
    if negative {
        let inverted: String = digits
            .bytes()
            .map(|b| char::from(b'9' - (b - b'0')))
            .collect();
        format!("0{:06}{inverted}:", 100_000 - exponent)
    } else {
        format!("2{:06}{digits}", 100_000 + exponent)
    }
}

/// Decode exactly one escaped tuple component, rejecting corrupt/trailing data.
pub fn decode_component(bytes: &[u8]) -> Result<Vec<u8>, extenddb_storage::error::StorageError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        i += 1;
        if b != 0 {
            out.push(b);
            continue;
        }
        if i == bytes.len() {
            break;
        }
        let next = bytes[i];
        i += 1;
        match next {
            255 => out.push(0),
            0 if i == bytes.len() => return Ok(out),
            _ => break,
        }
    }
    Err(extenddb_storage::error::StorageError::Internal(
        "Corrupt tuple component".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn components_preserve_binary_order(a in prop::collection::vec(any::<u8>(),0..80), b in prop::collection::vec(any::<u8>(),0..80)) {
            let (mut x,mut y)=(vec![],vec![]); component(&mut x,&a); component(&mut y,&b);
            prop_assert_eq!(a.cmp(&b),x.cmp(&y));
        }
        #[test]
        fn decimal_order(a in any::<i64>(), b in any::<i64>(), sa in 0i64..40, sb in 0i64..40) {
            let a=BigDecimal::new(a.into(),sa); let b=BigDecimal::new(b.into(),sb);
            prop_assert_eq!(a.cmp(&b),number(&a).cmp(&number(&b)));
        }
    }
    #[test]
    fn tuples_are_unambiguous() {
        assert_ne!(tuple(&["a", "bc"]), tuple(&["ab", "c"]));
        assert_ne!(tuple(&["a\0", "b"]), tuple(&["a", "\0b"]));
        assert!(tuple(&["a", "x"]).starts_with(&tuple(&["a"])));
        assert_eq!(prefix_end(&[1, 255]), Some(vec![2]));
        assert_eq!(prefix_end(&[255]), None);
    }
    #[test]
    fn decimal_canonical_and_extreme() {
        assert_eq!(
            number(&"1.000".parse().unwrap()),
            number(&"1e0".parse().unwrap())
        );
        let values = [
            "-9.9999999999999999999999999999999999999e125",
            "-1",
            "-0.001",
            "0",
            "1e-130",
            "1.0000000000000000000000000000000000001",
            "9.9999999999999999999999999999999999999e125",
        ];
        let encoded: Vec<_> = values.iter().map(|s| number(&s.parse().unwrap())).collect();
        assert!(encoded.windows(2).all(|w| w[0] < w[1]));
    }
}
