// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pure Streams request validation, shared by every storage backend.
//!
//! Syntax errors are rejected before storage lookup, so malformed identifiers
//! cannot turn into backend-specific not-found or internal errors. Existence
//! and ownership remain storage responsibilities. Sequence positions use decimal
//! strings rather than machine integers (the API permits up to 40 digits).
use crate::{error::DynamoDbError, types::ShardIteratorType};

fn invalid(message: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(message.into())
}

/// Validate the DynamoDB stream ARN structure without assuming a partition,
/// deployment region, or a particular backend's stream label format.
pub fn validate_stream_arn(arn: &str) -> Result<(), DynamoDbError> {
    let parts: Vec<_> = arn.splitn(6, ':').collect();
    let valid = (37..=1024).contains(&arn.len())
        && parts.len() == 6
        && parts[0] == "arn"
        && !parts[1].is_empty()
        && parts[2] == "dynamodb"
        && !parts[3].is_empty()
        && parts[4].len() == 12
        && parts[4].bytes().all(|b| b.is_ascii_digit())
        && parts[5].strip_prefix("table/").is_some_and(|resource| {
            resource
                .split_once("/stream/")
                .is_some_and(|(table, label)| {
                    !table.is_empty()
                        && !table.contains('/')
                        && !label.is_empty()
                        && !label.contains('/')
                })
        });
    if valid {
        Ok(())
    } else {
        Err(invalid("Invalid StreamArn"))
    }
}

/// Validate an explicitly supplied page limit; absence selects the API default.
pub fn validate_stream_limit(limit: Option<i64>, max: i64) -> Result<(), DynamoDbError> {
    if limit.is_some_and(|n| n < 1 || n > max) {
        return Err(invalid(&format!("Limit must be between 1 and {max}")));
    }
    Ok(())
}

/// Check the iterator's required/forbidden sequence parameter and decimal form.
pub fn validate_iterator_sequence(
    kind: ShardIteratorType,
    sequence: Option<&str>,
) -> Result<(), DynamoDbError> {
    match kind {
        ShardIteratorType::TrimHorizon | ShardIteratorType::Latest => {
            if sequence.is_some() {
                return Err(invalid(
                    "SequenceNumber must not be specified for LATEST or TRIM_HORIZON",
                ));
            }
        }
        ShardIteratorType::AtSequenceNumber | ShardIteratorType::AfterSequenceNumber => {
            let s = sequence.ok_or_else(|| {
                invalid(
                    "SequenceNumber is required for AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER",
                )
            })?;
            if !(21..=40).contains(&s.len()) || !s.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid(
                    "Invalid SequenceNumber: expected 21 to 40 decimal digits",
                ));
            }
        }
    }
    Ok(())
}

/// Convert a sequence to a backend's sortable width, optionally subtracting one
/// for inclusive AT semantics. Zero's predecessor is the trim-horizon sentinel.
pub fn sequence_position(raw: &str, width: usize, inclusive: bool) -> String {
    let mut digits = raw.trim_start_matches('0').as_bytes().to_vec();
    if inclusive {
        if digits.is_empty() {
            return String::new();
        }
        for digit in digits.iter_mut().rev() {
            if *digit > b'0' {
                *digit -= 1;
                break;
            }
            *digit = b'9';
        }
    }
    let s = String::from_utf8(digits).expect("validated decimal sequence");
    let s = s.trim_start_matches('0');
    format!("{:0>width$}", if s.is_empty() { "0" } else { s })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arn_structure_and_limits() {
        for partition in ["aws", "aws-cn", "aws-us-gov"] {
            assert!(
                validate_stream_arn(&format!(
                    "arn:{partition}:dynamodb:local:123456789012:table/foo/stream/label"
                ))
                .is_ok()
            );
        }
        for arn in [
            "invalid",
            "arn:aws:dynamodb:local:123456789012:table/foo",
            "arn:aws:dynamodb:local:123:table/foo/stream/label",
            "arn:aws:dynamodb:local:123456789012:table/foo/stream/",
        ] {
            assert!(validate_stream_arn(arn).is_err(), "{arn}");
        }
        for max in [100, 1000] {
            for n in [None, Some(1), Some(max)] {
                assert!(validate_stream_limit(n, max).is_ok());
            }
            for n in [-1, 0, max + 1] {
                assert!(validate_stream_limit(Some(n), max).is_err());
            }
        }
    }
    #[test]
    fn sequence_presence_and_decimal_bounds() {
        use ShardIteratorType::*;
        for kind in [Latest, TrimHorizon] {
            assert!(validate_iterator_sequence(kind, None).is_ok());
            assert!(validate_iterator_sequence(kind, Some(&"0".repeat(21))).is_err());
        }
        for kind in [AtSequenceNumber, AfterSequenceNumber] {
            assert!(validate_iterator_sequence(kind, None).is_err());
            for n in [21, 40] {
                assert!(validate_iterator_sequence(kind, Some(&"9".repeat(n))).is_ok());
            }
            for s in [
                "1".repeat(20),
                "1".repeat(41),
                format!("{}x", "1".repeat(20)),
            ] {
                assert!(validate_iterator_sequence(kind, Some(&s)).is_err());
            }
        }
    }
    #[test]
    fn positions_do_not_overflow_machine_integers() {
        assert_eq!(
            sequence_position("1000000000000000000000000000000000000000", 21, true),
            "999999999999999999999999999999999999999"
        );
        assert_eq!(sequence_position("0005", 6, true), "000004");
        assert_eq!(sequence_position("0005", 6, false), "000005");
        assert_eq!(sequence_position("0", 21, true), "");
        assert_eq!(sequence_position("1", 3, true), "000");
    }
}
