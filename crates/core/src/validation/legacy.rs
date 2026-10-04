// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Validate legacy read projections and condition operands before desugaring.
//! Legacy IN and CONTAINS accept a narrower set of constants than expressions;
//! keeping this boundary here avoids changing the modern evaluator's semantics.
use crate::{error::DynamoDbError, types::AttributeValue};
use std::cmp::Ordering;

/// Legacy projection members are literal attribute names, not document paths.
/// Check duplicates before compiling the projection or fetching data, including
/// when no requested item exists. Keep this separate from expression overlap
/// validation: `a` and `a.b` are distinct literal names in AttributesToGet.
pub fn validate_attributes_to_get(names: Option<&[String]>) -> Result<(), DynamoDbError> {
    let mut seen = std::collections::HashSet::new();
    for name in names.unwrap_or_default() {
        if !seen.insert(name) {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: Duplicate value in attribute name: {name}"
            )));
        }
    }
    Ok(())
}

/// Check operator spelling, arity and constant types without reading an item.
pub fn validate_legacy_comparison(
    op: &str,
    values: &[AttributeValue],
) -> Result<(), DynamoDbError> {
    let invalid = || {
        DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Invalid AttributeValueList: incorrect operand type, count or bounds for ComparisonOperator {op}"
        ))
    };
    let count_ok = match op {
        "NULL" | "NOT_NULL" => values.is_empty(),
        "BETWEEN" => values.len() == 2,
        "IN" => (1..=100).contains(&values.len()),
        "EQ" | "NE" | "LT" | "LE" | "GT" | "GE" | "CONTAINS" | "NOT_CONTAINS" | "BEGINS_WITH" => {
            values.len() == 1
        }
        _ => return Err(invalid()),
    };
    if !count_ok {
        return Err(invalid());
    }
    let scalar = |v: &AttributeValue| {
        matches!(
            v,
            AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_)
        )
    };
    if matches!(
        op,
        "LT" | "LE" | "GT" | "GE" | "BETWEEN" | "IN" | "CONTAINS" | "NOT_CONTAINS"
    ) && !values.iter().all(scalar)
    {
        return Err(invalid());
    }
    if op == "BEGINS_WITH" && !matches!(values[0], AttributeValue::S(_) | AttributeValue::B(_)) {
        return Err(invalid());
    }
    if matches!(op, "IN" | "BETWEEN")
        && values
            .iter()
            .any(|v| std::mem::discriminant(v) != std::mem::discriminant(&values[0]))
    {
        return Err(invalid());
    }
    if op == "BETWEEN" {
        let order = match (&values[0], &values[1]) {
            (AttributeValue::S(a), AttributeValue::S(b)) => a.cmp(b),
            (AttributeValue::B(a), AttributeValue::B(b)) => a.cmp(b),
            (AttributeValue::N(a), AttributeValue::N(b)) => {
                let a = super::number::validate_and_normalize_number(a)?;
                let b = super::number::validate_and_normalize_number(b)?;
                a.parse::<bigdecimal::BigDecimal>()
                    .map_err(|_| invalid())?
                    .cmp(&b.parse::<bigdecimal::BigDecimal>().map_err(|_| invalid())?)
            }
            _ => return Err(invalid()),
        };
        if order == Ordering::Greater {
            return Err(invalid());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_projection_rejects_duplicates_but_preserves_literal_names() {
        let names = ["a", "a.b", "a[0]", "名字"].map(str::to_owned);
        assert!(validate_attributes_to_get(Some(&names)).is_ok());
        assert!(validate_attributes_to_get(None).is_ok());
        for duplicate in ["a", "名字"] {
            let names = [duplicate, "unrelated", duplicate].map(str::to_owned);
            assert!(matches!(validate_attributes_to_get(Some(&names)),
                Err(DynamoDbError::ValidationException(message)) if message.ends_with(duplicate)));
        }
    }
    #[test]
    fn arity_and_constant_types_are_checked_without_rows() {
        use AttributeValue::*;
        for (op, values) in [
            ("NULL", vec![S("x".into())]),
            ("NOT_NULL", vec![Null]),
            ("LE", vec![L(vec![])]),
            ("CONTAINS", vec![Bool(true)]),
            ("IN", vec![S("x".into()), N("1".into())]),
            ("BETWEEN", vec![N("10".into()), N("2".into())]),
            ("BEGINS_WITH", vec![N("1".into())]),
            ("eq", vec![S("x".into())]),
        ] {
            assert!(validate_legacy_comparison(op, &values).is_err(), "{op}");
        }
        assert!(validate_legacy_comparison("IN", &[N("1".into()), N("10".into())]).is_ok());
        assert!(validate_legacy_comparison("BETWEEN", &[N("2".into()), N("10".into())]).is_ok());
        assert!(validate_legacy_comparison("NULL", &[]).is_ok());
    }
}
