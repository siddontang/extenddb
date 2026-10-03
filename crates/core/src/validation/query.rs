// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query schema validation before storage lookup. Both legacy KeyConditions and
//! expression syntax enter through the same AST, including multipart keys.
use crate::{
    error::DynamoDbError,
    expression::{Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition},
    types::{AttributeDefinition, AttributeValue, KeySchemaElement, KeyType},
};
use std::collections::HashSet;

fn invalid(message: impl Into<String>) -> DynamoDbError {
    DynamoDbError::ValidationException(message.into())
}

/// Check that every key condition names one distinct key of the selected table
/// or index and compares it with a constant of its declared scalar type.
pub fn validate_query_keys(
    condition: &KeyCondition,
    maps: &ExpressionMaps,
    schema: &[KeySchemaElement],
    definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let mut seen = HashSet::new();
    let mut check = |path: &[PathElement],
                     values: &[&Expr],
                     kind: KeyType,
                     begins: bool|
     -> Result<(), DynamoDbError> {
        let [PathElement::Attribute(name)] = path else {
            return Err(invalid(
                "Query key condition not supported: invalid key path",
            ));
        };
        let name = if let Some(reference) = name.strip_prefix('#') {
            maps.resolve_name(reference)?
        } else {
            name.as_str()
        };
        if !seen.insert(name.to_owned()) {
            return Err(invalid(format!(
                "KeyConditionExpressions must only contain one condition per key: {name}"
            )));
        }
        if !schema
            .iter()
            .any(|key| key.attribute_name == name && key.key_type == kind)
        {
            return Err(invalid(format!(
                "Query key condition not supported for key {name}"
            )));
        }
        for expr in values {
            let Expr::Placeholder(reference) = expr else {
                return Err(invalid("Query key condition requires a constant value"));
            };
            let value = maps.resolve_value_for(reference, "KeyConditionExpression")?;
            super::validate_key_attribute_type(name, value, definitions)?;
            if begins && !matches!(value, AttributeValue::S(_) | AttributeValue::B(_)) {
                return Err(invalid("BEGINS_WITH is not supported for numeric keys"));
            }
        }
        Ok(())
    };
    check(
        &condition.pk_path,
        &[&condition.pk_value],
        KeyType::Hash,
        false,
    )?;
    for (path, value) in &condition.extra_pk_conditions {
        check(path, &[value], KeyType::Hash, false)?;
    }
    if let Some(sk) = &condition.sk_condition {
        match sk {
            SortKeyCondition::Compare { path, value, .. } => {
                check(path, &[value], KeyType::Range, false)?
            }
            SortKeyCondition::Between { path, low, high } => {
                check(path, &[low, high], KeyType::Range, false)?
            }
            SortKeyCondition::BeginsWith { path, prefix } => {
                check(path, &[prefix], KeyType::Range, true)?
            }
        }
    }
    for (path, value) in &condition.extra_sk_conditions {
        check(path, &[value], KeyType::Range, false)?;
    }
    Ok(())
}

/// Reject filter references to keys of the selected access path. Applies to
/// every operand (not only the left side), and to legacy QueryFilter as well.
pub fn validate_query_filter(
    expr: &Expr,
    maps: &ExpressionMaps,
    schema: &[KeySchemaElement],
) -> Result<(), DynamoDbError> {
    match expr {
        Expr::Path(path) => {
            if let Some(PathElement::Attribute(name)) = path.first() {
                let name = if let Some(reference) = name.strip_prefix('#') {
                    maps.resolve_name(reference)?
                } else {
                    name.as_str()
                };
                if schema.iter().any(|key| key.attribute_name == name) {
                    return Err(invalid(format!(
                        "Filter Expression can only contain non-primary key attributes: Primary key attribute: {name}"
                    )));
                }
            }
        }
        Expr::And(a, b)
        | Expr::Or(a, b)
        | Expr::Compare {
            left: a, right: b, ..
        }
        | Expr::Arithmetic {
            left: a, right: b, ..
        } => {
            validate_query_filter(a, maps, schema)?;
            validate_query_filter(b, maps, schema)?;
        }
        Expr::Not(a) => validate_query_filter(a, maps, schema)?,
        Expr::Function { args, .. } => {
            for arg in args {
                validate_query_filter(arg, maps, schema)?;
            }
        }
        Expr::Between { operand, low, high } => {
            for arg in [operand, low, high] {
                validate_query_filter(arg, maps, schema)?;
            }
        }
        Expr::In { operand, list } => {
            validate_query_filter(operand, maps, schema)?;
            for arg in list {
                validate_query_filter(arg, maps, schema)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{parse_condition, parse_key_condition, tokenize};
    #[test]
    fn query_schema_and_filter_checks_do_not_need_rows() {
        let schema: Vec<KeySchemaElement> = serde_json::from_value(serde_json::json!([{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}])).unwrap();
        let defs = serde_json::from_value::<Vec<AttributeDefinition>>(serde_json::json!([{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"}])).unwrap();
        let mut maps = ExpressionMaps::default();
        maps.values
            .insert("p".into(), AttributeValue::S("partition".into()));
        maps.values
            .insert("s".into(), AttributeValue::N("1".into()));
        for text in ["pk = :p", "pk = :p AND sk >= :s"] {
            let condition = parse_key_condition(&tokenize(text).unwrap()).unwrap();
            assert!(validate_query_keys(&condition, &maps, &schema, &defs).is_ok());
        }
        for text in [
            "pk = :s",
            "pk = :p AND sk > :p",
            "pk = :p AND pk = :p",
            "pk = :p AND other = :s",
            "pk = :p AND begins_with(sk, :s)",
        ] {
            let condition = parse_key_condition(&tokenize(text).unwrap()).unwrap();
            assert!(
                validate_query_keys(&condition, &maps, &schema, &defs).is_err(),
                "{text}"
            );
        }
        maps.names.insert("alias".into(), "sk".into());
        for text in [
            "attribute_exists(pk)",
            "other = sk",
            "other = :p OR #alias = :s",
        ] {
            let filter = parse_condition(&tokenize(text).unwrap()).unwrap();
            assert!(validate_query_filter(&filter, &maps, &schema).is_err());
        }
    }
}
