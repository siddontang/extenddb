// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Item-independent expression checks.
//!
//! Validate every branch before evaluating conditions or reading rows. Otherwise
//! short-circuiting, absent attributes and empty tables can hide invalid input.
//! Path checks resolve aliases but never look at stored item contents.
use super::{Expr, ExpressionMaps, PathElement, UpdateAction};
use crate::{error::DynamoDbError, types::AttributeValue};

fn invalid(message: impl Into<String>) -> DynamoDbError {
    DynamoDbError::ValidationException(message.into())
}

/// Resolve aliases and reject paths which overlap or require the same parent to
/// be both a list and a map. Siblings and distinct list elements are valid.
pub fn validate_document_paths(
    paths: &[&[PathElement]],
    maps: &ExpressionMaps,
    kind: &str,
) -> Result<(), DynamoDbError> {
    let resolved: Vec<Vec<PathElement>> = paths
        .iter()
        .map(|path| {
            path.iter()
                .map(|el| match el {
                    PathElement::Attribute(name) if name.starts_with('#') => maps
                        .resolve_name(&name[1..])
                        .map(|s| PathElement::Attribute(s.to_owned())),
                    other => Ok(other.clone()),
                })
                .collect()
        })
        .collect::<Result<_, _>>()?;
    for (i, a) in resolved.iter().enumerate() {
        for b in &resolved[i + 1..] {
            let common = a.iter().zip(b).take_while(|(a, b)| a == b).count();
            let reason = if common == a.len().min(b.len()) {
                Some("overlap")
            } else if matches!(
                (&a[common], &b[common]),
                (PathElement::Attribute(_), PathElement::Index(_))
                    | (PathElement::Index(_), PathElement::Attribute(_))
            ) {
                Some("conflict")
            } else {
                None
            };
            if let Some(reason) = reason {
                return Err(invalid(format!(
                    "Invalid {kind}: Two document paths {reason} with each other; path one: {a:?}, path two: {b:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Validate condition function arity, constant type parameters and path-only
/// arguments, independently of whether their target attribute currently exists.
pub fn validate_condition_functions(
    expr: &Expr,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    match expr {
        Expr::Function { name, args } => {
            let arity = match name.as_str() {
                "attribute_exists" | "attribute_not_exists" | "size" => 1,
                "attribute_type" | "begins_with" | "contains" => 2,
                _ => {
                    return Err(invalid(format!(
                        "Function {name} is not allowed in condition expressions"
                    )));
                }
            };
            if args.len() != arity {
                return Err(invalid(format!(
                    "Invalid number of arguments for function {name}: expected {arity}, received {}",
                    args.len()
                )));
            }
            if matches!(name.as_str(), "attribute_exists" | "attribute_not_exists")
                && !matches!(args[0], Expr::Path(_))
            {
                return Err(invalid(format!(
                    "Function {name} requires an attribute path"
                )));
            }
            // Unlike existence tests, size and attribute_type also accept
            // expression values. Invalid size operands are still rejected
            // before evaluation, even in a short-circuited branch.
            if name == "size" {
                let supported = match &args[0] {
                    Expr::Path(_) => true,
                    Expr::Placeholder(reference) => matches!(
                        maps.resolve_value_for(reference, "ConditionExpression")?,
                        AttributeValue::S(_)
                            | AttributeValue::B(_)
                            | AttributeValue::SS(_)
                            | AttributeValue::NS(_)
                            | AttributeValue::BS(_)
                            | AttributeValue::L(_)
                            | AttributeValue::M(_)
                    ),
                    _ => false,
                };
                if !supported {
                    return Err(invalid("Incorrect operand type for function size"));
                }
            }
            if name == "attribute_type" {
                let Expr::Placeholder(placeholder) = &args[1] else {
                    return Err(invalid(
                        "attribute_type second argument must be an expression attribute value",
                    ));
                };
                let value = maps.resolve_value_for(placeholder, "ConditionExpression")?;
                if !matches!(value, AttributeValue::S(s) if ["S","SS","N","NS","B","BS","BOOL","NULL","L","M"].contains(&s.as_str()))
                {
                    return Err(invalid(format!(
                        "Invalid attribute_type argument: {value:?}"
                    )));
                }
            }
            for arg in args {
                validate_condition_functions(arg, maps)?;
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
            validate_condition_functions(a, maps)?;
            validate_condition_functions(b, maps)?;
        }
        Expr::Not(a) => validate_condition_functions(a, maps)?,
        Expr::Between { operand, low, high } => {
            for e in [operand, low, high] {
                validate_condition_functions(e, maps)?;
            }
        }
        Expr::In { operand, list } => {
            validate_condition_functions(operand, maps)?;
            for e in list {
                validate_condition_functions(e, maps)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Validate update targets before a write, including aliases and all action kinds.
pub fn validate_update_paths(
    actions: &[UpdateAction],
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    for action in actions {
        let (value, delete) = match action {
            UpdateAction::Add { value, .. } => (value, false),
            UpdateAction::Delete { value, .. } => (value, true),
            _ => continue,
        };
        if let Expr::Placeholder(reference) = value {
            let value = maps.resolve_value_for(reference, "UpdateExpression")?;
            if !(matches!(
                value,
                AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_)
            ) || (!delete && matches!(value, AttributeValue::N(_))))
            {
                return Err(invalid(
                    "Invalid UpdateExpression: An operand has an incorrect data type",
                ));
            }
        }
    }
    let paths: Vec<&[PathElement]> = actions
        .iter()
        .map(|a| match a {
            UpdateAction::Set { path, .. }
            | UpdateAction::Remove { path }
            | UpdateAction::Add { path, .. }
            | UpdateAction::Delete { path, .. } => path.as_slice(),
        })
        .collect();
    validate_document_paths(&paths, maps, "UpdateExpression")
}

#[cfg(test)]
mod tests {
    use super::super::{parse_condition, parse_projection, tokenize};
    use super::*;
    #[test]
    fn invalid_functions_are_rejected_without_an_item() {
        let mut maps = ExpressionMaps::default();
        maps.values
            .insert("type".into(), AttributeValue::S("DOG".into()));
        for text in [
            "attribute_exists(:type)",
            "size(size(absent)) = :type",
            "attribute_type(absent, :type)",
            "attribute_type(absent, other)",
            "attribute_exists(absent) OR attribute_type(absent, :type)",
            "size(a,b) = :type",
        ] {
            let expr = parse_condition(&tokenize(text).unwrap()).unwrap();
            assert!(
                validate_condition_functions(&expr, &maps).is_err(),
                "{text}"
            );
        }
        maps.values
            .insert("type".into(), AttributeValue::S("M".into()));
        let expr = parse_condition(&tokenize("attribute_type(absent, :type)").unwrap()).unwrap();
        assert!(validate_condition_functions(&expr, &maps).is_ok());
        for text in ["attribute_type(:type, :type)", "size(:type) = :type"] {
            let expr = parse_condition(&tokenize(text).unwrap()).unwrap();
            assert!(validate_condition_functions(&expr, &maps).is_ok());
        }
    }
    #[test]
    fn aliases_overlap_and_container_conflicts_are_checked() {
        let mut maps = ExpressionMaps::default();
        maps.names.insert("alias".into(), "a".into());
        for text in ["a, a.b", "#alias.b, a.b", "a.b, a[1]", "a[1].c, a[1][0]"] {
            let paths = parse_projection(&tokenize(text).unwrap()).unwrap();
            assert!(
                validate_document_paths(
                    &paths.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                    &maps,
                    "ProjectionExpression"
                )
                .is_err()
            );
        }
        for text in ["a.b, a.c", "a[0], a[1]", "a.b[0], a.c.d"] {
            let paths = parse_projection(&tokenize(text).unwrap()).unwrap();
            assert!(
                validate_document_paths(
                    &paths.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                    &maps,
                    "ProjectionExpression"
                )
                .is_ok()
            );
        }
    }
}

#[cfg(test)]
mod syntax_boundaries {
    use super::super::{parse_projection, parse_update, tokenize};
    #[test]
    fn repeated_clauses_function_case_and_path_depth() {
        for source in [
            "SET a=:v SET b=:v",
            "SET a=IF_NOT_EXISTS(a,:v)",
            "SET a=if_not_exists(:v,:v)",
            "SET 123=:v",
            "SET _bare=:v",
        ] {
            assert!(
                parse_update(&tokenize(source).unwrap()).is_err(),
                "{source}"
            );
        }
        assert!(parse_update(&tokenize("SET a=:v REMOVE b ADD c :n DELETE d :s").unwrap()).is_ok());
        assert!(parse_projection(&tokenize(&format!("a{}", ".b".repeat(31))).unwrap()).is_ok());
        assert!(parse_projection(&tokenize(&format!("a{}", ".b".repeat(32))).unwrap()).is_err());
    }
    #[test]
    fn non_ascii_syntax_never_panics() {
        for source in ["字字字字", "abcde😀😀", "a = :v 字字字", "ééééééé"] {
            assert!(tokenize(source).is_err());
        }
    }
}
