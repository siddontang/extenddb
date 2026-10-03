// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared parsing utilities for condition and update expression parsers.

use super::ast::PathElement;
use super::tokenizer::Token;
use crate::error::DynamoDbError;

/// Parse a document path from a token stream.
///
/// Handles `ident`, `#name`, `.attr`, and `[index]` syntax.
/// Used by both the condition expression parser and the update expression parser.
pub fn parse_path(tokens: &[Token], pos: &mut usize) -> Result<Vec<PathElement>, DynamoDbError> {
    let mut elements = Vec::new();

    match &tokens.get(*pos) {
        Some(Token::Ident(name))
            if name.as_bytes().first().is_some_and(u8::is_ascii_alphabetic) =>
        {
            elements.push(PathElement::Attribute(name.clone()));
            *pos += 1;
        }
        Some(Token::NameRef(name)) => {
            elements.push(PathElement::Attribute(format!("#{name}")));
            *pos += 1;
        }
        _ => {
            return Err(validation_err("expected attribute name"));
        }
    }

    while *pos < tokens.len() {
        if tokens[*pos] == Token::Dot {
            *pos += 1;
            match &tokens.get(*pos) {
                Some(Token::Ident(name))
                    if name.as_bytes().first().is_some_and(u8::is_ascii_alphabetic) =>
                {
                    elements.push(PathElement::Attribute(name.clone()));
                    *pos += 1;
                }
                Some(Token::NameRef(name)) => {
                    elements.push(PathElement::Attribute(format!("#{name}")));
                    *pos += 1;
                }
                _ => {
                    return Err(validation_err("expected attribute name after '.'"));
                }
            }
        } else if tokens[*pos] == Token::LBracket {
            *pos += 1;
            if let Some(Token::Ident(idx_str)) = tokens.get(*pos) {
                let idx: usize = idx_str.parse().map_err(|_| {
                    validation_err("Syntax error: expected numeric index in brackets")
                })?;
                elements.push(PathElement::Index(idx));
                *pos += 1;
            } else {
                return Err(validation_err(
                    "Syntax error: expected numeric index in brackets",
                ));
            }
            expect_token(tokens, pos, &Token::RBracket, "]", "expression")?;
        } else {
            break;
        }
    }

    if elements.len() > 32 {
        return Err(validation_err(
            "The document path has too many nesting levels",
        ));
    }
    Ok(elements)
}

/// Expect a specific token at the current position.
pub fn expect_token(
    tokens: &[Token],
    pos: &mut usize,
    expected: &Token,
    label: &str,
    context: &str,
) -> Result<(), DynamoDbError> {
    if *pos >= tokens.len() || tokens[*pos] != *expected {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid {context}: expected '{label}'"
        )));
    }
    *pos += 1;
    Ok(())
}

/// Reject redundant parentheses: a parenthesised group whose
/// entire content is itself a single parenthesised group, such as `((x))`.
/// Returns the bare error body so each parser can prefix its own expression type.
pub fn check_redundant_parens(tokens: &[Token]) -> Result<(), String> {
    // Single-pass stack: one bool per unmatched '('.
    // true = the preceding token was also '(' (double-open).
    // On ')', if popped is true AND the next token is ')', the group is redundant.
    let mut stack: Vec<bool> = Vec::new();

    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen => {
                stack.push(i > 0 && tokens[i - 1] == Token::LParen);
            }
            Token::RParen => {
                if let Some(true) = stack.pop()
                    && tokens.get(i + 1) == Some(&Token::RParen)
                {
                    return Err("The expression has redundant parentheses;".to_owned());
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validation_err(msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("Invalid expression: {msg}"))
}

/// Check nesting iteratively before entering a recursive update parser. Count
/// unmatched opens as well, so malformed input cannot overflow before a syntax
/// error is discovered. Width (many sibling calls) does not consume depth.
pub(super) fn check_parenthesis_depth(
    tokens: &[Token],
    max_depth: usize,
    kind: &str,
) -> Result<(), DynamoDbError> {
    let mut depth: usize = 0;
    for token in tokens {
        match token {
            Token::LParen => {
                depth += 1;
                if depth > max_depth {
                    return Err(DynamoDbError::ValidationException(format!(
                        "Invalid {kind}: expression nesting depth exceeded"
                    )));
                }
            }
            Token::RParen => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}
