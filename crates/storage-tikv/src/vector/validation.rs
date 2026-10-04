// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pure state-dependent UpdateTable rules. Request-only shape validation lives
//! in core; this module validates the current catalog snapshot before any index
//! or billing mutation. The caller's transaction protects that snapshot.
use super::VectorIndex;
use extenddb_core::types::*;
use extenddb_storage::error::StorageError;

pub(crate) struct Current<'a> {
    pub billing: BillingMode,
    pub keys: &'a [KeySchemaElement],
    pub attributes: &'a [AttributeDefinition],
    pub indexes: &'a [VectorIndex],
}

pub(crate) fn update(current: Current<'_>, input: &UpdateTableInput) -> Result<(), StorageError> {
    let updates = input.vector_index_updates.as_deref().unwrap_or_default();
    let creates: Vec<_> = updates.iter().filter_map(|u| u.create.as_ref()).collect();
    if input.billing_mode == Some(BillingMode::Provisioned) && !current.indexes.is_empty() {
        return Err(StorageError::Validation(
            if updates.is_empty() {
                VECTOR_INDEX_REQUIRES_PAY_PER_REQUEST
            } else {
                VECTOR_TABLE_REQUIRES_PAY_PER_REQUEST_MODE
            }
            .into(),
        ));
    }
    if !creates.is_empty()
        && input.billing_mode.unwrap_or(current.billing) != BillingMode::PayPerRequest
    {
        return Err(StorageError::Validation(
            VECTOR_INDEX_REQUIRES_PAY_PER_REQUEST.into(),
        ));
    }
    let deletes = updates.iter().filter(|u| u.delete.is_some()).count();
    if (current.indexes.len() + creates.len()).saturating_sub(deletes)
        > MAX_VECTOR_INDEXES_PER_TABLE
    {
        return Err(StorageError::LimitExceeded(
            VECTOR_INDEX_COUNT_LIMIT_UPDATE.into(),
        ));
    }
    for spec in creates {
        let attribute = &spec.vector_attribute.attribute_name;
        if let Some(key) = current.keys.iter().find(|k| &k.attribute_name == attribute) {
            let scalar = current
                .attributes
                .iter()
                .find(|a| &a.attribute_name == attribute)
                .map_or("S", |a| match a.attribute_type {
                    ScalarAttributeType::S => "S",
                    ScalarAttributeType::N => "N",
                    ScalarAttributeType::B => "B",
                });
            let kind = match key.key_type {
                KeyType::Hash => "HASH",
                KeyType::Range => "RANGE",
            };
            return Err(StorageError::Validation(vector_attribute_redefines_key(
                attribute,
                scalar,
                kind,
                spec.dimensions,
            )));
        }
        if let Some(existing) = current.indexes.iter().find(|i| {
            i.spec.vector_attribute.attribute_name == *attribute
                && i.spec.dimensions != spec.dimensions
        }) {
            return Err(StorageError::Validation(vector_attribute_redefines_vector(
                attribute,
                existing.spec.dimensions,
                spec.dimensions,
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn spec(name: &str, attribute: &str, dimensions: u32) -> VectorIndex {
        VectorIndex::new(serde_json::from_value(serde_json::json!({
            "IndexName":name, "VectorAttribute":{"AttributeName":attribute},
            "Dimensions":dimensions, "DistanceFunction":"COSINE", "Projection":{"ProjectionType":"ALL"}
        })).unwrap(), false).unwrap()
    }
    fn create(attribute: &str, dimensions: u32) -> UpdateTableInput {
        serde_json::from_value(
            serde_json::json!({"TableName":"table", "VectorIndexUpdates":[{
                "Create": spec("new", attribute, dimensions).spec
            }]}),
        )
        .unwrap()
    }
    fn state(indexes: &[VectorIndex], billing: BillingMode) -> Current<'_> {
        Current {
            billing,
            keys: &[],
            attributes: &[],
            indexes,
        }
    }
    #[test]
    fn billing_uses_effective_mode_and_preserves_existing_index_rules() {
        let indexes = [spec("old", "emb", 2)];
        let mut input: UpdateTableInput = serde_json::from_value(serde_json::json!({
            "TableName":"table", "BillingMode":"PROVISIONED"
        }))
        .unwrap();
        assert!(
            matches!(update(state(&indexes, BillingMode::PayPerRequest), &input), Err(StorageError::Validation(m)) if m == VECTOR_INDEX_REQUIRES_PAY_PER_REQUEST)
        );
        input.vector_index_updates = Some(vec![
            serde_json::from_value(serde_json::json!({"Delete":{"IndexName":"old"}})).unwrap(),
        ]);
        assert!(
            matches!(update(state(&indexes, BillingMode::PayPerRequest), &input), Err(StorageError::Validation(m)) if m == VECTOR_TABLE_REQUIRES_PAY_PER_REQUEST_MODE)
        );
        let mut input = create("emb", 2);
        assert!(update(state(&[], BillingMode::Provisioned), &input).is_err());
        input.billing_mode = Some(BillingMode::PayPerRequest);
        assert!(update(state(&[], BillingMode::Provisioned), &input).is_ok());
    }
    #[test]
    fn count_checks_the_boundary_and_net_effect() {
        let indexes: Vec<_> = (0..5).map(|i| spec(&format!("v{i}"), "emb", 2)).collect();
        let mut input = create("emb", 2);
        assert!(update(state(&indexes[..4], BillingMode::PayPerRequest), &input).is_ok());
        assert!(
            matches!(update(state(&indexes, BillingMode::PayPerRequest), &input), Err(StorageError::LimitExceeded(m)) if m == VECTOR_INDEX_COUNT_LIMIT_UPDATE)
        );
        // Core separately refuses multiple online actions. The state rule itself
        // evaluates net count instead of depending on the order of actions.
        input.vector_index_updates.as_mut().unwrap().push(
            serde_json::from_value(serde_json::json!({"Delete":{"IndexName":"v0"}})).unwrap(),
        );
        assert!(update(state(&indexes, BillingMode::PayPerRequest), &input).is_ok());
    }
    #[test]
    fn scalar_keys_and_existing_vector_dimensions_cannot_be_redefined() {
        let keys = [KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }];
        let attributes = [AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: ScalarAttributeType::N,
        }];
        assert!(
            matches!(update(Current { keys: &keys, attributes: &attributes, ..state(&[], BillingMode::PayPerRequest) }, &create("pk", 2)), Err(StorageError::Validation(m)) if m == vector_attribute_redefines_key("pk", "N", "HASH", 2))
        );
        let indexes = [spec("old", "emb", 2)];
        assert!(
            update(
                state(&indexes, BillingMode::PayPerRequest),
                &create("emb", 2)
            )
            .is_ok()
        );
        assert!(
            matches!(update(state(&indexes, BillingMode::PayPerRequest), &create("emb", 3)), Err(StorageError::Validation(m)) if m == vector_attribute_redefines_vector("emb", 2, 3))
        );
    }
}
