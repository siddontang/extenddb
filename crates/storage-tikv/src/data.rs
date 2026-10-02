// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! One mutation pipeline for standalone and transactional DynamoDB writes.
//!
//! Conditions are evaluated against the protected old image. Item, secondary
//! indexes, TTL entries, stream records and idempotency tokens share one commit.
//! An owned mutation makes replay explicit and keeps borrowed request lifetimes
//! out of the KV API. The module never opens nested transactions.

use crate::{TikvEngine, codec, index, kv};
use extenddb_core::{
    expression::{self, Expr, ExpressionMaps, KeyCondition, UpdateAction},
    types::*,
    validation,
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, ItemPairResult, QueryResult, StreamCapture, TransactGetOp,
    TransactWriteOp, error::StorageError,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub(crate) enum Change {
    Put,
    Delete,
    Update(Vec<UpdateAction>),
    Check,
}
#[derive(Clone)]
pub(crate) struct Mutation {
    pub info: TableKeyInfo,
    pub item: Item,
    pub condition: Option<Expr>,
    pub maps: ExpressionMaps,
    pub stream: Option<StreamCapture>,
    pub change: Change,
    pub ccf: ReturnValuesOnConditionCheckFailure,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Token {
    pub fingerprint: String,
    pub created_ms: i64,
}

impl TikvEngine {
    pub(crate) fn item_prefix(&self, id: &str) -> Vec<u8> {
        self.key(&["data", id, "item"])
    }
    pub(crate) fn item_key(
        &self,
        info: &TableKeyInfo,
        item: &Item,
    ) -> Result<Vec<u8>, StorageError> {
        let mut key = self.item_prefix(&info.table_id);
        codec::item_key(&mut key, item, &index::schema_order(&info.base_key_schema))?;
        Ok(key)
    }
    pub(crate) async fn mutate(
        &self,
        tx: &mut dyn kv::Transaction,
        op: &Mutation,
    ) -> Result<(Option<Item>, Option<Item>), StorageError> {
        let table = self.live_table(tx, &op.info).await?;
        let info = table.key_info();
        match op.change {
            Change::Put => validation::validate_item_keys(
                &op.item,
                &info.key_schema,
                &info.attribute_definitions,
            ),
            _ => validation::validate_key_only(
                &op.item,
                &info.key_schema,
                &info.attribute_definitions,
            ),
        }
        .map_err(|e| StorageError::Validation(e.to_string()))?;
        let key = self.item_key(&info, &op.item)?;
        let old: Option<Item> = kv::get(tx, key.clone()).await?;
        if let Some(condition) = &op.condition {
            let empty = Item::new();
            if !expression::evaluate_condition(condition, old.as_ref().unwrap_or(&empty), &op.maps)
                .map_err(|e| StorageError::Validation(e.to_string()))?
            {
                return Err(StorageError::ConditionFailed(old));
            }
        }
        let new = match &op.change {
            Change::Check => return Ok((old.clone(), old)),
            Change::Delete => None,
            Change::Put => Some(op.item.clone()),
            Change::Update(actions) => {
                let mut item = old.clone().unwrap_or_else(|| op.item.clone());
                expression::apply_update_validated(
                    actions,
                    &mut item,
                    &op.maps,
                    &info.vector_indexes,
                    &info.attribute_definitions,
                )
                .map_err(|e| StorageError::Validation(e.to_string()))?;
                Some(item)
            }
        };
        if let Some(item) = &new {
            validation::validate_item_size(item, self.max_item_size)
                .map_err(|e| StorageError::Validation(e.to_string()))?;
            let refs: Vec<_> = table
                .indexes
                .iter()
                .map(|i| validation::IndexKeyRef {
                    index_name: &i.name,
                    key_schema: &i.schema,
                })
                .collect();
            validation::validate_index_keys(item, &refs, &info.attribute_definitions)
                .map_err(|e| StorageError::Validation(e.to_string()))?;
            kv::put(tx, key, item).await?;
        } else {
            kv::delete(tx, key).await?;
        }
        for idx in &table.indexes {
            index::apply(
                self,
                tx,
                &info.table_id,
                idx,
                &info.key_schema,
                old.as_ref(),
                new.as_ref(),
            )
            .await?;
        }
        // TTL candidates are ordered by expiration and include the full base key.
        if let Some(attribute) = &table.ttl_attribute {
            for (image, insert) in [(old.as_ref(), false), (new.as_ref(), true)] {
                if let Some(item) = image
                    && let Some(AttributeValue::N(n)) = item.get(attribute)
                    && let Ok(expiry) = n.parse::<i64>()
                    && expiry >= 0
                {
                    let mut ttl = self.key(&["data", &info.table_id, "ttl"]);
                    ttl.extend_from_slice(&(expiry as u64).to_be_bytes());
                    codec::item_key(&mut ttl, item, &info.key_schema)?;
                    if insert {
                        kv::put(tx, ttl, &extract_key(item, &info.key_schema)).await?;
                    } else {
                        kv::delete(tx, ttl).await?;
                    }
                }
            }
        }
        if old != new {
            self.capture(tx, &table, old.as_ref(), new.as_ref(), op.stream.as_ref())
                .await?;
        }
        Ok((old, new))
    }
    async fn write(&self, op: Mutation) -> Result<(Option<Item>, Option<Item>), StorageError> {
        self.db
            .run(|tx| {
                let (e, op) = (self.clone(), op.clone());
                Box::pin(async move { e.mutate(tx, &op).await })
            })
            .await
    }
}
fn mutation(
    info: &TableKeyInfo,
    item: &Item,
    condition: Option<&Expr>,
    maps: &ExpressionMaps,
    stream: Option<&StreamCapture>,
    change: Change,
) -> Mutation {
    Mutation {
        info: info.clone(),
        item: item.clone(),
        condition: condition.cloned(),
        maps: maps.clone(),
        stream: stream.cloned(),
        change,
        ccf: ReturnValuesOnConditionCheckFailure::None,
    }
}

impl DataEngine for TikvEngine {
    fn put_item(
        &self,
        info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let op = mutation(info, &item, condition, maps, stream, Change::Put);
        Box::pin(async move {
            let (old, _) = self.write(op).await?;
            Ok(if return_old { old } else { None })
        })
    }
    fn delete_item(
        &self,
        info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let op = mutation(info, key, condition, maps, stream, Change::Delete);
        Box::pin(async move {
            let (old, _) = self.write(op).await?;
            Ok(if return_old { old } else { None })
        })
    }
    fn update_item(
        &self,
        info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult> {
        let op = mutation(
            info,
            key,
            condition,
            maps,
            stream,
            Change::Update(actions.to_vec()),
        );
        Box::pin(async move {
            let (old, new) = self.write(op).await?;
            Ok((
                if return_old { old } else { None },
                if return_new { new } else { None },
            ))
        })
    }
    fn get_item(
        &self,
        info: &TableKeyInfo,
        key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let (info, key) = (info.clone(), key.clone());
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, info, key) = (self.clone(), info.clone(), key.clone());
                    Box::pin(async move {
                        e.live_table(tx, &info).await?;
                        kv::get(tx, e.item_key(&info, &key)?).await
                    })
                })
                .await
        })
    }
    fn query(
        &self,
        info: &TableKeyInfo,
        condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        start: Option<&Item>,
        index: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let req = crate::query::Read {
            info: info.clone(),
            condition: Some(condition.clone()),
            maps: maps.clone(),
            forward,
            limit: limit.unwrap_or(128).clamp(1, 1024) as u32,
            start: start.cloned(),
            index: index.map(str::to_owned),
            segment: None,
        };
        Box::pin(self.read(req))
    }
    fn scan(
        &self,
        info: &TableKeyInfo,
        limit: Option<i64>,
        start: Option<&Item>,
        segment: Option<i64>,
        total: Option<i64>,
        index: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let req = crate::query::Read {
            info: info.clone(),
            condition: None,
            maps: ExpressionMaps::default(),
            forward: true,
            limit: limit.unwrap_or(128).clamp(1, 1024) as u32,
            start: start.cloned(),
            index: index.map(str::to_owned),
            segment: segment.zip(total),
        };
        Box::pin(self.read(req))
    }
    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        let ops: Vec<_> = ops
            .iter()
            .map(|o| (o.key_info.clone(), o.key.clone()))
            .collect();
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, ops) = (self.clone(), ops.clone());
                    Box::pin(async move {
                        let mut out = vec![];
                        for (info, key) in ops {
                            e.live_table(tx, &info).await?;
                            validation::validate_key_only(
                                &key,
                                &info.key_schema,
                                &info.attribute_definitions,
                            )
                            .map_err(|e| StorageError::Validation(e.to_string()))?;
                            out.push(kv::get(tx, e.item_key(&info, &key)?).await?);
                        }
                        Ok(out)
                    })
                })
                .await
        })
    }
    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        token: Option<IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let ops: Vec<_> = ops
            .iter()
            .map(|o| {
                let (mut m, ccf) = match o {
                    TransactWriteOp::Put {
                        key_info,
                        item,
                        condition,
                        maps,
                        stream,
                        return_values_on_ccf,
                    } => (
                        mutation(
                            key_info,
                            item,
                            *condition,
                            maps,
                            stream.as_ref(),
                            Change::Put,
                        ),
                        *return_values_on_ccf,
                    ),
                    TransactWriteOp::Delete {
                        key_info,
                        key,
                        condition,
                        maps,
                        stream,
                        return_values_on_ccf,
                    } => (
                        mutation(
                            key_info,
                            key,
                            *condition,
                            maps,
                            stream.as_ref(),
                            Change::Delete,
                        ),
                        *return_values_on_ccf,
                    ),
                    TransactWriteOp::Update {
                        key_info,
                        key,
                        actions,
                        condition,
                        maps,
                        stream,
                        return_values_on_ccf,
                    } => (
                        mutation(
                            key_info,
                            key,
                            *condition,
                            maps,
                            stream.as_ref(),
                            Change::Update(actions.to_vec()),
                        ),
                        *return_values_on_ccf,
                    ),
                    TransactWriteOp::ConditionCheck {
                        key_info,
                        key,
                        condition,
                        maps,
                        return_values_on_ccf,
                    } => (
                        mutation(key_info, key, Some(condition), maps, None, Change::Check),
                        *return_values_on_ccf,
                    ),
                };
                m.ccf = ccf;
                m
            })
            .collect();
        let token = token.map(|t| {
            (
                t.account_id.to_owned(),
                t.token.to_owned(),
                t.fingerprint.to_owned(),
            )
        });
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, ops, token) = (self.clone(), ops.clone(), token.clone());
                    Box::pin(async move {
                        if let Some((a, t, f)) = &token {
                            let key = e.key(&["token", a, t]);
                            if let Some(prior) = kv::get::<Token>(tx, key.clone()).await?
                                && prior.created_ms + 600_000 > e.clock.now_ms()
                            {
                                return Err(if prior.fingerprint == *f {
                                    StorageError::IdempotentReplay
                                } else {
                                    StorageError::IdempotentMismatch
                                });
                            }
                            kv::put(
                                tx,
                                key,
                                &Token {
                                    fingerprint: f.clone(),
                                    created_ms: e.clock.now_ms(),
                                },
                            )
                            .await?;
                        }
                        let mut reasons = vec![];
                        let mut failed = false;
                        for op in ops {
                            match e.mutate(tx, &op).await {
                                Ok(_) => reasons.push(CancellationReason::none()),
                                Err(StorageError::ConditionFailed(old)) => {
                                    failed = true;
                                    reasons.push(
                                        CancellationReason::condition_check_failed_with_item(
                                            if op.ccf == ReturnValuesOnConditionCheckFailure::AllOld
                                            {
                                                old
                                            } else {
                                                None
                                            },
                                        ),
                                    );
                                }
                                Err(StorageError::Validation(message)) => {
                                    failed = true;
                                    reasons.push(CancellationReason::validation_error(message));
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        if failed {
                            Err(StorageError::TransactionCanceled(reasons))
                        } else {
                            Ok(())
                        }
                    })
                })
                .await
        })
    }
    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let e = self.clone();
                    Box::pin(async move {
                        let mut count = 0;
                        for (k, v) in kv::all(tx, e.key(&["token"])).await? {
                            let t: Token = kv::decode(&v)?;
                            if t.created_ms + max_age.saturating_mul(1000) <= e.clock.now_ms() {
                                kv::delete(tx, k).await?;
                                count += 1;
                            }
                        }
                        Ok(count)
                    })
                })
                .await
        })
    }
}
