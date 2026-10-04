// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Explicit transactional change log implementing DynamoDB Streams.
//!
//! Each stream generation has 16 logical shards. A shard counter and its record
//! commit with the item mutation, so readers never advance past an uncommitted
//! lower sequence. This intentionally trades per-shard contention for simple
//! ordering. Stream generations survive disable/delete for retention; shard
//! records carry account ownership independently of live table metadata.

use crate::{TikvEngine, codec, kv, query::stable_hash, table::Table};
use extenddb_core::types::*;
use extenddb_storage::{
    StreamCapture, StreamEngine, StreamListResult, StreamRecordsResult, error::StorageError,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

const SHARDS: usize = 16;
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Stream {
    pub account: String,
    #[serde(default)]
    pub account_generation: String,
    pub arn: String,
    pub label: String,
    pub table_name: String,
    pub schema: Vec<KeySchemaElement>,
    pub view: StreamViewType,
    pub shards: Vec<String>,
    pub enabled: bool,
    pub created_ms: i64,
}
#[derive(Clone, Serialize, Deserialize)]
struct ShardOwner {
    account: String,
    arn: String,
}

impl TikvEngine {
    pub(crate) async fn configure_stream(
        &self,
        tx: &mut dyn kv::Transaction,
        t: &mut Table,
    ) -> Result<(), StorageError> {
        let desired = t
            .description
            .stream_specification
            .as_ref()
            .filter(|s| s.stream_enabled)
            .and_then(|s| s.stream_view_type);
        if let Some(arn) = &t.description.latest_stream_arn
            && let Some(mut old) = kv::get::<Stream>(tx, self.key(&["stream", arn])).await?
        {
            if old.enabled && desired == Some(old.view) {
                return Ok(());
            }
            old.enabled = false;
            kv::put(tx, self.key(&["stream", arn]), &old).await?;
        }
        if let Some(view) = desired {
            let label = format!("{}-{}", self.clock.now_ms(), uuid::Uuid::new_v4());
            let arn = extenddb_storage::util::stream_arn(
                &self.region,
                &t.account,
                &t.description.table_name,
                &label,
            );
            let mut shards = vec![];
            for n in 0..SHARDS {
                let id = format!("shardId-{}-{n:04}", uuid::Uuid::new_v4());
                kv::put(
                    tx,
                    self.key(&["shard", &id]),
                    &ShardOwner {
                        account: t.account.clone(),
                        arn: arn.clone(),
                    },
                )
                .await?;
                shards.push(id);
            }
            let stream = Stream {
                account: t.account.clone(),
                account_generation: kv::get::<crate::catalog::Account>(
                    tx,
                    self.key(&["account", &t.account]),
                )
                .await?
                .ok_or_else(|| StorageError::TableNotFound(t.account.clone()))?
                .generation,
                arn: arn.clone(),
                label: label.clone(),
                table_name: t.description.table_name.clone(),
                schema: t.description.key_schema.clone(),
                view,
                shards,
                enabled: true,
                created_ms: self.clock.now_ms(),
            };
            kv::put(tx, self.key(&["stream", &arn]), &stream).await?;
            t.description.latest_stream_arn = Some(arn);
            t.description.latest_stream_label = Some(label);
        }
        Ok(())
    }
    async fn sequence(
        &self,
        tx: &mut dyn kv::Transaction,
        shard: &str,
    ) -> Result<String, StorageError> {
        if kv::get::<ShardOwner>(tx, self.key(&["shard", shard]))
            .await?
            .is_none()
        {
            return Err(StorageError::TableNotFound(shard.into()));
        }
        let k = self.key(&["sequence", shard]);
        let n: u64 = kv::get(tx, k.clone()).await?.unwrap_or(0);
        let n = n
            .checked_add(1)
            .ok_or_else(|| StorageError::Internal("Stream sequence exhausted".into()))?;
        kv::put(tx, k, &n).await?;
        Ok(format!("{n:021}"))
    }
    pub(crate) async fn capture(
        &self,
        tx: &mut dyn kv::Transaction,
        t: &Table,
        old: Option<&Item>,
        new: Option<&Item>,
        capture: Option<&StreamCapture>,
    ) -> Result<(), StorageError> {
        let Some(arn) = &t.description.latest_stream_arn else {
            return Ok(());
        };
        let Some(stream) = kv::snapshot_get::<Stream>(tx, self.key(&["stream", arn])).await? else {
            return Err(StorageError::Internal("Missing stream generation".into()));
        };
        if !stream.enabled {
            return Ok(());
        }
        let Some(image) = new.or(old) else {
            return Ok(());
        };
        let hashes: Vec<_> = t
            .description
            .key_schema
            .iter()
            .filter(|k| k.key_type == KeyType::Hash)
            .cloned()
            .collect();
        let mut hash = vec![];
        codec::item_key(&mut hash, image, &hashes)?;
        let shard = &stream.shards[stable_hash(&hash) as usize % SHARDS];
        let sequence = self.sequence(tx, shard).await?;
        let keys = extract_key(image, &t.description.key_schema);
        let new_image = if matches!(
            stream.view,
            StreamViewType::NewImage | StreamViewType::NewAndOldImages
        ) {
            new.cloned()
        } else {
            None
        };
        let old_image = if matches!(
            stream.view,
            StreamViewType::OldImage | StreamViewType::NewAndOldImages
        ) {
            old.cloned()
        } else {
            None
        };
        let size = item_size_bytes(&keys)
            + new_image.as_ref().map(item_size_bytes).unwrap_or(0)
            + old_image.as_ref().map(item_size_bytes).unwrap_or(0);
        let record = StreamRecord {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_name: if old.is_none() {
                StreamEventName::Insert
            } else if new.is_none() {
                StreamEventName::Remove
            } else {
                StreamEventName::Modify
            },
            event_version: "1.1".into(),
            event_source: "aws:dynamodb".into(),
            aws_region: self.region.to_string(),
            user_identity: capture.and_then(|c| c.user_identity.clone()),
            dynamodb: StreamRecordData {
                approximate_creation_date_time: self.clock.now_ms() / 1000,
                keys,
                new_image,
                old_image,
                sequence_number: sequence.clone(),
                size_bytes: size as i64,
                stream_view_type: stream.view,
            },
        };
        kv::put(tx, self.key(&["records", shard, &sequence]), &record).await
    }
    async fn owned_stream(
        &self,
        tx: &mut dyn kv::Transaction,
        account: &str,
        arn: &str,
    ) -> Result<Stream, StorageError> {
        let s: Stream = kv::get(tx, self.key(&["stream", arn]))
            .await?
            .ok_or_else(|| StorageError::TableNotFound(arn.into()))?;
        let owner = kv::get::<crate::catalog::Account>(tx, self.key(&["account", account])).await?;
        if s.account != account || owner.is_none_or(|a| a.generation != s.account_generation) {
            return Err(StorageError::TableNotFound(arn.into()));
        }
        Ok(s)
    }
}
impl StreamEngine for TikvEngine {
    fn write_stream_record(
        &self,
        account: &str,
        record: &StreamRecord,
        shard: &str,
        _name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let (a, r, s) = (account.to_owned(), record.clone(), shard.to_owned());
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, a, r, s) = (self.clone(), a.clone(), r.clone(), s.clone());
                    Box::pin(async move {
                        let owner: ShardOwner = kv::get(tx, e.key(&["shard", &s]))
                            .await?
                            .ok_or_else(|| StorageError::TableNotFound(s.clone()))?;
                        if owner.account != a {
                            return Err(StorageError::TableNotFound(s));
                        }
                        e.owned_stream(tx, &a, &owner.arn).await?;
                        let k = e.key(&["records", &s, &r.dynamodb.sequence_number]);
                        if kv::get::<StreamRecord>(tx, k.clone()).await?.is_some() {
                            return Err(StorageError::Validation(
                                "Duplicate stream sequence".into(),
                            ));
                        }
                        kv::put(tx, k, &r).await
                    })
                })
                .await
        })
    }
    fn get_stream_records(
        &self,
        account: &str,
        shard: &str,
        after: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        let (a, s, after) = (
            account.to_owned(),
            shard.to_owned(),
            after.map(str::to_owned),
        );
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, a, s, after) = (self.clone(), a.clone(), s.clone(), after.clone());
                    Box::pin(async move {
                        let owner: Option<ShardOwner> = kv::get(tx, e.key(&["shard", &s])).await?;
                        let Some(owner) = owner.filter(|o| o.account == a) else {
                            return Err(StorageError::Validation("Invalid ShardIterator".into()));
                        };
                        match e.owned_stream(tx, &a, &owner.arn).await {
                            Ok(_) => {}
                            Err(StorageError::TableNotFound(_)) => {
                                return Err(StorageError::Validation(
                                    "Invalid ShardIterator".into(),
                                ));
                            }
                            Err(err) => return Err(err),
                        }
                        let p = e.key(&["records", &s]);
                        let start = if let Some(after) = after {
                            let mut k = e.key(&["records", &s, &after]);
                            k.push(0);
                            k
                        } else {
                            p.clone()
                        };
                        let rows = tx
                            .scan(
                                start,
                                codec::prefix_end(&p),
                                limit.clamp(1, 1000) as u32,
                                false,
                            )
                            .await
                            .map_err(kv::storage_error)?;
                        let records: Vec<StreamRecord> = rows
                            .into_iter()
                            .map(|(_, v)| kv::decode(&v))
                            .collect::<Result<_, _>>()?;
                        let last = records.last().map(|r| r.dynamodb.sequence_number.clone());
                        Ok((records, last))
                    })
                })
                .await
        })
    }
    fn describe_stream(
        &self,
        account: &str,
        input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        let (a, arn, start, limit) = (
            account.to_owned(),
            input.stream_arn.clone(),
            input.exclusive_start_shard_id.clone(),
            input.limit.unwrap_or(100).clamp(1, 100) as usize,
        );
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, a, arn, start) = (self.clone(), a.clone(), arn.clone(), start.clone());
                    Box::pin(async move {
                        let mut s = e.owned_stream(tx, &a, &arn).await?;
                        s.shards.sort();
                        let mut shards: Vec<_> = s
                            .shards
                            .into_iter()
                            .filter(|id| start.as_ref().is_none_or(|v| id > v))
                            .collect();
                        let more = shards.len() > limit;
                        shards.truncate(limit);
                        let last = if more { shards.last().cloned() } else { None };
                        let mut descriptions = vec![];
                        for id in shards {
                            let n: Option<u64> = kv::get(tx, e.key(&["sequence", &id])).await?;
                            descriptions.push(Shard {
                                shard_id: id,
                                parent_shard_id: None,
                                sequence_number_range: SequenceNumberRange {
                                    starting_sequence_number: format!("{:021}", 1),
                                    ending_sequence_number: (!s.enabled)
                                        .then(|| format!("{:021}", n.unwrap_or(0))),
                                },
                            });
                        }
                        Ok(StreamDescription {
                            stream_arn: s.arn,
                            stream_label: s.label,
                            stream_status: if s.enabled {
                                StreamStatus::Enabled
                            } else {
                                StreamStatus::Disabled
                            },
                            stream_view_type: s.view,
                            table_name: s.table_name,
                            key_schema: s.schema,
                            shards: descriptions,
                            last_evaluated_shard_id: last,
                        })
                    })
                })
                .await
        })
    }
    fn list_streams(
        &self,
        account: &str,
        table: Option<&str>,
        limit: i64,
        start: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult> {
        let (a, t, start) = (
            account.to_owned(),
            table.map(str::to_owned),
            start.map(str::to_owned),
        );
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, a, t, start) = (self.clone(), a.clone(), t.clone(), start.clone());
                    Box::pin(async move {
                        let Some(account) =
                            kv::get::<crate::catalog::Account>(tx, e.key(&["account", &a])).await?
                        else {
                            return Ok((vec![], None));
                        };
                        let mut streams = vec![];
                        for (_, v) in kv::all(tx, e.key(&["stream"])).await? {
                            let s: Stream = kv::decode(&v)?;
                            if s.account == a
                                && s.account_generation == account.generation
                                && t.as_ref().is_none_or(|n| &s.table_name == n)
                                && start.as_ref().is_none_or(|a| &s.arn > a)
                            {
                                streams.push(StreamSummary {
                                    stream_arn: s.arn,
                                    stream_label: s.label,
                                    table_name: s.table_name,
                                });
                            }
                        }
                        streams.sort_by(|a, b| a.stream_arn.cmp(&b.stream_arn));
                        let n = limit.clamp(1, 100) as usize;
                        let more = streams.len() > n;
                        streams.truncate(n);
                        let last = if more {
                            streams.last().map(|s| s.stream_arn.clone())
                        } else {
                            None
                        };
                        Ok((streams, last))
                    })
                })
                .await
        })
    }
    fn cleanup_expired_stream_records(
        &self,
        hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        let cutoff = self.clock.now_ms() / 1000 - hours.saturating_mul(3600);
        Box::pin(async move {
            self.prune_step::<StreamRecord>("records", move |r| {
                r.dynamodb.approximate_creation_date_time < cutoff
            })
            .await
        })
    }
    fn assign_shard(
        &self,
        account: &str,
        name: &str,
        partition: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        let (a, n, p) = (account.to_owned(), name.to_owned(), partition.to_owned());
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, a, n, p) = (self.clone(), a.clone(), n.clone(), p.clone());
                    Box::pin(async move {
                        let t = e.table_by_name(tx, &a, &n).await?;
                        let arn = t
                            .description
                            .latest_stream_arn
                            .ok_or(StorageError::TableNotFound(n))?;
                        let s = e.owned_stream(tx, &a, &arn).await?;
                        let mut key = vec![];
                        codec::component(&mut key, p.as_bytes());
                        Ok(s.shards[stable_hash(&key) as usize % SHARDS].clone())
                    })
                })
                .await
        })
    }
    fn next_sequence_number(&self, shard: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        let s = shard.to_owned();
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, s) = (self.clone(), s.clone());
                    Box::pin(async move { e.sequence(tx, &s).await })
                })
                .await
        })
    }
    fn validate_shard(
        &self,
        account: &str,
        arn: &str,
        shard: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let (a, arn, s) = (account.to_owned(), arn.to_owned(), shard.to_owned());
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, a, arn, s) = (self.clone(), a.clone(), arn.clone(), s.clone());
                    Box::pin(async move {
                        let stream = e.owned_stream(tx, &a, &arn).await?;
                        if stream.shards.contains(&s) {
                            Ok(())
                        } else {
                            Err(StorageError::TableNotFound(s))
                        }
                    })
                })
                .await
        })
    }
    fn latest_sequence_number(
        &self,
        shard: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        let s = shard.to_owned();
        Box::pin(async move {
            self.db
                .run(|tx| {
                    let (e, s) = (self.clone(), s.clone());
                    Box::pin(async move {
                        let p = e.key(&["records", &s]);
                        let rows = tx
                            .scan(p.clone(), codec::prefix_end(&p), 1, true)
                            .await
                            .map_err(kv::storage_error)?;
                        rows.first()
                            .map(|(_, v)| {
                                kv::decode::<StreamRecord>(v).map(|r| r.dynamodb.sequence_number)
                            })
                            .transpose()
                    })
                })
                .await
        })
    }
}
