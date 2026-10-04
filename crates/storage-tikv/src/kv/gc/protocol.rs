// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Minimal PD protobuf projection for the three GC-barrier RPCs we use.
//! Wire tags follow kvproto/pdpb.proto (Apache-2.0), also present in the TiDB
//! reference's rust/third_party/tikv-client-rs/proto/pdpb.proto. Unknown fields are
//! ignored by Prost. No GC-advancing request type is exposed by this module.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Header {
    #[prost(uint64, tag = "1")]
    pub cluster_id: u64,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct PdError {
    #[prost(int32, tag = "1")]
    pub kind: i32,
    #[prost(string, tag = "2")]
    pub message: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct ResponseHeader {
    #[prost(uint64, tag = "1")]
    pub cluster_id: u64,
    #[prost(message, optional, tag = "2")]
    pub error: Option<PdError>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Request {
    #[prost(message, optional, tag = "1")]
    pub header: Option<Header>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Member {
    #[prost(string, repeated, tag = "4")]
    pub urls: Vec<String>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Members {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(message, optional, tag = "3")]
    pub leader: Option<Member>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct SafePoint {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(uint64, tag = "2")]
    pub safe_point: u64,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Pin {
    #[prost(message, optional, tag = "1")]
    pub header: Option<Header>,
    #[prost(bytes = "vec", tag = "2")]
    pub service: Vec<u8>,
    #[prost(int64, tag = "3")]
    pub ttl: i64,
    #[prost(uint64, tag = "4")]
    pub safe_point: u64,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Pinned {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(uint64, tag = "4")]
    pub minimum: u64,
}
