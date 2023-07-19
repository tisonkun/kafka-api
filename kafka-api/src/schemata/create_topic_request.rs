// Copyright 2023 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;

use bytes::Buf;

use crate::{codec::*, err_decode_message_null};

#[derive(Debug, Default)]
pub struct CreateTopicsRequest {
    /// The topics to create.
    pub topics: Vec<CreatableTopic>,
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
    /// If true, check that the topics can be created as specified, but don't create anything.
    pub validate_only: bool,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for CreateTopicsRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut res = CreateTopicsRequest {
            topics: NullableArray(Struct(version), version >= 5)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("name"))?,
            timeout_ms: Int32.decode(buf)?,
            ..Default::default()
        };
        if version >= 1 {
            res.validate_only = Bool.decode(buf)?;
        }
        if version >= 5 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct CreatableTopic {
    /// The topic name.
    pub name: String,
    /// The number of partitions to create in the topic, or -1 if we are either specifying a manual
    /// partition assignment or using the default partitions.
    pub num_partitions: i32,
    /// The number of replicas to create for each partition in the topic, or -1 if we are either
    /// specifying a manual partition assignment or using the default replication factor.
    pub replication_factor: i16,
    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    pub assignments: Vec<CreatableReplicaAssignment>,
    /// The custom topic configurations to set.
    pub configs: Vec<CreatableTopicConfig>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for CreatableTopic {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut res = CreatableTopic {
            name: NullableString(version >= 5)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("name"))?,
            num_partitions: Int32.decode(buf)?,
            replication_factor: Int16.decode(buf)?,
            assignments: NullableArray(Struct(version), version >= 5)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("assignments"))?,
            configs: NullableArray(Struct(version), version >= 5)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("assignments"))?,
            ..Default::default()
        };
        if version >= 5 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct CreatableTopicConfig {
    /// The configuration name.
    pub name: String,
    /// The configuration value.
    pub value: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for CreatableTopicConfig {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut res = CreatableTopicConfig {
            name: NullableString(version >= 5)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("name"))?,
            value: NullableString(version >= 5).decode(buf)?,
            ..Default::default()
        };
        if version >= 5 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct CreatableReplicaAssignment {
    /// The partition index.
    pub partition_index: i32,
    /// The brokers to place the partition on.
    pub broker_ids: Vec<i32>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for CreatableReplicaAssignment {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut res = CreatableReplicaAssignment {
            partition_index: Int32.decode(buf)?,
            ..Default::default()
        };
        res.broker_ids = NullableArray(Int32, version >= 5)
            .decode(buf)?
            .ok_or_else(|| err_decode_message_null("broker_ids"))?;
        if version >= 5 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}
