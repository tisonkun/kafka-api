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

use bytes::BufMut;

use crate::{codec::*, err_encode_message_unsupported};

// Version 1 is the same as version 0.
//
// Version 2 adds a top-level error code.
//
// Version 3 adds the throttle time.
//
// Starting in version 4, on quota violation, brokers send out responses before throttling.
//
// Version 5 adds the leader epoch to the committed offset.
//
// Version 6 is the first flexible version.
//
// Version 7 adds pending offset commit as new error response on partition level.
//
// Version 8 is adding support for fetching offsets for multiple groups

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The responses per topic.
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// The top-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The responses per group id.
    pub groups: Vec<OffsetFetchResponseGroup>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for OffsetFetchResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version >= 3 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        if version <= 7 {
            NullableArray(Struct(version), version >= 6).encode(buf, self.topics.as_slice())?;
        }
        if (2..=7).contains(&version) {
            Int16.encode(buf, self.error_code)?;
        }
        if version >= 8 {
            NullableArray(Struct(version), true).encode(buf, self.groups.as_slice())?;
        }
        if version >= 6 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchResponseTopic {
    /// The topic name.
    pub name: String,
    /// The responses per partition.
    pub partitions: Vec<OffsetFetchResponsePartition>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for OffsetFetchResponseTopic {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version > 7 {
            Err(err_encode_message_unsupported(
                version,
                "OffsetFetchResponseTopic",
            ))?
        }
        NullableString(version >= 6).encode(buf, self.name.as_str())?;
        NullableArray(Struct(version), version >= 6).encode(buf, self.partitions.as_slice())?;
        if version >= 6 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchResponsePartition {
    /// The partition index.
    pub partition_index: i32,
    /// The committed message offset.
    pub committed_offset: i32,
    /// The leader epoch.
    pub committed_leader_epoch: i32,
    /// The partition metadata.
    pub metadata: Option<String>,
    /// The partition-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for OffsetFetchResponsePartition {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        Int32.encode(buf, self.partition_index)?;
        Int32.encode(buf, self.committed_offset)?;
        if version >= 5 {
            Int32.encode(buf, self.committed_leader_epoch)?;
        }
        NullableString(version >= 6).encode(buf, self.metadata.as_deref())?;
        Int16.encode(buf, self.error_code)?;
        if version >= 6 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchResponseGroup {
    /// The group to fetch offsets for.
    pub group_id: String,
    /// The responses per topic.
    pub topics: Vec<OffsetFetchResponseTopics>,
    /// The group-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for OffsetFetchResponseGroup {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version < 8 {
            Err(err_encode_message_unsupported(
                version,
                "OffsetFetchResponseGroup",
            ))?
        }
        NullableString(true).encode(buf, self.group_id.as_str())?;
        NullableArray(Struct(version), true).encode(buf, self.topics.as_slice())?;
        Int16.encode(buf, self.error_code)?;
        RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchResponseTopics {
    /// The topic name.
    pub name: String,
    /// The responses per partition.
    pub partitions: Vec<OffsetFetchResponsePartitions>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for OffsetFetchResponseTopics {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        NullableString(true).encode(buf, self.name.as_str())?;
        NullableArray(Struct(version), true).encode(buf, self.partitions.as_slice())?;
        RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct OffsetFetchResponsePartitions {
    /// The partition index.
    pub partition_index: i32,
    /// The committed message offset.
    pub committed_offset: i64,
    /// The leader epoch.
    pub committed_leader_epoch: i32,
    /// The partition metadata.
    pub metadata: Option<String>,
    /// The partition-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for OffsetFetchResponsePartitions {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> io::Result<()> {
        Int32.encode(buf, self.partition_index)?;
        Int64.encode(buf, self.committed_offset)?;
        Int32.encode(buf, self.committed_leader_epoch)?;
        NullableString(true).encode(buf, self.metadata.as_deref())?;
        Int16.encode(buf, self.error_code)?;
        RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        Ok(())
    }
}