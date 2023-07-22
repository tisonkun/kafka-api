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

// Version 1 added the throttle time.
//
// Version 2 added the log append time.
//
// Version 3 is the same as version 2.
//
// Version 4 added KAFKA_STORAGE_ERROR as a possible error code.
//
// Version 5 added LogStartOffset to filter out spurious
// OutOfOrderSequenceExceptions on the client.
//
// Version 8 added RecordErrors and ErrorMessage to include information about
// records that cause the whole batch to be dropped.  See KIP-467 for details.
//
// Version 9 enables flexible versions.

#[derive(Debug, Default, Clone)]
pub struct ProduceResponse {
    /// Each produce response
    pub responses: Vec<TopicProduceResponse>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for ProduceResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        NullableArray(Struct(version), version >= 9).encode(buf, self.responses.as_slice())?;
        if version > 1 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        if version >= 9 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct TopicProduceResponse {
    /// The topic name.
    pub name: String,
    /// Each partition that we produced to within the topic.
    pub partition_responses: Vec<PartitionProduceResponse>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for TopicProduceResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        NullableString(version >= 9).encode(buf, self.name.as_str())?;
        NullableArray(Struct(version), version >= 9)
            .encode(buf, self.partition_responses.as_slice())?;
        if version >= 9 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct PartitionProduceResponse {
    /// The partition index.
    pub index: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The base offset.
    pub base_offset: i64,
    /// The timestamp returned by broker after appending the messages. If CreateTime is used for
    /// the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp
    /// will be the broker local time when the messages are appended.
    pub log_append_time_ms: i64,
    /// The log start offset.
    pub log_start_offset: i64,
    /// The batch indices of records that caused the batch to be dropped.
    pub record_errors: Vec<BatchIndexAndErrorMessage>,
    /// The global error message summarizing the common root cause of the records that caused the
    /// batch to be dropped.
    pub error_message: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for PartitionProduceResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        Int32.encode(buf, self.index)?;
        Int16.encode(buf, self.error_code)?;
        Int64.encode(buf, self.base_offset)?;
        if version >= 2 {
            Int64.encode(buf, self.log_append_time_ms)?;
        }
        if version >= 5 {
            Int64.encode(buf, self.log_start_offset)?;
        }
        if version >= 8 {
            NullableArray(Struct(version), version >= 9)
                .encode(buf, self.record_errors.as_slice())?;
        }
        if version >= 8 {
            NullableString(version >= 9).encode(buf, self.error_message.as_deref())?;
        }
        if version >= 9 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct BatchIndexAndErrorMessage {
    /// The batch index of the record that cause the batch to be dropped.
    pub batch_index: i32,
    /// The error message of the record that caused the batch to be dropped.
    pub batch_index_error_message: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for BatchIndexAndErrorMessage {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version < 8 {
            Err(err_encode_message_unsupported(
                version,
                "BatchIndexAndErrorMessage",
            ))?
        }
        Int32.encode(buf, self.batch_index)?;
        NullableString(version >= 9).encode(buf, self.batch_index_error_message.as_deref())?;
        if version >= 9 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}
