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

#[derive(Debug, Default, Clone)]
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Results for each topic we tried to create.
    pub topics: Vec<CreatableTopicResult>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for CreateTopicsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version >= 2 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        NullableArray(Struct(version), version >= 5).encode(buf, self.topics.as_slice())?;
        if version >= 5 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct CreatableTopicResult {
    /// The topic name.
    pub name: String,
    /// The unique topic ID
    pub topic_id: uuid::Uuid,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<String>,
    /// Optional topic config error returned if configs are not returned in the response.
    pub topic_config_error_code: i16,
    /// Number of partitions of the topic.
    pub num_partitions: i32,
    /// Replication factor of the topic.
    pub replication_factor: i16,
    /// Configuration of the topic.
    pub configs: Vec<CreatableTopicConfigs>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for CreatableTopicResult {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        NullableString(version >= 5).encode(buf, self.name.as_str())?;
        if version >= 7 {
            Uuid.encode(buf, self.topic_id)?;
        }
        Int16.encode(buf, self.error_code)?;
        if version >= 1 {
            NullableString(version >= 5).encode(buf, self.error_message.as_deref())?;
        }
        if version >= 5 {
            Int32.encode(buf, self.num_partitions)?;
            Int16.encode(buf, self.replication_factor)?;
            NullableArray(Struct(version), true).encode(buf, self.configs.as_slice())?;
        }
        if version >= 5 {
            let mut unknown_tagged_fields = vec![];
            if self.topic_config_error_code != 0 {
                unknown_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: Int16.encode_alloc(self.topic_config_error_code)?,
                })
            }
            unknown_tagged_fields.append(&mut self.unknown_tagged_fields.clone());
            RawTaggedFieldList.encode(buf, &unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct CreatableTopicConfigs {
    /// The configuration name.
    pub name: String,
    /// The configuration value.
    pub value: Option<String>,
    /// True if the configuration is read-only.
    pub read_only: bool,
    /// The configuration source.
    pub config_source: i8,
    /// True if this configuration is sensitive.
    pub is_sensitive: bool,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for CreatableTopicConfigs {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version < 5 {
            Err(err_encode_message_unsupported(
                version,
                "CreatableTopicConfigs",
            ))?
        }
        NullableString(true).encode(buf, self.name.as_str())?;
        NullableString(true).encode(buf, self.value.as_deref())?;
        Bool.encode(buf, self.read_only)?;
        Int8.encode(buf, self.config_source)?;
        Bool.encode(buf, self.is_sensitive)?;
        RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        Ok(())
    }
}
