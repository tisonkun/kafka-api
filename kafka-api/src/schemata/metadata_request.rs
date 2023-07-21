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

use crate::{codec::*, err_decode_message_null, err_decode_message_unsupported};

#[derive(Debug, Default, Clone)]
pub struct MetadataRequest {
    /// The topics to fetch metadata for.
    pub topics: Vec<MetadataRequestTopic>,
    /// If this is true, the broker may auto-create topics that we requested which do not already
    /// exist, if it is configured to do so.
    pub allow_auto_topic_creation: bool,
    /// Whether to include cluster authorized operations.
    pub include_cluster_authorized_operations: bool,
    /// Whether to include topic authorized operations.
    pub include_topic_authorized_operations: bool,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for MetadataRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut this = MetadataRequest {
            topics: NullableArray(Struct(version), version >= 9)
                .decode(buf)?
                .or_else(|| if version >= 1 { Some(vec![]) } else { None })
                .ok_or_else(|| err_decode_message_null("topics"))?,
            ..Default::default()
        };
        if version >= 4 {
            this.allow_auto_topic_creation = Bool.decode(buf)?;
        } else {
            this.allow_auto_topic_creation = true;
        };
        if (8..=10).contains(&version) {
            this.include_cluster_authorized_operations = Bool.decode(buf)?;
        }
        if version >= 9 {
            this.include_topic_authorized_operations = Bool.decode(buf)?;
        }
        if version >= 9 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}

#[derive(Debug, Default, Clone)]
pub struct MetadataRequestTopic {
    /// The topic id.
    pub topic_id: uuid::Uuid,
    /// The topic name.
    pub name: Option<String>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for MetadataRequestTopic {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> io::Result<Self> {
        if version > 12 {
            Err(err_decode_message_unsupported(
                version,
                "MetadataRequestTopic",
            ))?
        }
        let mut this = MetadataRequestTopic::default();
        if version >= 10 {
            this.topic_id = Uuid.decode(buf)?;
        }
        this.name = if version >= 10 {
            NullableString(true).decode(buf)?
        } else {
            Some(
                NullableString(version >= 9)
                    .decode(buf)?
                    .ok_or_else(|| err_decode_message_null("name"))?,
            )
        };
        if version >= 9 {
            this.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(this)
    }
}
