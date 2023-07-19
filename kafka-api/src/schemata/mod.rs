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

use std::{io, io::Cursor};

use crate::{apikey::ApiMessageType, codec::*, request_header::RequestHeader};

pub mod api_versions_request;
pub mod api_versions_response;
pub mod create_topic_request;
pub mod create_topic_response;
pub mod init_producer_id_request;
pub mod init_producer_id_response;
pub mod metadata_request;
pub mod metadata_response;
pub mod request_header;
pub mod response_header;

#[derive(Debug)]
pub enum Request {
    ApiVersionsRequest(api_versions_request::ApiVersionsRequest),
    CreateTopicRequest(create_topic_request::CreateTopicsRequest),
    InitProducerIdRequest(init_producer_id_request::InitProducerIdRequest),
    MetadataRequest(metadata_request::MetadataRequest),
}

impl Request {
    pub fn decode<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> io::Result<Self> {
        let pos = cursor.position();
        let api_key = Int16.decode(cursor)?;
        let api_version = Int16.decode(cursor)?;
        let header_version = ApiMessageType::try_from(api_key)?.request_header_version(api_version);

        cursor.set_position(pos);

        let header = RequestHeader::decode(cursor, header_version)?;
        let api_type = ApiMessageType::try_from(header.request_api_key)?;
        let api_version = header.request_api_version;

        match api_type {
            ApiMessageType::ApiVersions => {
                api_versions_request::ApiVersionsRequest::decode(cursor, api_version)
                    .map(Request::ApiVersionsRequest)
            }
            ApiMessageType::CreateTopics => {
                create_topic_request::CreateTopicsRequest::decode(cursor, api_version)
                    .map(Request::CreateTopicRequest)
            }
            ApiMessageType::InitProducerId => {
                init_producer_id_request::InitProducerIdRequest::decode(cursor, api_version)
                    .map(Request::InitProducerIdRequest)
            }
            ApiMessageType::Metadata => {
                metadata_request::MetadataRequest::decode(cursor, api_version)
                    .map(Request::MetadataRequest)
            }
            _ => unimplemented!("{}", api_type.api_key),
        }
    }
}
