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

use bytes::BufMut;

use crate::{
    apikey::ApiMessageType, codec::*, request_header::RequestHeader,
    response_header::ResponseHeader,
};

// Upstream baseline commits
// -------------------------
// Generator https://github.com/apache/kafka/commit/c08120f83f7318f15dcf14d525876d18caf6afd0
// Message   https://github.com/apache/kafka/commit/443bd1dd82d45f1169b3d5ff83d62c4e9220511c

pub mod api_versions_request;
pub mod api_versions_response;
pub mod create_topic_request;
pub mod create_topic_response;
pub mod find_coordinator_request;
pub mod find_coordinator_response;
pub mod init_producer_id_request;
pub mod init_producer_id_response;
pub mod join_group_request;
pub mod join_group_response;
pub mod metadata_request;
pub mod metadata_response;
pub mod produce_request;
pub mod produce_response;
pub mod request_header;
pub mod response_header;
pub mod sync_group_request;
pub mod sync_group_response;

#[derive(Debug)]
pub enum Request {
    ApiVersionsRequest(api_versions_request::ApiVersionsRequest),
    CreateTopicRequest(create_topic_request::CreateTopicsRequest),
    FindCoordinatorRequest(find_coordinator_request::FindCoordinatorRequest),
    InitProducerIdRequest(init_producer_id_request::InitProducerIdRequest),
    JoinGroupRequest(join_group_request::JoinGroupRequest),
    MetadataRequest(metadata_request::MetadataRequest),
    ProduceRequest(produce_request::ProduceRequest),
    SyncGroupRequest(sync_group_request::SyncGroupRequest),
}

impl Request {
    pub fn decode<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> io::Result<(RequestHeader, Request)> {
        let pos = cursor.position();
        let api_key = Int16.decode(cursor)?;
        let api_version = Int16.decode(cursor)?;
        let header_version = ApiMessageType::try_from(api_key)?.request_header_version(api_version);

        cursor.set_position(pos);

        let header = RequestHeader::decode(cursor, header_version)?;
        let api_type = ApiMessageType::try_from(header.request_api_key)?;
        let api_version = header.request_api_version;

        let request = match api_type {
            ApiMessageType::ApiVersions => {
                api_versions_request::ApiVersionsRequest::decode(cursor, api_version)
                    .map(Request::ApiVersionsRequest)
            }
            ApiMessageType::CreateTopics => {
                create_topic_request::CreateTopicsRequest::decode(cursor, api_version)
                    .map(Request::CreateTopicRequest)
            }
            ApiMessageType::FindCoordinator => {
                find_coordinator_request::FindCoordinatorRequest::decode(cursor, api_version)
                    .map(Request::FindCoordinatorRequest)
            }
            ApiMessageType::InitProducerId => {
                init_producer_id_request::InitProducerIdRequest::decode(cursor, api_version)
                    .map(Request::InitProducerIdRequest)
            }
            ApiMessageType::JoinGroup => {
                join_group_request::JoinGroupRequest::decode(cursor, api_version)
                    .map(Request::JoinGroupRequest)
            }
            ApiMessageType::Metadata => {
                metadata_request::MetadataRequest::decode(cursor, api_version)
                    .map(Request::MetadataRequest)
            }
            ApiMessageType::Produce => produce_request::ProduceRequest::decode(cursor, api_version)
                .map(Request::ProduceRequest),
            ApiMessageType::SyncGroup => {
                sync_group_request::SyncGroupRequest::decode(cursor, api_version)
                    .map(Request::SyncGroupRequest)
            }
            _ => unimplemented!("{}", api_type.api_key),
        }?;

        Ok((header, request))
    }
}

#[derive(Debug)]
pub enum Response {
    ApiVersionsResponse(api_versions_response::ApiVersionsResponse),
    CreateTopicsResponse(create_topic_response::CreateTopicsResponse),
    FindCoordinatorResponse(find_coordinator_response::FindCoordinatorResponse),
    InitProducerIdResponse(init_producer_id_response::InitProducerIdResponse),
    JoinGroupResponse(join_group_response::JoinGroupResponse),
    MetadataResponse(metadata_response::MetadataResponse),
    ProduceResponse(produce_response::ProduceResponse),
    SyncGroupResponse(sync_group_response::SyncGroupResponse),
}

impl Response {
    pub fn encode_alloc(&self, header: RequestHeader) -> io::Result<bytes::Bytes> {
        let api_type = ApiMessageType::try_from(header.request_api_key)?;
        let api_version = header.request_api_version;
        let correlation_id = header.correlation_id;

        let mut buf = vec![];
        let response_header_version = api_type.response_header_version(api_version);
        let response_header = ResponseHeader {
            correlation_id,
            unknown_tagged_fields: vec![],
        };
        response_header.encode(&mut buf, response_header_version)?;

        match self {
            Response::ApiVersionsResponse(resp) => resp.encode(&mut buf, api_version)?,
            Response::CreateTopicsResponse(resp) => resp.encode(&mut buf, api_version)?,
            Response::FindCoordinatorResponse(resp) => resp.encode(&mut buf, api_version)?,
            Response::InitProducerIdResponse(resp) => resp.encode(&mut buf, api_version)?,
            Response::JoinGroupResponse(resp) => resp.encode(&mut buf, api_version)?,
            Response::MetadataResponse(resp) => resp.encode(&mut buf, api_version)?,
            Response::ProduceResponse(resp) => resp.encode(&mut buf, api_version)?,
            Response::SyncGroupResponse(resp) => resp.encode(&mut buf, api_version)?,
        }

        let mut bs = bytes::BytesMut::new();
        Int32.encode(&mut bs, buf.len() as i32)?;
        bs.put_slice(buf.as_slice());
        Ok(bs.freeze())
    }
}
