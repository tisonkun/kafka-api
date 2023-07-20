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

use crate::err_codec_message;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ApiMessageType {
    pub api_key: i16,
    pub lowest_supported_version: i16,
    pub highest_supported_version: i16,
}

#[allow(non_upper_case_globals)]
impl ApiMessageType {
    pub const Produce: Self = ApiMessageType::new(0, 0, 9);
    pub const Metadata: Self = ApiMessageType::new(3, 0, 12);
    pub const FindCoordinator: Self = ApiMessageType::new(10, 0, 4);
    pub const JoinGroup: Self = ApiMessageType::new(11, 0, 9);
    pub const SyncGroup: Self = ApiMessageType::new(14, 0, 5);
    pub const ApiVersions: Self = ApiMessageType::new(18, 0, 3);
    pub const CreateTopics: Self = ApiMessageType::new(19, 0, 7);
    pub const InitProducerId: Self = ApiMessageType::new(22, 0, 4);

    const fn new(
        api_key: i16,
        lowest_supported_version: i16,
        highest_supported_version: i16,
    ) -> Self {
        Self {
            api_key,
            lowest_supported_version,
            highest_supported_version,
        }
    }
}

impl TryFrom<i16> for ApiMessageType {
    type Error = io::Error;

    fn try_from(api_key: i16) -> Result<Self, Self::Error> {
        match api_key {
            0 => Ok(ApiMessageType::Produce),
            3 => Ok(ApiMessageType::Metadata),
            10 => Ok(ApiMessageType::FindCoordinator),
            11 => Ok(ApiMessageType::JoinGroup),
            14 => Ok(ApiMessageType::SyncGroup),
            18 => Ok(ApiMessageType::ApiVersions),
            19 => Ok(ApiMessageType::CreateTopics),
            22 => Ok(ApiMessageType::InitProducerId),
            _ => Err(err_codec_message(format!("unknown api key {api_key}"))),
        }
    }
}

impl ApiMessageType {
    pub fn request_header_version(&self, api_version: i16) -> i16 {
        // the current different is whether the request is flexible
        fn resolve_request_header_version(flexible: bool) -> i16 {
            if flexible {
                2
            } else {
                1
            }
        }
        match *self {
            ApiMessageType::Produce => resolve_request_header_version(api_version >= 9),
            ApiMessageType::Metadata => resolve_request_header_version(api_version >= 9),
            ApiMessageType::FindCoordinator => resolve_request_header_version(api_version >= 3),
            ApiMessageType::JoinGroup => resolve_request_header_version(api_version >= 6),
            ApiMessageType::SyncGroup => resolve_request_header_version(api_version >= 4),
            ApiMessageType::ApiVersions => resolve_request_header_version(api_version >= 3),
            ApiMessageType::CreateTopics => resolve_request_header_version(api_version >= 5),
            ApiMessageType::InitProducerId => resolve_request_header_version(api_version >= 2),
            _ => unreachable!("unknown api type {}", self.api_key),
        }
    }

    pub fn response_header_version(&self, api_version: i16) -> i16 {
        // the current different is whether the response is flexible
        fn resolve_response_header_version(flexible: bool) -> i16 {
            if flexible {
                1
            } else {
                0
            }
        }

        match *self {
            ApiMessageType::Produce => resolve_response_header_version(api_version >= 9),
            ApiMessageType::Metadata => resolve_response_header_version(api_version >= 9),
            ApiMessageType::FindCoordinator => resolve_response_header_version(api_version >= 3),
            ApiMessageType::JoinGroup => resolve_response_header_version(api_version >= 6),
            ApiMessageType::SyncGroup => resolve_response_header_version(api_version >= 4),
            // ApiVersionsResponse always includes a v0 header. See KIP-511 for details.
            ApiMessageType::ApiVersions => 0,
            ApiMessageType::CreateTopics => resolve_response_header_version(api_version >= 5),
            ApiMessageType::InitProducerId => resolve_response_header_version(api_version >= 2),
            _ => unreachable!("unknown api type {}", self.api_key),
        }
    }
}
