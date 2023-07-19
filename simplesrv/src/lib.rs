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

use kafka_api::{
    api_versions_response::{ApiVersion, ApiVersionsResponse},
    apikey::ApiMessageType,
    Request, Response,
};

pub struct Broker {}

impl Broker {
    pub fn reply(&mut self, request: Request) -> Response {
        match request {
            Request::ApiVersionsRequest(_) => Response::ApiVersionsResponse(ApiVersionsResponse {
                error_code: 0,
                api_keys: supported_apis()
                    .iter()
                    .map(|api| ApiVersion {
                        api_key: api.api_key,
                        min_version: api.lowest_supported_version,
                        max_version: api.highest_supported_version,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            _ => unimplemented!("{:?}", request),
            // Request::CreateTopicRequest(_) => {}
            // Request::InitProducerIdRequest(_) => {}
            // Request::MetadataRequest(_) => {}
        }
    }
}

const fn supported_apis() -> &'static [ApiMessageType] {
    &[
        ApiMessageType::Metadata,
        ApiMessageType::ApiVersions,
        ApiMessageType::CreateTopics,
        ApiMessageType::InitProducerId,
    ]
}
