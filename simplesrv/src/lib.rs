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

use std::{cmp::max, collections::BTreeMap};

use kafka_api::{
    api_versions_request::ApiVersionsRequest,
    api_versions_response::{ApiVersion, ApiVersionsResponse},
    apikey::ApiMessageType,
    create_topic_request::CreateTopicsRequest,
    create_topic_response::{CreatableTopicResult, CreateTopicsResponse},
    metadata_request::MetadataRequest,
    metadata_response::{
        MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    },
    Request, Response,
};
use tracing::trace;

#[derive(Debug, Clone)]
pub struct ClusterMeta {
    pub cluster_id: String,
    pub controller_id: i32,
    pub brokers: Vec<BrokerMeta>,
}

#[derive(Debug, Clone)]
pub struct BrokerMeta {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
struct TopicMeta {
    topic_id: uuid::Uuid,
    topic_name: String,
    partitions: BTreeMap<i32, PartitionMeta>,
}

#[derive(Debug, Clone)]
struct PartitionMeta {}

pub struct Broker {
    broker_meta: BrokerMeta, // this
    cluster_meta: ClusterMeta,
    topics: BTreeMap<String, TopicMeta>,
}

impl Broker {
    pub fn new(broker_meta: BrokerMeta, cluster_meta: ClusterMeta) -> Broker {
        Broker {
            broker_meta,
            cluster_meta,
            topics: BTreeMap::new(),
        }
    }

    pub fn reply(&mut self, request: Request) -> Response {
        let response = match request {
            Request::ApiVersionsRequest(request) => {
                Response::ApiVersionsResponse(self.receive_api_versions_request(request))
            }
            Request::CreateTopicRequest(request) => {
                Response::CreateTopicsResponse(self.receive_create_topic_request(request))
            }
            Request::MetadataRequest(request) => {
                Response::MetadataResponse(self.receive_metadata_request(request))
            }
            _ => unimplemented!("{:?}", request),
            // Request::InitProducerIdRequest(_) => {}
        };
        trace!("Reply {response:?}");
        response
    }

    fn receive_api_versions_request(
        &mut self,
        _request: ApiVersionsRequest,
    ) -> ApiVersionsResponse {
        let api_keys = supported_apis()
            .iter()
            .map(|api| ApiVersion {
                api_key: api.api_key,
                min_version: api.lowest_supported_version,
                max_version: api.highest_supported_version,
                ..Default::default()
            })
            .collect();

        ApiVersionsResponse {
            error_code: 0,
            api_keys,
            ..Default::default()
        }
    }

    fn receive_metadata_request(&mut self, _request: MetadataRequest) -> MetadataResponse {
        let brokers = self
            .cluster_meta
            .brokers
            .iter()
            .map(|broker_meta| MetadataResponseBroker {
                node_id: broker_meta.node_id,
                host: broker_meta.host.clone(),
                port: broker_meta.port,
                ..Default::default()
            })
            .collect();

        let topics = self
            .topics
            .values()
            .map(|topic| MetadataResponseTopic {
                name: Some(topic.topic_name.clone()),
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .keys()
                    .map(|idx| MetadataResponsePartition {
                        partition_index: *idx,
                        leader_id: self.broker_meta.node_id,
                        replica_nodes: self
                            .cluster_meta
                            .brokers
                            .iter()
                            .map(|broker| broker.node_id)
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();

        MetadataResponse {
            brokers,
            cluster_id: Some(self.cluster_meta.cluster_id.clone()),
            controller_id: self.cluster_meta.controller_id,
            topics,
            ..Default::default()
        }
    }

    fn receive_create_topic_request(
        &mut self,
        request: CreateTopicsRequest,
    ) -> CreateTopicsResponse {
        let mut topics = vec![];
        for topic in request.topics.iter() {
            let topic_id = uuid::Uuid::new_v4();
            let topic_name = topic.name.clone();
            let num_partitions = max(topic.num_partitions, 1);

            let partitions = (0..num_partitions)
                .map(|idx| (idx, PartitionMeta {}))
                .collect();

            // TODO - handle topic already exist
            self.topics.insert(
                topic_name.clone(),
                TopicMeta {
                    topic_id,
                    topic_name,
                    partitions,
                },
            );

            topics.push(CreatableTopicResult {
                name: topic.name.clone(),
                topic_id,
                num_partitions,
                ..Default::default()
            })
        }

        CreateTopicsResponse {
            topics,
            ..Default::default()
        }
    }
}

const fn supported_apis() -> &'static [ApiMessageType] {
    &[
        ApiMessageType::ApiVersions,
        ApiMessageType::CreateTopics,
        ApiMessageType::InitProducerId,
        ApiMessageType::Metadata,
    ]
}
