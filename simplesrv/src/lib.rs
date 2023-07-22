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

use std::{
    cmp::max,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    sync::atomic::{AtomicI64, Ordering},
};

use kafka_api::{
    api_versions_request::ApiVersionsRequest,
    api_versions_response::{ApiVersion, ApiVersionsResponse},
    apikey::ApiMessageType,
    create_topic_request::CreateTopicsRequest,
    create_topic_response::{CreatableTopicResult, CreateTopicsResponse},
    error::Error,
    find_coordinator_request::FindCoordinatorRequest,
    find_coordinator_response::{Coordinator, FindCoordinatorResponse},
    init_producer_id_request::InitProducerIdRequest,
    init_producer_id_response::InitProducerIdResponse,
    join_group_request::JoinGroupRequest,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    metadata_request::MetadataRequest,
    metadata_response::{
        MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    },
    offset_fetch_request::OffsetFetchRequest,
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
        OffsetFetchResponseTopics,
    },
    produce_request::ProduceRequest,
    produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
    sync_group_request::SyncGroupRequest,
    sync_group_response::SyncGroupResponse,
    Request, Response,
};
use tracing::{debug, trace};

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: String,
    pub client_host: String,
}

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

#[derive(Debug, Default)]
struct GroupCoordinator {
    groups: BTreeMap<String, GroupMeta>,
}

#[derive(Debug)]
struct GroupMeta {
    group_id: String,
    leader_id: Option<String>,
    generation_id: i32,
    protocol: Option<String>,
    protocol_type: Option<String>,
    members: BTreeMap<String, MemberMeta>,
}

impl GroupMeta {
    fn new(group_id: String) -> GroupMeta {
        GroupMeta {
            group_id,
            leader_id: None,
            generation_id: 0,
            protocol: None,
            protocol_type: None,
            members: BTreeMap::new(),
        }
    }

    fn add(&mut self, member: MemberMeta) {
        if self.members.is_empty() {
            self.protocol_type = Some(member.protocol_type.clone());
        }
        debug_assert_eq!(self.group_id, member.group_id);
        debug_assert_eq!(
            self.protocol_type.as_deref(),
            Some(member.protocol_type.as_str())
        );
        if self.leader_id.is_none() {
            self.leader_id = Some(member.member_id.clone());
        }
        self.members.insert(member.member_id.clone(), member);
        self.make_next_generation();
    }

    fn make_next_generation(&mut self) {
        self.generation_id += 1;
        if self.members.is_empty() {
            self.protocol = None;
        } else {
            self.protocol = self.select_protocol();
        }
    }

    fn select_protocol(&self) -> Option<String> {
        let candidates = self.candidate_protocols();
        let mut votes = BTreeMap::new();
        for member in self.members.values() {
            let vote_for = member.vote(&candidates);
            *(votes.entry(vote_for).or_insert(0)) += 1;
        }
        votes
            .iter()
            .map(|(k, v)| (v, k))
            .next()
            .map(|(_, selected)| selected)
            .cloned()
    }

    fn candidate_protocols(&self) -> BTreeSet<String> {
        self.members
            .values()
            .flat_map(|m| m.protocols.keys())
            .cloned()
            .collect()
    }

    fn sync(&mut self, assignments: BTreeMap<String, bytes::Bytes>) {
        for member in self.members.values_mut() {
            let assignment = assignments
                .get(&member.member_id)
                .cloned()
                .unwrap_or_default();
            member.assignment = assignment;
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct MemberMeta {
    group_id: String,
    member_id: String,
    client_id: String,
    client_host: String,
    protocol_type: String,
    protocols: BTreeMap<String, bytes::Bytes>,
    assignment: bytes::Bytes,
    rebalance_timeout_ms: i32,
    session_timeout_ms: i32,
}

impl MemberMeta {
    fn vote(&self, candidates: &BTreeSet<String>) -> String {
        trace!(
            "{} supports protocols {:?}, vote among {:?}",
            self.member_id,
            self.protocols,
            candidates
        );
        candidates
            .iter()
            .find(|p| self.protocols.contains_key(*p))
            .expect("member does not support any of the candidate protocols")
            .clone()
    }
}

#[derive(Debug)]
pub struct Broker {
    broker_meta: BrokerMeta, // this
    cluster_meta: ClusterMeta,
    topics: BTreeMap<String, TopicMeta>,
    producers: AtomicI64,
    topic_partition_store: BTreeMap<(uuid::Uuid, i32), Vec<bytes::Bytes>>,
    group_coordinator: GroupCoordinator,
}

impl Broker {
    pub fn new(broker_meta: BrokerMeta, cluster_meta: ClusterMeta) -> Broker {
        Broker {
            broker_meta,
            cluster_meta,
            topics: BTreeMap::new(),
            producers: AtomicI64::new(1),
            topic_partition_store: BTreeMap::new(),
            group_coordinator: GroupCoordinator::default(),
        }
    }

    pub fn reply(&mut self, client_info: ClientInfo, request: Request) -> Response {
        let response = match request {
            Request::ApiVersionsRequest(request) => {
                Response::ApiVersionsResponse(self.receive_api_versions(request))
            }
            Request::CreateTopicRequest(request) => {
                Response::CreateTopicsResponse(self.receive_create_topic(request))
            }
            Request::FindCoordinatorRequest(request) => {
                Response::FindCoordinatorResponse(self.receive_find_coordinator(request))
            }
            Request::MetadataRequest(request) => {
                Response::MetadataResponse(self.receive_metadata(request))
            }
            Request::InitProducerIdRequest(request) => {
                Response::InitProducerIdResponse(self.receive_init_producer(request))
            }
            Request::JoinGroupRequest(request) => {
                Response::JoinGroupResponse(self.receive_join_group(client_info, request))
            }
            Request::OffsetFetchRequest(request) => {
                Response::OffsetFetchResponse(self.receive_offset_fetch(request))
            }
            Request::ProduceRequest(request) => {
                Response::ProduceResponse(self.receive_produce(request))
            }
            Request::SyncGroupRequest(request) => {
                Response::SyncGroupResponse(self.receive_sync_group(request))
            }
        };
        trace!("Broker state: {self:?}");
        debug!("Reply {response:?}");
        response
    }

    fn receive_api_versions(&mut self, _request: ApiVersionsRequest) -> ApiVersionsResponse {
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

    fn receive_metadata(&mut self, _request: MetadataRequest) -> MetadataResponse {
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

    fn receive_create_topic(&mut self, request: CreateTopicsRequest) -> CreateTopicsResponse {
        let mut topics = vec![];
        for topic in request.topics.iter() {
            let topic_id = uuid::Uuid::new_v4();
            let topic_name = topic.name.clone();
            let num_partitions = max(topic.num_partitions, 1);

            let mut partitions = BTreeMap::new();
            for idx in 0..num_partitions {
                partitions.insert(idx, PartitionMeta {});
                self.topic_partition_store.insert((topic_id, idx), vec![]);
            }

            // TODO - return error on topic already exist
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

    fn receive_init_producer(&mut self, _request: InitProducerIdRequest) -> InitProducerIdResponse {
        InitProducerIdResponse {
            producer_id: self.producers.fetch_add(1, Ordering::SeqCst),
            producer_epoch: 0,
            ..Default::default()
        }
    }

    fn receive_produce(&mut self, request: ProduceRequest) -> ProduceResponse {
        let mut responses = vec![];
        for topic in request.topic_data {
            let topic_name = topic.name.clone();
            // TODO - return error on topic not exist
            let topic_meta = self.topics.get(&topic_name).unwrap();
            let mut partition_responses = vec![];
            for partition in topic.partition_data {
                let idx = partition.index;
                if let Some(record) = partition.records {
                    // TODO - return error on topic partition not exist
                    let store = self
                        .topic_partition_store
                        .get_mut(&(topic_meta.topic_id, idx))
                        .unwrap();
                    store.push(record);
                }
                partition_responses.push(PartitionProduceResponse {
                    index: idx,
                    ..Default::default()
                });
            }
            responses.push(TopicProduceResponse {
                name: topic_name,
                partition_responses,
                ..Default::default()
            });
        }

        ProduceResponse {
            responses,
            ..Default::default()
        }
    }

    fn receive_find_coordinator(
        &mut self,
        request: FindCoordinatorRequest,
    ) -> FindCoordinatorResponse {
        let mut coordinators = vec![];
        // TODO - support lower versions
        for key in request.coordinator_keys {
            coordinators.push(Coordinator {
                key,
                node_id: self.broker_meta.node_id,
                host: self.broker_meta.host.clone(),
                port: self.broker_meta.port,
                ..Default::default()
            });
        }

        FindCoordinatorResponse {
            coordinators,
            ..Default::default()
        }
    }

    fn receive_join_group(
        &mut self,
        client_info: ClientInfo,
        request: JoinGroupRequest,
    ) -> JoinGroupResponse {
        let group_id = request.group_id.clone();

        let group = match self.group_coordinator.groups.entry(group_id.clone()) {
            Entry::Vacant(entry) => {
                if request.member_id.is_empty() {
                    entry.insert(GroupMeta::new(group_id.clone()))
                } else {
                    // Only try to create the group the member_id is empty - if member_id is
                    // specified but group does not exist, the request would be rejected.
                    return JoinGroupResponse {
                        error_code: Error::UNKNOWN_MEMBER_ID.code(),
                        member_id: request.member_id,
                        ..Default::default()
                    };
                }
            }
            Entry::Occupied(entry) => entry.into_mut(),
        };

        let member_id = if request.member_id.is_empty() {
            let random_suffix = uuid::Uuid::new_v4().to_string();
            format!("{}-{}", client_info.client_id, random_suffix)
        } else {
            request.member_id
        };

        let member = MemberMeta {
            group_id,
            member_id: member_id.clone(),
            client_id: client_info.client_id.clone(),
            client_host: client_info.client_host,
            protocol_type: request.protocol_type.clone(),
            protocols: request
                .protocols
                .iter()
                .cloned()
                .map(|p| (p.name, p.metadata))
                .collect(),
            assignment: bytes::Bytes::new(),
            rebalance_timeout_ms: request.rebalance_timeout_ms,
            session_timeout_ms: request.session_timeout_ms,
        };

        group.add(member);

        JoinGroupResponse {
            generation_id: group.generation_id,
            protocol_type: group.protocol_type.clone(),
            protocol_name: group.protocol.clone(),
            leader: group.leader_id.clone().unwrap(),
            member_id,
            members: group
                .members
                .values()
                .map(|member| JoinGroupResponseMember {
                    member_id: member.member_id.clone(),
                    metadata: group
                        .protocol
                        .as_ref()
                        .and_then(|p| member.protocols.get(p))
                        .cloned()
                        .unwrap_or_default(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn receive_sync_group(&mut self, request: SyncGroupRequest) -> SyncGroupResponse {
        let assignments = request
            .assignments
            .iter()
            .cloned()
            .map(|assign| (assign.member_id, assign.assignment))
            .collect::<BTreeMap<String, bytes::Bytes>>();
        let group = self
            .group_coordinator
            .groups
            .get_mut(&request.group_id)
            .unwrap();
        group.sync(assignments);

        let member = group.members.get(&request.member_id).unwrap();

        SyncGroupResponse {
            protocol_type: request.protocol_type.clone(),
            protocol_name: request.protocol_name.clone(),
            assignment: member.assignment.clone(),
            ..Default::default()
        }
    }

    fn receive_offset_fetch(&mut self, request: OffsetFetchRequest) -> OffsetFetchResponse {
        let mut groups = vec![];
        for group in request.groups.iter() {
            let mut topics = vec![];
            for topic in group.topics.iter() {
                let mut partitions = vec![];
                for idx in topic.partition_indexes.iter() {
                    partitions.push(OffsetFetchResponsePartitions {
                        partition_index: *idx,
                        committed_offset: 0,
                        committed_leader_epoch: 0,
                        ..Default::default()
                    });
                }
                topics.push(OffsetFetchResponseTopics {
                    name: topic.name.clone(),
                    partitions,
                    ..Default::default()
                });
            }
            groups.push(OffsetFetchResponseGroup {
                group_id: group.group_id.clone(),
                topics,
                ..Default::default()
            });
        }
        OffsetFetchResponse {
            groups,
            ..Default::default()
        }
    }
}

const fn supported_apis() -> &'static [ApiMessageType] {
    &[
        ApiMessageType::API_VERSIONS,
        ApiMessageType::CREATE_TOPICS,
        ApiMessageType::FIND_COORDINATOR,
        ApiMessageType::INIT_PRODUCER_ID,
        ApiMessageType::JOIN_GROUP,
        ApiMessageType::METADATA,
        ApiMessageType::OFFSET_FETCH,
        ApiMessageType::PRODUCE,
        ApiMessageType::SYNC_GROUP,
    ]
}
