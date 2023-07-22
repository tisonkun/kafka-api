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
    pub latest_version_unstable: bool,
}

impl ApiMessageType {
    pub const PRODUCE: Self = ApiMessageType::new(0, 0, 9, false);
    pub const FETCH: Self = ApiMessageType::new(1, 0, 15, false);
    pub const LIST_OFFSETS: Self = ApiMessageType::new(2, 0, 8, false);
    pub const METADATA: Self = ApiMessageType::new(3, 0, 12, false);
    pub const LEADER_AND_ISR: Self = ApiMessageType::new(4, 0, 7, false);
    pub const STOP_REPLICA: Self = ApiMessageType::new(5, 0, 4, false);
    pub const UPDATE_METADATA: Self = ApiMessageType::new(6, 0, 8, false);
    pub const CONTROLLED_SHUTDOWN: Self = ApiMessageType::new(7, 0, 3, false);
    pub const OFFSET_COMMIT: Self = ApiMessageType::new(8, 0, 8, false);
    pub const OFFSET_FETCH: Self = ApiMessageType::new(9, 0, 8, false);
    pub const FIND_COORDINATOR: Self = ApiMessageType::new(10, 0, 4, false);
    pub const JOIN_GROUP: Self = ApiMessageType::new(11, 0, 9, false);
    pub const HEARTBEAT: Self = ApiMessageType::new(12, 0, 4, false);
    pub const LEAVE_GROUP: Self = ApiMessageType::new(13, 0, 5, false);
    pub const SYNC_GROUP: Self = ApiMessageType::new(14, 0, 5, false);
    pub const DESCRIBE_GROUPS: Self = ApiMessageType::new(15, 0, 5, false);
    pub const LIST_GROUPS: Self = ApiMessageType::new(16, 0, 4, false);
    pub const SASL_HANDSHAKE: Self = ApiMessageType::new(17, 0, 1, false);
    pub const API_VERSIONS: Self = ApiMessageType::new(18, 0, 3, false);
    pub const CREATE_TOPICS: Self = ApiMessageType::new(19, 0, 7, false);
    pub const DELETE_TOPICS: Self = ApiMessageType::new(20, 0, 6, false);
    pub const DELETE_RECORDS: Self = ApiMessageType::new(21, 0, 2, false);
    pub const INIT_PRODUCER_ID: Self = ApiMessageType::new(22, 0, 4, false);
    pub const OFFSET_FOR_LEADER_EPOCH: Self = ApiMessageType::new(23, 0, 4, false);
    pub const ADD_PARTITIONS_TO_TXN: Self = ApiMessageType::new(24, 0, 4, false);
    pub const ADD_OFFSETS_TO_TXN: Self = ApiMessageType::new(25, 0, 3, false);
    pub const END_TXN: Self = ApiMessageType::new(26, 0, 3, false);
    pub const WRITE_TXN_MARKERS: Self = ApiMessageType::new(27, 0, 1, false);
    pub const TXN_OFFSET_COMMIT: Self = ApiMessageType::new(28, 0, 3, false);
    pub const DESCRIBE_ACLS: Self = ApiMessageType::new(29, 0, 3, false);
    pub const CREATE_ACLS: Self = ApiMessageType::new(30, 0, 3, false);
    pub const DELETE_ACLS: Self = ApiMessageType::new(31, 0, 3, false);
    pub const DESCRIBE_CONFIGS: Self = ApiMessageType::new(32, 0, 4, false);
    pub const ALTER_CONFIGS: Self = ApiMessageType::new(33, 0, 2, false);
    pub const ALTER_REPLICA_LOG_DIRS: Self = ApiMessageType::new(34, 0, 2, false);
    pub const DESCRIBE_LOG_DIRS: Self = ApiMessageType::new(35, 0, 4, false);
    pub const SASL_AUTHENTICATE: Self = ApiMessageType::new(36, 0, 2, false);
    pub const CREATE_PARTITIONS: Self = ApiMessageType::new(37, 0, 3, false);
    pub const CREATE_DELEGATION_TOKEN: Self = ApiMessageType::new(38, 0, 3, false);
    pub const RENEW_DELEGATION_TOKEN: Self = ApiMessageType::new(39, 0, 2, false);
    pub const EXPIRE_DELEGATION_TOKEN: Self = ApiMessageType::new(40, 0, 2, false);
    pub const DESCRIBE_DELEGATION_TOKEN: Self = ApiMessageType::new(41, 0, 3, false);
    pub const DELETE_GROUPS: Self = ApiMessageType::new(42, 0, 2, false);
    pub const ELECT_LEADERS: Self = ApiMessageType::new(43, 0, 2, false);
    pub const INCREMENTAL_ALTER_CONFIGS: Self = ApiMessageType::new(44, 0, 1, false);
    pub const ALTER_PARTITION_REASSIGNMENTS: Self = ApiMessageType::new(45, 0, 0, false);
    pub const LIST_PARTITION_REASSIGNMENTS: Self = ApiMessageType::new(46, 0, 0, false);
    pub const OFFSET_DELETE: Self = ApiMessageType::new(47, 0, 0, false);
    pub const DESCRIBE_CLIENT_QUOTAS: Self = ApiMessageType::new(48, 0, 1, false);
    pub const ALTER_CLIENT_QUOTAS: Self = ApiMessageType::new(49, 0, 1, false);
    pub const DESCRIBE_USER_SCRAM_CREDENTIALS: Self = ApiMessageType::new(50, 0, 0, false);
    pub const ALTER_USER_SCRAM_CREDENTIALS: Self = ApiMessageType::new(51, 0, 0, false);
    pub const VOTE: Self = ApiMessageType::new(52, 0, 0, false);
    pub const BEGIN_QUORUM_EPOCH: Self = ApiMessageType::new(53, 0, 0, false);
    pub const END_QUORUM_EPOCH: Self = ApiMessageType::new(54, 0, 0, false);
    pub const DESCRIBE_QUORUM: Self = ApiMessageType::new(55, 0, 1, false);
    pub const ALTER_PARTITION: Self = ApiMessageType::new(56, 0, 3, false);
    pub const UPDATE_FEATURES: Self = ApiMessageType::new(57, 0, 1, false);
    pub const ENVELOPE: Self = ApiMessageType::new(58, 0, 0, false);
    pub const FETCH_SNAPSHOT: Self = ApiMessageType::new(59, 0, 0, false);
    pub const DESCRIBE_CLUSTER: Self = ApiMessageType::new(60, 0, 0, false);
    pub const DESCRIBE_PRODUCERS: Self = ApiMessageType::new(61, 0, 0, false);
    pub const BROKER_REGISTRATION: Self = ApiMessageType::new(62, 0, 1, false);
    pub const BROKER_HEARTBEAT: Self = ApiMessageType::new(63, 0, 0, false);
    pub const UNREGISTER_BROKER: Self = ApiMessageType::new(64, 0, 0, false);
    pub const DESCRIBE_TRANSACTIONS: Self = ApiMessageType::new(65, 0, 0, false);
    pub const LIST_TRANSACTIONS: Self = ApiMessageType::new(66, 0, 0, false);
    pub const ALLOCATE_PRODUCER_IDS: Self = ApiMessageType::new(67, 0, 0, false);
    pub const CONSUMER_GROUP_HEARTBEAT: Self = ApiMessageType::new(68, 0, 0, true);

    const fn new(
        api_key: i16,
        lowest_supported_version: i16,
        highest_supported_version: i16,
        latest_version_unstable: bool,
    ) -> Self {
        Self {
            api_key,
            lowest_supported_version,
            highest_supported_version,
            latest_version_unstable,
        }
    }
}

impl TryFrom<i16> for ApiMessageType {
    type Error = io::Error;

    fn try_from(api_key: i16) -> Result<Self, Self::Error> {
        match api_key {
            0 => Ok(ApiMessageType::PRODUCE),
            1 => Ok(ApiMessageType::FETCH),
            2 => Ok(ApiMessageType::LIST_OFFSETS),
            3 => Ok(ApiMessageType::METADATA),
            4 => Ok(ApiMessageType::LEADER_AND_ISR),
            5 => Ok(ApiMessageType::STOP_REPLICA),
            6 => Ok(ApiMessageType::UPDATE_METADATA),
            7 => Ok(ApiMessageType::CONTROLLED_SHUTDOWN),
            8 => Ok(ApiMessageType::OFFSET_COMMIT),
            9 => Ok(ApiMessageType::OFFSET_FETCH),
            10 => Ok(ApiMessageType::FIND_COORDINATOR),
            11 => Ok(ApiMessageType::JOIN_GROUP),
            12 => Ok(ApiMessageType::HEARTBEAT),
            13 => Ok(ApiMessageType::LEAVE_GROUP),
            14 => Ok(ApiMessageType::SYNC_GROUP),
            15 => Ok(ApiMessageType::DESCRIBE_GROUPS),
            16 => Ok(ApiMessageType::LIST_GROUPS),
            17 => Ok(ApiMessageType::SASL_HANDSHAKE),
            18 => Ok(ApiMessageType::API_VERSIONS),
            19 => Ok(ApiMessageType::CREATE_TOPICS),
            20 => Ok(ApiMessageType::DELETE_TOPICS),
            21 => Ok(ApiMessageType::DELETE_RECORDS),
            22 => Ok(ApiMessageType::INIT_PRODUCER_ID),
            23 => Ok(ApiMessageType::OFFSET_FOR_LEADER_EPOCH),
            24 => Ok(ApiMessageType::ADD_PARTITIONS_TO_TXN),
            25 => Ok(ApiMessageType::ADD_OFFSETS_TO_TXN),
            26 => Ok(ApiMessageType::END_TXN),
            27 => Ok(ApiMessageType::WRITE_TXN_MARKERS),
            28 => Ok(ApiMessageType::TXN_OFFSET_COMMIT),
            29 => Ok(ApiMessageType::DESCRIBE_ACLS),
            30 => Ok(ApiMessageType::CREATE_ACLS),
            31 => Ok(ApiMessageType::DELETE_ACLS),
            32 => Ok(ApiMessageType::DESCRIBE_CONFIGS),
            33 => Ok(ApiMessageType::ALTER_CONFIGS),
            34 => Ok(ApiMessageType::ALTER_REPLICA_LOG_DIRS),
            35 => Ok(ApiMessageType::DESCRIBE_LOG_DIRS),
            36 => Ok(ApiMessageType::SASL_AUTHENTICATE),
            37 => Ok(ApiMessageType::CREATE_PARTITIONS),
            38 => Ok(ApiMessageType::CREATE_DELEGATION_TOKEN),
            39 => Ok(ApiMessageType::RENEW_DELEGATION_TOKEN),
            40 => Ok(ApiMessageType::EXPIRE_DELEGATION_TOKEN),
            41 => Ok(ApiMessageType::DESCRIBE_DELEGATION_TOKEN),
            42 => Ok(ApiMessageType::DELETE_GROUPS),
            43 => Ok(ApiMessageType::ELECT_LEADERS),
            44 => Ok(ApiMessageType::INCREMENTAL_ALTER_CONFIGS),
            45 => Ok(ApiMessageType::ALTER_PARTITION_REASSIGNMENTS),
            46 => Ok(ApiMessageType::LIST_PARTITION_REASSIGNMENTS),
            47 => Ok(ApiMessageType::OFFSET_DELETE),
            48 => Ok(ApiMessageType::DESCRIBE_CLIENT_QUOTAS),
            49 => Ok(ApiMessageType::ALTER_CLIENT_QUOTAS),
            50 => Ok(ApiMessageType::DESCRIBE_USER_SCRAM_CREDENTIALS),
            51 => Ok(ApiMessageType::ALTER_USER_SCRAM_CREDENTIALS),
            52 => Ok(ApiMessageType::VOTE),
            53 => Ok(ApiMessageType::BEGIN_QUORUM_EPOCH),
            54 => Ok(ApiMessageType::END_QUORUM_EPOCH),
            55 => Ok(ApiMessageType::DESCRIBE_QUORUM),
            56 => Ok(ApiMessageType::ALTER_PARTITION),
            57 => Ok(ApiMessageType::UPDATE_FEATURES),
            58 => Ok(ApiMessageType::ENVELOPE),
            59 => Ok(ApiMessageType::FETCH_SNAPSHOT),
            60 => Ok(ApiMessageType::DESCRIBE_CLUSTER),
            61 => Ok(ApiMessageType::DESCRIBE_PRODUCERS),
            62 => Ok(ApiMessageType::BROKER_REGISTRATION),
            63 => Ok(ApiMessageType::BROKER_HEARTBEAT),
            64 => Ok(ApiMessageType::UNREGISTER_BROKER),
            65 => Ok(ApiMessageType::DESCRIBE_TRANSACTIONS),
            66 => Ok(ApiMessageType::LIST_TRANSACTIONS),
            67 => Ok(ApiMessageType::ALLOCATE_PRODUCER_IDS),
            68 => Ok(ApiMessageType::CONSUMER_GROUP_HEARTBEAT),
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
            ApiMessageType::PRODUCE => resolve_request_header_version(api_version >= 9),
            ApiMessageType::FETCH => resolve_request_header_version(api_version >= 12),
            ApiMessageType::LIST_OFFSETS => resolve_request_header_version(api_version >= 6),
            ApiMessageType::METADATA => resolve_request_header_version(api_version >= 9),
            ApiMessageType::LEADER_AND_ISR => resolve_request_header_version(api_version >= 4),
            ApiMessageType::STOP_REPLICA => resolve_request_header_version(api_version >= 2),
            ApiMessageType::UPDATE_METADATA => resolve_request_header_version(api_version >= 6),
            ApiMessageType::CONTROLLED_SHUTDOWN => {
                // Version 0 of ControlledShutdownRequest has a non-standard request header
                // which does not include clientId.  Version 1 of ControlledShutdownRequest
                // and later use the standard request header.
                if api_version == 0 {
                    0
                } else {
                    resolve_request_header_version(api_version >= 3)
                }
            }
            ApiMessageType::OFFSET_COMMIT => resolve_request_header_version(api_version >= 8),
            ApiMessageType::OFFSET_FETCH => resolve_request_header_version(api_version >= 6),
            ApiMessageType::FIND_COORDINATOR => resolve_request_header_version(api_version >= 3),
            ApiMessageType::JOIN_GROUP => resolve_request_header_version(api_version >= 6),
            ApiMessageType::HEARTBEAT => resolve_request_header_version(api_version >= 4),
            ApiMessageType::LEAVE_GROUP => resolve_request_header_version(api_version >= 4),
            ApiMessageType::SYNC_GROUP => resolve_request_header_version(api_version >= 4),
            ApiMessageType::DESCRIBE_GROUPS => resolve_request_header_version(api_version >= 5),
            ApiMessageType::LIST_GROUPS => resolve_request_header_version(api_version >= 3),
            ApiMessageType::SASL_HANDSHAKE => {
                // The flexible version is none.
                // @see https://issues.apache.org/jira/browse/KAFKA-9577
                1
            }
            ApiMessageType::API_VERSIONS => resolve_request_header_version(api_version >= 3),
            ApiMessageType::CREATE_TOPICS => resolve_request_header_version(api_version >= 5),
            ApiMessageType::DELETE_TOPICS => resolve_request_header_version(api_version >= 4),
            ApiMessageType::DELETE_RECORDS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::INIT_PRODUCER_ID => resolve_request_header_version(api_version >= 2),
            ApiMessageType::OFFSET_FOR_LEADER_EPOCH => {
                resolve_request_header_version(api_version >= 4)
            }
            ApiMessageType::ADD_PARTITIONS_TO_TXN => {
                resolve_request_header_version(api_version >= 3)
            }
            ApiMessageType::ADD_OFFSETS_TO_TXN => resolve_request_header_version(api_version >= 3),
            ApiMessageType::END_TXN => resolve_request_header_version(api_version >= 3),
            ApiMessageType::WRITE_TXN_MARKERS => resolve_request_header_version(api_version >= 1),
            ApiMessageType::TXN_OFFSET_COMMIT => resolve_request_header_version(api_version >= 3),
            ApiMessageType::DESCRIBE_ACLS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::CREATE_ACLS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::DELETE_ACLS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::DESCRIBE_CONFIGS => resolve_request_header_version(api_version >= 4),
            ApiMessageType::ALTER_CONFIGS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::ALTER_REPLICA_LOG_DIRS => {
                resolve_request_header_version(api_version >= 2)
            }
            ApiMessageType::DESCRIBE_LOG_DIRS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::SASL_AUTHENTICATE => resolve_request_header_version(api_version >= 2),
            ApiMessageType::CREATE_PARTITIONS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::CREATE_DELEGATION_TOKEN => {
                resolve_request_header_version(api_version >= 2)
            }
            ApiMessageType::RENEW_DELEGATION_TOKEN => {
                resolve_request_header_version(api_version >= 2)
            }
            ApiMessageType::EXPIRE_DELEGATION_TOKEN => {
                resolve_request_header_version(api_version >= 2)
            }
            ApiMessageType::DESCRIBE_DELEGATION_TOKEN => {
                resolve_request_header_version(api_version >= 2)
            }
            ApiMessageType::DELETE_GROUPS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::ELECT_LEADERS => resolve_request_header_version(api_version >= 2),
            ApiMessageType::INCREMENTAL_ALTER_CONFIGS => {
                resolve_request_header_version(api_version >= 1)
            }
            ApiMessageType::ALTER_PARTITION_REASSIGNMENTS => 2,
            ApiMessageType::LIST_PARTITION_REASSIGNMENTS => 2,
            ApiMessageType::OFFSET_DELETE => {
                // The flexible version is none. Perhaps omitted when proceeding https://issues.apache.org/jira/browse/KAFKA-8885.
                // Ditto for those below.
                1
            }
            ApiMessageType::DESCRIBE_CLIENT_QUOTAS => {
                resolve_request_header_version(api_version >= 1)
            }
            ApiMessageType::ALTER_CLIENT_QUOTAS => resolve_request_header_version(api_version >= 1),
            ApiMessageType::DESCRIBE_USER_SCRAM_CREDENTIALS => 2,
            ApiMessageType::ALTER_USER_SCRAM_CREDENTIALS => 2,
            ApiMessageType::VOTE => 2,
            ApiMessageType::BEGIN_QUORUM_EPOCH => 1,
            ApiMessageType::END_QUORUM_EPOCH => 1,
            ApiMessageType::DESCRIBE_QUORUM => 2,
            ApiMessageType::ALTER_PARTITION => 2,
            ApiMessageType::UPDATE_FEATURES => 2,
            ApiMessageType::ENVELOPE => 2,
            ApiMessageType::FETCH_SNAPSHOT => 2,
            ApiMessageType::DESCRIBE_CLUSTER => 2,
            ApiMessageType::DESCRIBE_PRODUCERS => 2,
            ApiMessageType::BROKER_REGISTRATION => 2,
            ApiMessageType::BROKER_HEARTBEAT => 2,
            ApiMessageType::UNREGISTER_BROKER => 2,
            ApiMessageType::DESCRIBE_TRANSACTIONS => 2,
            ApiMessageType::LIST_TRANSACTIONS => 2,
            ApiMessageType::ALLOCATE_PRODUCER_IDS => 2,
            ApiMessageType::CONSUMER_GROUP_HEARTBEAT => 2,
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
            ApiMessageType::PRODUCE => resolve_response_header_version(api_version >= 9),
            ApiMessageType::FETCH => resolve_response_header_version(api_version >= 12),
            ApiMessageType::LIST_OFFSETS => resolve_response_header_version(api_version >= 6),
            ApiMessageType::METADATA => resolve_response_header_version(api_version >= 9),
            ApiMessageType::LEADER_AND_ISR => resolve_response_header_version(api_version >= 4),
            ApiMessageType::STOP_REPLICA => resolve_response_header_version(api_version >= 2),
            ApiMessageType::UPDATE_METADATA => resolve_response_header_version(api_version >= 6),
            ApiMessageType::CONTROLLED_SHUTDOWN => {
                resolve_response_header_version(api_version >= 3)
            }
            ApiMessageType::OFFSET_COMMIT => resolve_response_header_version(api_version >= 8),
            ApiMessageType::OFFSET_FETCH => resolve_response_header_version(api_version >= 6),
            ApiMessageType::FIND_COORDINATOR => resolve_response_header_version(api_version >= 3),
            ApiMessageType::JOIN_GROUP => resolve_response_header_version(api_version >= 6),
            ApiMessageType::HEARTBEAT => resolve_response_header_version(api_version >= 4),
            ApiMessageType::LEAVE_GROUP => resolve_response_header_version(api_version >= 4),
            ApiMessageType::SYNC_GROUP => resolve_response_header_version(api_version >= 4),
            ApiMessageType::DESCRIBE_GROUPS => resolve_response_header_version(api_version >= 5),
            ApiMessageType::LIST_GROUPS => resolve_response_header_version(api_version >= 3),
            ApiMessageType::SASL_HANDSHAKE => {
                // The flexible version is none.
                // @see https://issues.apache.org/jira/browse/KAFKA-9577
                0
            }
            ApiMessageType::API_VERSIONS => {
                // ApiVersionsResponse always includes a v0 header.
                // @see KIP-511 https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers
                0
            }
            ApiMessageType::CREATE_TOPICS => resolve_response_header_version(api_version >= 5),
            ApiMessageType::DELETE_TOPICS => resolve_response_header_version(api_version >= 4),
            ApiMessageType::DELETE_RECORDS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::INIT_PRODUCER_ID => resolve_response_header_version(api_version >= 2),
            ApiMessageType::OFFSET_FOR_LEADER_EPOCH => {
                resolve_response_header_version(api_version >= 4)
            }
            ApiMessageType::ADD_PARTITIONS_TO_TXN => {
                resolve_response_header_version(api_version >= 3)
            }
            ApiMessageType::ADD_OFFSETS_TO_TXN => resolve_response_header_version(api_version >= 3),
            ApiMessageType::END_TXN => resolve_response_header_version(api_version >= 3),
            ApiMessageType::WRITE_TXN_MARKERS => resolve_response_header_version(api_version >= 1),
            ApiMessageType::TXN_OFFSET_COMMIT => resolve_response_header_version(api_version >= 3),
            ApiMessageType::DESCRIBE_ACLS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::CREATE_ACLS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::DELETE_ACLS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::DESCRIBE_CONFIGS => resolve_response_header_version(api_version >= 4),
            ApiMessageType::ALTER_CONFIGS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::ALTER_REPLICA_LOG_DIRS => {
                resolve_response_header_version(api_version >= 2)
            }
            ApiMessageType::DESCRIBE_LOG_DIRS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::SASL_AUTHENTICATE => resolve_response_header_version(api_version >= 2),
            ApiMessageType::CREATE_PARTITIONS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::CREATE_DELEGATION_TOKEN => {
                resolve_response_header_version(api_version >= 2)
            }
            ApiMessageType::RENEW_DELEGATION_TOKEN => {
                resolve_response_header_version(api_version >= 2)
            }
            ApiMessageType::EXPIRE_DELEGATION_TOKEN => {
                resolve_response_header_version(api_version >= 2)
            }
            ApiMessageType::DESCRIBE_DELEGATION_TOKEN => {
                resolve_response_header_version(api_version >= 2)
            }
            ApiMessageType::DELETE_GROUPS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::ELECT_LEADERS => resolve_response_header_version(api_version >= 2),
            ApiMessageType::INCREMENTAL_ALTER_CONFIGS => {
                resolve_response_header_version(api_version >= 1)
            }
            ApiMessageType::ALTER_PARTITION_REASSIGNMENTS => 1,
            ApiMessageType::LIST_PARTITION_REASSIGNMENTS => 1,
            ApiMessageType::OFFSET_DELETE => {
                // The flexible version is none. Perhaps omitted when proceeding https://issues.apache.org/jira/browse/KAFKA-8885.
                // Ditto for those below.
                0
            }
            ApiMessageType::DESCRIBE_CLIENT_QUOTAS => {
                resolve_response_header_version(api_version >= 1)
            }
            ApiMessageType::ALTER_CLIENT_QUOTAS => {
                resolve_response_header_version(api_version >= 1)
            }
            ApiMessageType::DESCRIBE_USER_SCRAM_CREDENTIALS => 1,
            ApiMessageType::ALTER_USER_SCRAM_CREDENTIALS => 1,
            ApiMessageType::VOTE => 1,
            ApiMessageType::BEGIN_QUORUM_EPOCH => 0,
            ApiMessageType::END_QUORUM_EPOCH => 0,
            ApiMessageType::DESCRIBE_QUORUM => 1,
            ApiMessageType::ALTER_PARTITION => 1,
            ApiMessageType::UPDATE_FEATURES => 1,
            ApiMessageType::ENVELOPE => 1,
            ApiMessageType::FETCH_SNAPSHOT => 1,
            ApiMessageType::DESCRIBE_CLUSTER => 1,
            ApiMessageType::DESCRIBE_PRODUCERS => 1,
            ApiMessageType::BROKER_REGISTRATION => 1,
            ApiMessageType::BROKER_HEARTBEAT => 1,
            ApiMessageType::UNREGISTER_BROKER => 1,
            ApiMessageType::DESCRIBE_TRANSACTIONS => 1,
            ApiMessageType::LIST_TRANSACTIONS => 1,
            ApiMessageType::ALLOCATE_PRODUCER_IDS => 1,
            ApiMessageType::CONSUMER_GROUP_HEARTBEAT => 1,
            _ => unreachable!("unknown api type {}", self.api_key),
        }
    }
}
