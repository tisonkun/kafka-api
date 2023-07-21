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

// Derived - https://kafka.apache.org/protocol.html#protocol_error_codes
// Upstream baseline commit - https://github.com/apache/kafka/commit/3be7f7d611d0786f2f98159d5c7492b0d94a2bb7

#[derive(Debug, Clone)]
pub struct Error {
    code: i16,
    message: &'static str,
    retryable: bool,
}

impl Error {
    pub const UNKNOWN_SERVER_ERROR: Error = Error::new(
        -1,
        "The server experienced an unexpected error when processing the request.",
        false,
    );
    pub const NONE: Error = Error::new(0, "", false);
    pub const OFFSET_OUT_OF_RANGE: Error = Error::new(
        1,
        "The requested offset is not within the range of offsets maintained by the server.",
        false,
    );
    pub const CORRUPT_MESSAGE: Error = Error::new(2, "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.", true);
    pub const UNKNOWN_TOPIC_OR_PARTITION: Error =
        Error::new(3, "This server does not host this topic-partition.", true);
    pub const INVALID_FETCH_SIZE: Error =
        Error::new(4, "The requested fetch size is invalid.", false);
    pub const LEADER_NOT_AVAILABLE: Error = Error::new(5, "There is no leader for this topic-partition as we are in the middle of a leadership election.", true);
    pub const NOT_LEADER_OR_FOLLOWER: Error = Error::new(6, "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.", true);
    pub const REQUEST_TIMED_OUT: Error = Error::new(7, "The request timed out.", true);
    pub const BROKER_NOT_AVAILABLE: Error = Error::new(8, "The broker is not available.", false);
    pub const REPLICA_NOT_AVAILABLE: Error = Error::new(9, "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.", true);
    pub const MESSAGE_TOO_LARGE: Error = Error::new(
        10,
        "The request included a message larger than the max message size the server will accept.",
        false,
    );
    pub const STALE_CONTROLLER_EPOCH: Error =
        Error::new(11, "The controller moved to another broker.", false);
    pub const OFFSET_METADATA_TOO_LARGE: Error = Error::new(
        12,
        "The metadata field of the offset request was too large.",
        false,
    );
    pub const NETWORK_EXCEPTION: Error = Error::new(
        13,
        "The server disconnected before a response was received.",
        true,
    );
    pub const COORDINATOR_LOAD_IN_PROGRESS: Error = Error::new(
        14,
        "The coordinator is loading and hence can't process requests.",
        true,
    );
    pub const COORDINATOR_NOT_AVAILABLE: Error =
        Error::new(15, "The coordinator is not available.", true);
    pub const NOT_COORDINATOR: Error = Error::new(16, "This is not the correct coordinator.", true);
    pub const INVALID_TOPIC_EXCEPTION: Error = Error::new(
        17,
        "The request attempted to perform an operation on an invalid topic.",
        false,
    );
    pub const RECORD_LIST_TOO_LARGE: Error = Error::new(
        18,
        "The request included message batch larger than the configured segment size on the server.",
        false,
    );
    pub const NOT_ENOUGH_REPLICAS: Error = Error::new(
        19,
        "Messages are rejected since there are fewer in-sync replicas than required.",
        true,
    );
    pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: Error = Error::new(
        20,
        "Messages are written to the log, but to fewer in-sync replicas than required.",
        true,
    );
    pub const INVALID_REQUIRED_ACKS: Error = Error::new(
        21,
        "Produce request specified an invalid value for required acks.",
        false,
    );
    pub const ILLEGAL_GENERATION: Error =
        Error::new(22, "Specified group generation id is not valid.", false);
    pub const INCONSISTENT_GROUP_PROTOCOL: Error = Error::new(23, "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.", false);
    pub const INVALID_GROUP_ID: Error = Error::new(24, "The configured groupId is invalid.", false);
    pub const UNKNOWN_MEMBER_ID: Error =
        Error::new(25, "The coordinator is not aware of this member.", false);
    pub const INVALID_SESSION_TIMEOUT: Error = Error::new(26, "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).", false);
    pub const REBALANCE_IN_PROGRESS: Error = Error::new(
        27,
        "The group is rebalancing, so a rejoin is needed.",
        false,
    );
    pub const INVALID_COMMIT_OFFSET_SIZE: Error =
        Error::new(28, "The committing offset data size is not valid.", false);
    pub const TOPIC_AUTHORIZATION_FAILED: Error =
        Error::new(29, "Topic authorization failed.", false);
    pub const GROUP_AUTHORIZATION_FAILED: Error =
        Error::new(30, "Group authorization failed.", false);
    pub const CLUSTER_AUTHORIZATION_FAILED: Error =
        Error::new(31, "Cluster authorization failed.", false);
    pub const INVALID_TIMESTAMP: Error = Error::new(
        32,
        "The timestamp of the message is out of acceptable range.",
        false,
    );
    pub const UNSUPPORTED_SASL_MECHANISM: Error = Error::new(
        33,
        "The broker does not support the requested SASL mechanism.",
        false,
    );
    pub const ILLEGAL_SASL_STATE: Error = Error::new(
        34,
        "Request is not valid given the current SASL state.",
        false,
    );
    pub const UNSUPPORTED_VERSION: Error =
        Error::new(35, "The version of API is not supported.", false);
    pub const TOPIC_ALREADY_EXISTS: Error =
        Error::new(36, "Topic with this name already exists.", false);
    pub const INVALID_PARTITIONS: Error = Error::new(37, "Number of partitions is below 1.", false);
    pub const INVALID_REPLICATION_FACTOR: Error = Error::new(
        38,
        "Replication factor is below 1 or larger than the number of available brokers.",
        false,
    );
    pub const INVALID_REPLICA_ASSIGNMENT: Error =
        Error::new(39, "Replica assignment is invalid.", false);
    pub const INVALID_CONFIG: Error = Error::new(40, "Configuration is invalid.", false);
    pub const NOT_CONTROLLER: Error = Error::new(
        41,
        "This is not the correct controller for this cluster.",
        true,
    );
    pub const INVALID_REQUEST: Error = Error::new(42, "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.", false);
    pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: Error = Error::new(
        43,
        "The message format version on the broker does not support the request.",
        false,
    );
    pub const POLICY_VIOLATION: Error = Error::new(
        44,
        "Request parameters do not satisfy the configured policy.",
        false,
    );
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: Error = Error::new(
        45,
        "The broker received an out of order sequence number.",
        false,
    );
    pub const DUPLICATE_SEQUENCE_NUMBER: Error = Error::new(
        46,
        "The broker received a duplicate sequence number.",
        false,
    );
    pub const INVALID_PRODUCER_EPOCH: Error = Error::new(
        47,
        "Producer attempted to produce with an old epoch.",
        false,
    );
    pub const INVALID_TXN_STATE: Error = Error::new(
        48,
        "The producer attempted a transactional operation in an invalid state.",
        false,
    );
    pub const INVALID_PRODUCER_ID_MAPPING: Error = Error::new(49, "The producer attempted to use a producer id which is not currently assigned to its transactional id.", false);
    pub const INVALID_TRANSACTION_TIMEOUT: Error = Error::new(50, "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).", false);
    pub const CONCURRENT_TRANSACTIONS: Error = Error::new(51, "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.", true);
    pub const TRANSACTION_COORDINATOR_FENCED: Error = Error::new(52, "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.", false);
    pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: Error =
        Error::new(53, "Transactional Id authorization failed.", false);
    pub const SECURITY_DISABLED: Error = Error::new(54, "Security features are disabled.", false);
    pub const OPERATION_NOT_ATTEMPTED: Error = Error::new(55, "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.", false);
    pub const KAFKA_STORAGE_ERROR: Error = Error::new(
        56,
        "Disk error when trying to access log file on the disk.",
        true,
    );
    pub const LOG_DIR_NOT_FOUND: Error = Error::new(
        57,
        "The user-specified log directory is not found in the broker config.",
        false,
    );
    pub const SASL_AUTHENTICATION_FAILED: Error =
        Error::new(58, "SASL Authentication failed.", false);
    pub const UNKNOWN_PRODUCER_ID: Error = Error::new(59, "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.", false);
    pub const REASSIGNMENT_IN_PROGRESS: Error =
        Error::new(60, "A partition reassignment is in progress.", false);
    pub const DELEGATION_TOKEN_AUTH_DISABLED: Error =
        Error::new(61, "Delegation Token feature is not enabled.", false);
    pub const DELEGATION_TOKEN_NOT_FOUND: Error =
        Error::new(62, "Delegation Token is not found on server.", false);
    pub const DELEGATION_TOKEN_OWNER_MISMATCH: Error =
        Error::new(63, "Specified Principal is not valid Owner/Renewer.", false);
    pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: Error = Error::new(64, "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.", false);
    pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: Error =
        Error::new(65, "Delegation Token authorization failed.", false);
    pub const DELEGATION_TOKEN_EXPIRED: Error =
        Error::new(66, "Delegation Token is expired.", false);
    pub const INVALID_PRINCIPAL_TYPE: Error =
        Error::new(67, "Supplied principalType is not supported.", false);
    pub const NON_EMPTY_GROUP: Error = Error::new(68, "The group is not empty.", false);
    pub const GROUP_ID_NOT_FOUND: Error = Error::new(69, "The group id does not exist.", false);
    pub const FETCH_SESSION_ID_NOT_FOUND: Error =
        Error::new(70, "The fetch session ID was not found.", true);
    pub const INVALID_FETCH_SESSION_EPOCH: Error =
        Error::new(71, "The fetch session epoch is invalid.", true);
    pub const LISTENER_NOT_FOUND: Error = Error::new(72, "There is no listener on the leader broker that matches the listener on which metadata request was processed.", true);
    pub const TOPIC_DELETION_DISABLED: Error = Error::new(73, "Topic deletion is disabled.", false);
    pub const FENCED_LEADER_EPOCH: Error = Error::new(
        74,
        "The leader epoch in the request is older than the epoch on the broker.",
        true,
    );
    pub const UNKNOWN_LEADER_EPOCH: Error = Error::new(
        75,
        "The leader epoch in the request is newer than the epoch on the broker.",
        true,
    );
    pub const UNSUPPORTED_COMPRESSION_TYPE: Error = Error::new(
        76,
        "The requesting client does not support the compression type of given partition.",
        false,
    );
    pub const STALE_BROKER_EPOCH: Error = Error::new(77, "Broker epoch has changed.", false);
    pub const OFFSET_NOT_AVAILABLE: Error = Error::new(78, "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.", true);
    pub const MEMBER_ID_REQUIRED: Error = Error::new(79, "The group member needs to have a valid member id before actually entering a consumer group.", false);
    pub const PREFERRED_LEADER_NOT_AVAILABLE: Error =
        Error::new(80, "The preferred leader was not available.", true);
    pub const GROUP_MAX_SIZE_REACHED: Error =
        Error::new(81, "The consumer group has reached its max size.", false);
    pub const FENCED_INSTANCE_ID: Error = Error::new(82, "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.", false);
    pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: Error = Error::new(
        83,
        "Eligible topic partition leaders are not available.",
        true,
    );
    pub const ELECTION_NOT_NEEDED: Error =
        Error::new(84, "Leader election not needed for topic partition.", true);
    pub const NO_REASSIGNMENT_IN_PROGRESS: Error =
        Error::new(85, "No partition reassignment is in progress.", false);
    pub const GROUP_SUBSCRIBED_TO_TOPIC: Error = Error::new(86, "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.", false);
    pub const INVALID_RECORD: Error = Error::new(
        87,
        "This record has failed the validation on broker and hence will be rejected.",
        false,
    );
    pub const UNSTABLE_OFFSET_COMMIT: Error = Error::new(
        88,
        "There are unstable offsets that need to be cleared.",
        true,
    );
    pub const THROTTLING_QUOTA_EXCEEDED: Error =
        Error::new(89, "The throttling quota has been exceeded.", true);
    pub const PRODUCER_FENCED: Error = Error::new(
        90,
        "There is a newer producer with the same transactionalId which fences the current one.",
        false,
    );
    pub const RESOURCE_NOT_FOUND: Error = Error::new(
        91,
        "A request illegally referred to a resource that does not exist.",
        false,
    );
    pub const DUPLICATE_RESOURCE: Error = Error::new(
        92,
        "A request illegally referred to the same resource twice.",
        false,
    );
    pub const UNACCEPTABLE_CREDENTIAL: Error = Error::new(
        93,
        "Requested credential would not meet criteria for acceptability.",
        false,
    );
    pub const INCONSISTENT_VOTER_SET: Error = Error::new(94, "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters", false);
    pub const INVALID_UPDATE_VERSION: Error =
        Error::new(95, "The given update version was invalid.", false);
    pub const FEATURE_UPDATE_FAILED: Error = Error::new(
        96,
        "Unable to update finalized features due to an unexpected server error.",
        false,
    );
    pub const PRINCIPAL_DESERIALIZATION_FAILURE: Error = Error::new(97, "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.", false);
    pub const SNAPSHOT_NOT_FOUND: Error = Error::new(98, "Requested snapshot was not found", false);
    pub const POSITION_OUT_OF_RANGE: Error = Error::new(99, "Requested position is not greater than or equal to zero, and less than the size of the snapshot.", false);
    pub const UNKNOWN_TOPIC_ID: Error =
        Error::new(100, "This server does not host this topic ID.", true);
    pub const DUPLICATE_BROKER_REGISTRATION: Error =
        Error::new(101, "This broker ID is already in use.", false);
    pub const BROKER_ID_NOT_REGISTERED: Error =
        Error::new(102, "The given broker ID was not registered.", false);
    pub const INCONSISTENT_TOPIC_ID: Error = Error::new(
        103,
        "The log's topic ID did not match the topic ID in the request",
        true,
    );
    pub const INCONSISTENT_CLUSTER_ID: Error = Error::new(
        104,
        "The clusterId in the request does not match that found on the server",
        false,
    );
    pub const TRANSACTIONAL_ID_NOT_FOUND: Error =
        Error::new(105, "The transactionalId could not be found", false);
    pub const FETCH_SESSION_TOPIC_ID_ERROR: Error = Error::new(
        106,
        "The fetch session encountered inconsistent topic ID usage",
        true,
    );
    pub const INELIGIBLE_REPLICA: Error = Error::new(
        107,
        "The new ISR contains at least one ineligible replica.",
        false,
    );
    pub const NEW_LEADER_ELECTED: Error = Error::new(108, "The AlterPartition request successfully updated the partition state but the leader has changed.", false);
    pub const OFFSET_MOVED_TO_TIERED_STORAGE: Error = Error::new(
        109,
        "The requested offset is moved to tiered storage.",
        false,
    );
    pub const FENCED_MEMBER_EPOCH: Error = Error::new(110, "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.", false);
    pub const UNRELEASED_INSTANCE_ID: Error = Error::new(111, "The instance ID is still used by another member in the consumer group. That member must leave first.", false);
    pub const UNSUPPORTED_ASSIGNOR: Error = Error::new(
        112,
        "The assignor or its version range is not supported by the consumer group.",
        false,
    );

    const fn new(code: i16, message: &'static str, retryable: bool) -> Error {
        Error {
            code,
            message,
            retryable,
        }
    }

    pub fn code(&self) -> i16 {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }

    pub fn retryable(&self) -> bool {
        self.retryable
    }
}
