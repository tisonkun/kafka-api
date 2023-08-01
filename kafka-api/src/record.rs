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

#[derive(Debug, Default, Clone)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_len: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: i32,
    /// bit 0~2:
    ///    0: no compression
    ///    1: gzip
    ///    2: snappy
    ///    3: lz4
    ///    4: zstd
    /// bit 3: timestampType
    /// bit 4: isTransactional (0 means not transactional)
    /// bit 5: isControlBatch (0 means not a control batch)
    /// bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for
    /// compaction)
    /// bit 7~15: unused
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug, Default, Clone)]
pub struct Record {
    pub len: i32, // varint
    /// bit 0~7: unused
    pub attributes: i8,
    pub timestamp_delta: i64, // varlong
    pub offset_delta: i32,    // varint
    pub key_len: i32,         // varint
    pub key: Option<bytes::Bytes>,
    pub value_len: i32, // varint
    pub value: Option<bytes::Bytes>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Default, Clone)]
pub struct Header {
    pub key_len: i32, // varint
    pub key: Option<bytes::Bytes>,
    pub value_len: i32, // varint
    pub value: Option<bytes::Bytes>,
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;

    use crate::codec::Records;

    const RECORD: [u8; 94] = [
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first offset
        0x0, 0x0, 0x0, 0x52, // record batch size
        0xFF, 0xFF, 0xFF, 0xFF, // partition leader epoch
        0x2,  // magic byte
        0xE2, 0x3F, 0xC9, 0x74, // crc
        0x0, 0x0, // attributes
        0x0, 0x0, 0x0, 0x0, // last offset delta
        0x0, 0x0, 0x1, 0x89, 0xAF, 0x78, 0x40, 0x72, // base timestamp
        0x0, 0x0, 0x1, 0x89, 0xAF, 0x78, 0x40, 0x72, // max timestamp
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // producer ID
        0x0, 0x0, // producer epoch
        0x0, 0x0, 0x0, 0x0, // base sequence
        0x0, 0x0, 0x0, 0x1,  // record counts
        0x40, // first record size
        0x0,  // attribute
        0x0,  // timestamp delta
        0x0,  // offset delta
        0x1,  // key length (zigzag : -1)
        // empty key payload
        0x34, // value length (zigzag : 26)
        0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x74, 0x68, 0x65, 0x20, 0x66, 0x69, 0x72,
        0x73, 0x74, 0x20, 0x6D, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2E, // value payload
        0x0,  // header counts
    ];

    #[test]
    fn test_codec_records() -> io::Result<()> {
        let mut bs = Bytes::from_static(&RECORD);
        let record_batches = Records.decode_batches(&mut bs)?;
        assert_eq!(record_batches.len(), 1);
        let record_batch = &record_batches[0];
        assert_eq!(record_batch.magic, 2);
        assert_eq!(record_batch.records.len(), 1);
        let record = &record_batch.records[0];
        assert_eq!(record.key_len, -1);
        assert_eq!(record.key, None);
        assert_eq!(record.value_len, 26);
        assert_eq!(
            record.value,
            Some(Bytes::from_static("This is the first message.".as_ref()))
        );
        Ok(())
    }
}
