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
    cell::RefCell,
    fmt::{Debug, Formatter},
    slice::IterMut,
};

use bytes::{Buf, BufMut};

use crate::{
    bytebuffer::ByteBuffer,
    codec::{Decoder, RecordList},
};

pub const BASE_OFFSET_OFFSET: usize = 0;
pub const BASE_OFFSET_LENGTH: usize = 8;
pub const LENGTH_OFFSET: usize = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
pub const LENGTH_LENGTH: usize = 4;
pub const PARTITION_LEADER_EPOCH_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
pub const PARTITION_LEADER_EPOCH_LENGTH: usize = 4;
pub const MAGIC_OFFSET: usize = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
pub const MAGIC_LENGTH: usize = 1;
pub const CRC_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const CRC_LENGTH: usize = 4;
pub const ATTRIBUTES_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
pub const ATTRIBUTE_LENGTH: usize = 2;
pub const LAST_OFFSET_DELTA_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
pub const LAST_OFFSET_DELTA_LENGTH: usize = 4;
pub const BASE_TIMESTAMP_OFFSET: usize = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
pub const BASE_TIMESTAMP_LENGTH: usize = 8;
pub const MAX_TIMESTAMP_OFFSET: usize = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
pub const MAX_TIMESTAMP_LENGTH: usize = 8;
pub const PRODUCER_ID_OFFSET: usize = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
pub const PRODUCER_ID_LENGTH: usize = 8;
pub const PRODUCER_EPOCH_OFFSET: usize = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
pub const PRODUCER_EPOCH_LENGTH: usize = 2;
pub const BASE_SEQUENCE_OFFSET: usize = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
pub const BASE_SEQUENCE_LENGTH: usize = 4;
pub const RECORDS_COUNT_OFFSET: usize = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
pub const RECORDS_COUNT_LENGTH: usize = 4;
pub const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
pub const RECORD_BATCH_OVERHEAD: usize = RECORDS_OFFSET;

pub const HEADER_SIZE_UP_TO_MAGIC: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const LOG_OVERHEAD: usize = LENGTH_OFFSET + LENGTH_LENGTH;

#[derive(Debug, Default)]
pub struct Records {
    buf: ByteBuffer,
    batches: RefCell<Vec<RecordBatch>>,
}

impl AsRef<[u8]> for Records {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Clone for Records {
    fn clone(&self) -> Self {
        Records {
            buf: self.buf.clone(),
            batches: RefCell::default(),
        }
    }
}

impl Records {
    pub fn new(buf: ByteBuffer) -> Self {
        let batches = RefCell::new(vec![]);
        Records { buf, batches }
    }

    pub fn batches(&self) -> IterMut<'_, RecordBatch> {
        if self.batches.borrow().is_empty() {
            let mut batches = self.batches.borrow_mut();

            let mut offset = 0;
            let mut remaining = self.buf.len() - offset;
            while remaining > 0 {
                assert!(
                    remaining >= HEADER_SIZE_UP_TO_MAGIC,
                    "no enough bytes when decode records (remaining: {})",
                    remaining
                );

                let record_size = (&self.buf[LENGTH_OFFSET..]).get_i32();
                let batch_size = record_size as usize + LOG_OVERHEAD;

                assert!(
                    remaining >= batch_size,
                    "no enough bytes when decode records (remaining: {})",
                    remaining
                );

                let record = match (&self.buf[MAGIC_OFFSET..]).get_i8() {
                    2 => {
                        let buf = self.buf.slice(offset..offset + batch_size);
                        offset += batch_size;
                        remaining -= batch_size;
                        RecordBatch { buf }
                    }
                    v => unimplemented!("record batch version {}", v),
                };

                batches.push(record);
            }
        }

        let batches = self.batches.as_ptr();
        // SAFETY - self.batches.borrow_mut() must never be called again
        unsafe { (&mut *batches).iter_mut() }
    }
}

#[derive(Default)]
pub struct RecordBatch {
    buf: ByteBuffer,
}

impl Debug for RecordBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("RecordBatch");
        de.field("base_offset", &self.base_offset());
        de.field("base_sequence", &self.base_sequence());
        de.field("last_offset_delta", &self.last_offset_delta());
        de.field("records_count", &self.records_count());
        de.field("records", &self.records());
        de.finish()
    }
}

impl RecordBatch {
    pub fn set_last_offset(&mut self, offset: i64) {
        let base_offset = offset - self.last_offset_delta() as i64;
        self.buf
            .chunk_mut_in(BASE_OFFSET_OFFSET..)
            .put_i64(base_offset);
    }

    pub fn base_offset(&self) -> i64 {
        (&self.buf[BASE_OFFSET_OFFSET..]).get_i64()
    }

    pub fn base_sequence(&self) -> i32 {
        (&self.buf[BASE_SEQUENCE_OFFSET..]).get_i32()
    }

    pub fn last_offset_delta(&self) -> i32 {
        (&self.buf[LAST_OFFSET_DELTA_OFFSET..]).get_i32()
    }

    pub fn records_count(&self) -> i32 {
        (&self.buf[RECORDS_COUNT_OFFSET..]).get_i32()
    }

    pub fn records(&self) -> Vec<Record> {
        let mut records = self.buf.slice(RECORDS_COUNT_OFFSET..);
        RecordList.decode(&mut records).expect("malformed records")
    }
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

    use super::*;

    const RECORD: &[u8] = &[
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
        let records = Records::new(ByteBuffer::new(RECORD.to_vec()));
        let record_batches = records.batches().collect::<Vec<_>>();
        assert_eq!(record_batches.len(), 1);
        let record_batch = &record_batches[0];
        assert_eq!(record_batch.records_count(), 1);
        let record_vec = record_batch.records();
        assert_eq!(record_vec.len(), 1);
        let record = &record_vec[0];
        assert_eq!(record.key_len, -1);
        assert_eq!(record.key, None);
        assert_eq!(record.value_len, 26);
        assert_eq!(record.value, Some("This is the first message.".into()));
        Ok(())
    }
}
