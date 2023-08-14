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

use std::fmt::{Debug, Formatter};

use bytes::{Buf, BufMut};

use crate::{
    bytebuffer::ByteBuffer,
    codec::{Decoder, RecordList},
    records::*,
};

#[derive(Default)]
pub struct RecordBatch {
    pub(super) buf: ByteBuffer,
}

impl Debug for RecordBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("RecordBatch");
        de.field("magic", &self.magic());
        de.field("offset", &(self.base_offset()..=self.last_offset()));
        de.field("sequence", &(self.base_sequence()..=self.last_sequence()));
        de.field("is_transactional", &self.is_transactional());
        de.field("is_control_batch", &self.is_control_batch());
        de.field("compression_type", &self.compression_type());
        de.field("timestamp_type", &self.timestamp_type());
        de.field("crc", &self.checksum());
        de.field("records_count", &self.records_count());
        de.field("records", &self.records());
        de.finish()
    }
}

/// Similar to [i32::wrapping_add], but wrap to `0` instead of [i32::MIN].
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// Similar to [i32::wrapping_add], but wrap at `0` instead of [i32::MIN].
pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    if sequence < decrement {
        i32::MAX - (decrement - sequence) + 1
    } else {
        sequence - decrement
    }
}

impl RecordBatch {
    pub fn set_last_offset(&mut self, offset: i64) {
        let base_offset = offset - self.last_offset_delta() as i64;
        self.buf
            .mut_slice_in(BASE_OFFSET_OFFSET..)
            .put_i64(base_offset);
    }

    pub fn set_partition_leader_epoch(&mut self, epoch: i32) {
        self.buf
            .mut_slice_in(PARTITION_LEADER_EPOCH_OFFSET..)
            .put_i32(epoch);
    }

    pub fn magic(&self) -> i8 {
        (&self.buf[MAGIC_OFFSET..]).get_i8()
    }

    pub fn base_offset(&self) -> i64 {
        (&self.buf[BASE_OFFSET_OFFSET..]).get_i64()
    }

    pub fn last_offset(&self) -> i64 {
        self.base_offset() + self.last_offset_delta() as i64
    }

    pub fn base_sequence(&self) -> i32 {
        (&self.buf[BASE_SEQUENCE_OFFSET..]).get_i32()
    }

    pub fn last_sequence(&self) -> i32 {
        match self.base_sequence() {
            NO_SEQUENCE => NO_SEQUENCE,
            seq => increment_sequence(seq, self.last_offset_delta()),
        }
    }

    pub fn last_offset_delta(&self) -> i32 {
        (&self.buf[LAST_OFFSET_DELTA_OFFSET..]).get_i32()
    }

    pub fn max_timestamp(&self) -> i64 {
        (&self.buf[MAX_TIMESTAMP_OFFSET..]).get_i64()
    }

    pub fn records_count(&self) -> i32 {
        (&self.buf[RECORDS_COUNT_OFFSET..]).get_i32()
    }

    pub fn records(&self) -> Vec<Record> {
        let mut records = self.buf.slice(RECORDS_COUNT_OFFSET..);
        RecordList.decode(&mut records).expect("malformed records")
    }

    pub fn checksum(&self) -> u32 {
        (&self.buf[CRC_OFFSET..]).get_u32()
    }

    pub fn is_transactional(&self) -> bool {
        self.attributes() & TRANSACTIONAL_FLAG_MASK > 0
    }

    pub fn is_control_batch(&self) -> bool {
        self.attributes() & CONTROL_FLAG_MASK > 0
    }

    pub fn timestamp_type(&self) -> TimestampType {
        if self.attributes() & TIMESTAMP_TYPE_MASK != 0 {
            TimestampType::LogAppendTime
        } else {
            TimestampType::CreateTime
        }
    }

    pub fn compression_type(&self) -> CompressionType {
        (self.attributes() & COMPRESSION_CODEC_MASK).into()
    }

    pub fn delete_horizon_ms(&self) -> Option<i64> {
        if self.has_delete_horizon_ms() {
            Some((&self.buf[BASE_TIMESTAMP_OFFSET..]).get_i64())
        } else {
            None
        }
    }

    fn has_delete_horizon_ms(&self) -> bool {
        self.attributes() & DELETE_HORIZON_FLAG_MASK > 0
    }

    // note we're not using the second byte of attributes
    fn attributes(&self) -> u8 {
        (&self.buf[ATTRIBUTES_OFFSET..]).get_u16() as u8
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::records::MutableRecords;

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
        let records = MutableRecords::new(ByteBuffer::new(RECORD.to_vec()));
        let record_batches = records.batches();
        assert_eq!(record_batches.len(), 1);
        let record_batch = &record_batches[0];
        assert_eq!(record_batch.records_count(), 1);
        let record_vec = record_batch.records();
        assert_eq!(record_vec.len(), 1);
        let record = &record_vec[0];
        assert_eq!(record.key_len, -1);
        assert_eq!(record.key, None);
        assert_eq!(record.value_len, 26);
        assert_eq!(
            record.value.as_deref().map(String::from_utf8_lossy),
            Some("This is the first message.".into())
        );
        Ok(())
    }
}
