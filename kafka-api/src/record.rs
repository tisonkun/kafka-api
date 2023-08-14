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
    cell::OnceCell,
    fmt::{Debug, Formatter},
    slice::{Iter, IterMut},
};

use bytes::{Buf, BufMut};
use tracing::warn;

use crate::{
    bytebuffer::ByteBuffer,
    codec::{Decoder, RecordList},
};

pub const NO_SEQUENCE: i32 = -1;

// The current attributes are given below:
// ---------------------------------------------------------------------------------------------------------------------------
// | Unused (7-15) | Delete Horizon Flag (6) | Control (5) | Transactional (4) | Timestamp Type (3) | Compression Type (0-2) |
// ---------------------------------------------------------------------------------------------------------------------------
pub const COMPRESSION_CODEC_MASK: u8 = 0x07;
pub const TIMESTAMP_TYPE_MASK: u8 = 0x08;
pub const TRANSACTIONAL_FLAG_MASK: u8 = 0x10;
pub const CONTROL_FLAG_MASK: u8 = 0x20;
pub const DELETE_HORIZON_FLAG_MASK: u8 = 0x40;

// offset table
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

#[derive(Default)]
pub struct MutableRecords {
    buf: ByteBuffer,
    batches: OnceCell<Vec<RecordBatch>>,
}

impl Clone for MutableRecords {
    /// ATTENTION - Cloning Records is a heavy operation.
    ///
    /// MutableRecords is a public struct and it has a [MutableRecords::mut_batches] method that
    /// modifies the underlying [ByteBuffer]. If we only do a shallow clone, then two MutableRecords
    /// that doesn't have any ownership overlapping can modify the same underlying bytes.
    ///
    /// Generally, MutableRecords users iterate over batches with [MutableRecords::batches] or
    /// [MutableRecords::mut_batches], and pass ownership instead of clone. This clone behavior is
    /// similar to clone a [Vec].
    ///
    /// To produce a read-only view without copy, use [MutableRecords::freeze] instead.
    fn clone(&self) -> Self {
        warn!("Cloning mutable records will copy bytes and is not encouraged; try MutableRecords::freeze.");
        MutableRecords {
            buf: ByteBuffer::new(self.buf.to_vec()),
            batches: OnceCell::new(),
        }
    }
}

impl Debug for MutableRecords {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.batches.get_or_init(|| load_batches(&self.buf)), f)
    }
}

impl MutableRecords {
    pub fn new(buf: ByteBuffer) -> Self {
        let batches = OnceCell::new();
        MutableRecords { buf, batches }
    }

    pub fn freeze(self) -> ReadOnlyRecords {
        ReadOnlyRecords::ByteBuffer(ByteBufferRecords::new(self.buf))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_bytes()
    }

    pub fn mut_batches(&mut self) -> IterMut<'_, RecordBatch> {
        self.batches.get_or_init(|| load_batches(&self.buf));
        // SAFETY - init above
        unsafe { self.batches.get_mut().unwrap_unchecked() }.iter_mut()
    }

    pub fn batches(&self) -> Iter<'_, RecordBatch> {
        self.batches.get_or_init(|| load_batches(&self.buf)).iter()
    }
}

#[derive(Debug, Clone)]
pub enum ReadOnlyRecords {
    None,
    ByteBuffer(ByteBufferRecords),
}

impl Default for ReadOnlyRecords {
    fn default() -> Self {
        ReadOnlyRecords::None
    }
}

impl ReadOnlyRecords {
    pub fn size(&self) -> usize {
        match self {
            ReadOnlyRecords::None => 0,
            ReadOnlyRecords::ByteBuffer(r) => r.buf.len(),
        }
    }

    pub fn batches(&self) -> Iter<'_, RecordBatch> {
        match self {
            ReadOnlyRecords::None => [].iter(),
            ReadOnlyRecords::ByteBuffer(r) => r.batches(),
        }
    }
}

#[derive(Default)]
pub struct ByteBufferRecords {
    buf: ByteBuffer,
    batches: OnceCell<Vec<RecordBatch>>,
}

impl Clone for ByteBufferRecords {
    fn clone(&self) -> Self {
        ByteBufferRecords {
            buf: self.buf.clone(),
            batches: OnceCell::new(),
        }
    }
}

impl Debug for ByteBufferRecords {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&load_batches(&self.buf), f)
    }
}

impl ByteBufferRecords {
    fn new(buf: ByteBuffer) -> ByteBufferRecords {
        let batches = OnceCell::new();
        ByteBufferRecords { buf, batches }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_bytes()
    }

    pub fn batches(&self) -> Iter<'_, RecordBatch> {
        self.batches.get_or_init(|| load_batches(&self.buf)).iter()
    }
}

fn load_batches(buf: &ByteBuffer) -> Vec<RecordBatch> {
    let mut batches = vec![];

    let mut offset = 0;
    let mut remaining = buf.len() - offset;
    while remaining > 0 {
        assert!(
            remaining >= HEADER_SIZE_UP_TO_MAGIC,
            "no enough bytes when decode records (remaining: {})",
            remaining
        );

        let record_size = (&buf[LENGTH_OFFSET..]).get_i32();
        let batch_size = record_size as usize + LOG_OVERHEAD;

        assert!(
            remaining >= batch_size,
            "no enough bytes when decode records (remaining: {})",
            remaining
        );

        let record = match (&buf[MAGIC_OFFSET..]).get_i8() {
            2 => {
                let buf = buf.slice(offset..offset + batch_size);
                offset += batch_size;
                remaining -= batch_size;
                RecordBatch { buf }
            }
            v => unimplemented!("record batch version {}", v),
        };

        batches.push(record);
    }

    batches
}

#[derive(Default)]
pub struct RecordBatch {
    buf: ByteBuffer,
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

#[derive(Debug, Default, Clone)]
pub struct Record {
    pub len: i32, // varint
    /// bit 0~7: unused
    pub attributes: i8,
    pub timestamp_delta: i64, // varlong
    pub offset_delta: i32,    // varint
    pub key_len: i32,         // varint
    pub key: Option<ByteBuffer>,
    pub value_len: i32, // varint
    pub value: Option<ByteBuffer>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Default, Clone)]
pub struct Header {
    pub key_len: i32, // varint
    pub key: Option<ByteBuffer>,
    pub value_len: i32, // varint
    pub value: Option<ByteBuffer>,
}

#[derive(Debug, Clone, Copy)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
}

#[derive(Debug, Default, Clone, Copy)]
pub enum CompressionType {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl From<u8> for CompressionType {
    fn from(ty: u8) -> Self {
        match ty {
            0 => CompressionType::None,
            1 => CompressionType::Gzip,
            2 => CompressionType::Snappy,
            3 => CompressionType::Lz4,
            4 => CompressionType::Zstd,
            _ => unreachable!("Unknown compression type id: {}", ty),
        }
    }
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
        let records = MutableRecords::new(ByteBuffer::new(RECORD.to_vec()));
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
        assert_eq!(
            record.value.as_deref().map(String::from_utf8_lossy),
            Some("This is the first message.".into())
        );
        Ok(())
    }
}
