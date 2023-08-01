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

use std::{io, mem::size_of};

use bytes::{Buf, BufMut};

use crate::{
    err_codec_message, err_io_other,
    record::{Header, Record, RecordBatch},
};

pub trait Decoder<T: Sized> {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<T>;
}

pub trait Encoder<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, value: T) -> io::Result<()>;

    fn encode_alloc(&self, value: T) -> io::Result<bytes::Bytes> {
        let mut bs = bytes::BytesMut::new();
        self.encode(&mut bs, value)?;
        Ok(bs.freeze())
    }
}

pub trait Decodable: Sized {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> io::Result<Self>;
}

pub trait Encodable: Sized {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()>;
}

#[derive(Debug, Default, Clone)]
pub struct RawTaggedField {
    pub tag: i32,
    pub data: bytes::Bytes,
}

pub(super) struct RawTaggedFieldList;

impl RawTaggedFieldList {
    pub(super) fn decode_with<B: Buf, F>(
        &self,
        buf: &mut B,
        mut f: F,
    ) -> io::Result<Vec<RawTaggedField>>
    where
        F: FnMut(&mut B, i32, usize) -> io::Result<bool>,
    {
        let n = VarInt.decode(buf)?;
        let mut res = vec![];
        for _ in 0..n {
            let tag = VarInt.decode(buf)?;
            let size = VarInt.decode(buf)? as usize;
            let consumed = f(buf, tag, size)?;
            if !consumed {
                let data = read_exact_bytes_of(buf, size, "tagged fields")?;
                res.push(RawTaggedField { tag, data });
            }
        }
        Ok(res)
    }
}

impl Decoder<Vec<RawTaggedField>> for RawTaggedFieldList {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<Vec<RawTaggedField>> {
        RawTaggedFieldList.decode_with(buf, |_, _, _| Ok(false))
    }
}

impl Encoder<&[RawTaggedField]> for RawTaggedFieldList {
    fn encode<B: BufMut>(&self, buf: &mut B, fields: &[RawTaggedField]) -> io::Result<()> {
        VarInt.encode(buf, fields.len() as i32)?;
        for field in fields {
            VarInt.encode(buf, field.tag)?;
            VarInt.encode(buf, field.data.len() as i32)?;
            buf.put_slice(&field.data);
        }
        Ok(())
    }
}

macro_rules! define_ints_codec {
    ($name:ident, $ty:ty, $put:ident, $get:ident) => {
        #[derive(Debug, Copy, Clone)]
        pub(super) struct $name;

        impl Decoder<$ty> for $name {
            fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<$ty> {
                if buf.remaining() >= size_of::<$ty>() {
                    Ok(buf.$get())
                } else {
                    Err(err_codec_message(format!(
                        stringify!(no enough bytes when decode $ty (remaining: {})),
                        buf.remaining()
                    )))
                }
            }
        }

        impl Encoder<$ty> for $name {
            fn encode<B: BufMut>(&self, buf: &mut B, value: $ty) -> io::Result<()> {
                self.encode(buf, &value)
            }
        }

        impl Encoder<&$ty> for $name {
            fn encode<B: BufMut>(&self, buf: &mut B, value: &$ty) -> io::Result<()> {
                if buf.remaining_mut() >= size_of::<$ty>() {
                    buf.$put(*value);
                    Ok(())
                } else {
                    Err(err_codec_message(format!(
                        stringify!(no enough bytes when encode $ty (remaining: {})),
                        buf.remaining_mut()
                    )))
                }
            }
        }
    };
}

define_ints_codec!(Int8, i8, put_i8, get_i8);
define_ints_codec!(Int16, i16, put_i16, get_i16);
define_ints_codec!(Int32, i32, put_i32, get_i32);
define_ints_codec!(Int64, i64, put_i64, get_i64);
define_ints_codec!(UInt8, u8, put_u8, get_u8);
define_ints_codec!(UInt16, u16, put_u16, get_u16);
define_ints_codec!(UInt32, u32, put_u32, get_u32);
define_ints_codec!(UInt64, u64, put_u64, get_u64);
define_ints_codec!(Float32, f32, put_f32, get_f32);
define_ints_codec!(Float64, f64, put_f64, get_f64);

#[derive(Debug, Copy, Clone)]
pub(super) struct Bool;

impl Decoder<bool> for Bool {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<bool> {
        if buf.remaining() >= size_of::<u8>() {
            Ok(buf.get_u8() != 0)
        } else {
            Err(err_codec_message(format!(
                "no enough bytes when decode boolean (remaining: {})",
                buf.remaining()
            )))
        }
    }
}

impl Encoder<bool> for Bool {
    fn encode<B: BufMut>(&self, buf: &mut B, value: bool) -> io::Result<()> {
        if buf.remaining_mut() >= size_of::<u8>() {
            buf.put_u8(value as u8);
            Ok(())
        } else {
            Err(err_codec_message(format!(
                "no enough bytes when encode boolean (remaining: {})",
                buf.remaining_mut()
            )))
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct VarInt;

impl Decoder<i32> for VarInt {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<i32> {
        let mut res = 0;
        for i in 0.. {
            debug_assert!(i < 5); // no larger than i32
            if buf.remaining() >= size_of::<u8>() {
                let next = buf.get_u8() as i32;
                res |= (next & 0x7F) << (i * 7);
                if next < 0x80 {
                    break;
                }
            } else {
                return Err(err_codec_message(format!(
                    "no enough bytes when decode varint (res: {res}, remaining: {})",
                    buf.remaining()
                )));
            }
        }
        Ok(res)
    }
}

impl Encoder<i32> for VarInt {
    fn encode<B: BufMut>(&self, buf: &mut B, value: i32) -> io::Result<()> {
        let mut v = value;
        while v >= 0x80 {
            buf.put_u8((v as u8) | 0x80);
            v >>= 7;
        }
        buf.put_u8(v as u8);
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct VarLong;

impl Decoder<i64> for VarLong {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<i64> {
        let mut res = 0;
        for i in 0.. {
            debug_assert!(i < 10); // no larger than i64
            if buf.remaining() >= size_of::<u8>() {
                let next = buf.get_u8() as i64;
                res |= (next & 0x7F) << (i * 7);
                if next < 0x80 {
                    break;
                }
            } else {
                return Err(err_codec_message(format!(
                    "no enough bytes when decode varlong (res: {res}, remaining: {})",
                    buf.remaining()
                )));
            }
        }
        Ok(res)
    }
}

impl Encoder<i64> for VarLong {
    fn encode<B: BufMut>(&self, buf: &mut B, value: i64) -> io::Result<()> {
        let mut v = value;
        while v >= 0x80 {
            buf.put_u8((v as u8) | 0x80);
            v >>= 7;
        }
        buf.put_u8(v as u8);
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct NullableString(pub bool /* flexible */);

impl Decoder<Option<String>> for NullableString {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<Option<String>> {
        let len = if self.0 {
            VarInt.decode(buf)? - 1
        } else {
            Int16.decode(buf)? as i32
        };

        match read_nullable_bytes(buf, len, "string")? {
            None => Ok(None),
            Some(bs) => {
                let str = String::from_utf8(bs.to_vec()).map_err(err_io_other)?;
                Ok(Some(str))
            }
        }
    }
}

impl Encoder<Option<&str>> for NullableString {
    fn encode<B: BufMut>(&self, buf: &mut B, value: Option<&str>) -> io::Result<()> {
        write_slice(buf, value.map(|s| s.as_bytes()), self.0)
    }
}

impl Encoder<&str> for NullableString {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &str) -> io::Result<()> {
        write_slice(buf, Some(value.as_bytes()), self.0)
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct NullableBytes(pub bool /* flexible */);

impl Decoder<Option<bytes::Bytes>> for NullableBytes {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<Option<bytes::Bytes>> {
        let len = if self.0 {
            VarInt.decode(buf)? - 1
        } else {
            Int16.decode(buf)? as i32
        };
        read_nullable_bytes(buf, len, "bytes")
    }
}

impl Encoder<Option<&bytes::Bytes>> for NullableBytes {
    fn encode<B: BufMut>(&self, buf: &mut B, value: Option<&bytes::Bytes>) -> io::Result<()> {
        write_slice(buf, value.map(|bs| bs.as_ref()), self.0)
    }
}

impl Encoder<&bytes::Bytes> for NullableBytes {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &bytes::Bytes) -> io::Result<()> {
        write_slice(buf, Some(value.as_ref()), self.0)
    }
}

fn write_slice<B: BufMut>(buf: &mut B, slice: Option<&[u8]>, flexible: bool) -> io::Result<()> {
    match slice {
        None => {
            if flexible {
                VarInt.encode(buf, 0)?
            } else {
                Int32.encode(buf, -1)?
            }
        }
        Some(bs) => {
            let len = bs.len() as i16;
            if flexible {
                VarInt.encode(buf, len as i32 + 1)?;
            } else {
                Int16.encode(buf, len)?;
            }
            buf.put_slice(bs);
        }
    }
    Ok(())
}

fn read_nullable_bytes<B: Buf>(
    buf: &mut B,
    len: i32,
    ty: &str,
) -> io::Result<Option<bytes::Bytes>> {
    match len {
        -1 => Ok(None),
        n if n >= 0 => {
            let n = n as usize;
            let bs = read_exact_bytes_of(buf, n, ty)?;
            Ok(Some(bs))
        }
        n => Err(err_codec_message(format!(
            "illegal length {n} when decode {ty}"
        ))),
    }
}

fn read_exact_bytes_of<B: Buf>(buf: &mut B, n: usize, ty: &str) -> io::Result<bytes::Bytes> {
    if buf.remaining() >= n {
        Ok(buf.copy_to_bytes(n))
    } else {
        Err(err_codec_message(format!(
            "no enough {n} bytes when decode {ty:?} (remaining: {})",
            buf.remaining()
        )))
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct NullableArray<E>(pub E, pub bool /* flexible */);

impl<T, E: Decoder<T>> Decoder<Option<Vec<T>>> for NullableArray<E> {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<Option<Vec<T>>> {
        let len = if self.1 {
            VarInt.decode(buf)? - 1
        } else {
            Int32.decode(buf)?
        };
        match len {
            -1 => Ok(None),
            n if n >= 0 => {
                let n = n as usize;
                let mut result = Vec::with_capacity(n);
                for _ in 0..n {
                    result.push(self.0.decode(buf)?);
                }
                Ok(Some(result))
            }
            n => Err(err_codec_message(format!(
                "illegal length {n} when decode array"
            ))),
        }
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<Option<&[T]>> for NullableArray<E> {
    fn encode<B: BufMut>(&self, buf: &mut B, value: Option<&[T]>) -> io::Result<()> {
        match value {
            None => {
                if self.1 {
                    VarInt.encode(buf, 0)
                } else {
                    Int32.encode(buf, -1)
                }
            }
            Some(s) => self.encode(buf, s),
        }
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<&[T]> for NullableArray<E> {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &[T]) -> io::Result<()> {
        if self.1 {
            VarInt.encode(buf, value.len() as i32 + 1)?;
        } else {
            Int32.encode(buf, value.len() as i32)?;
        }
        for v in value {
            self.0.encode(buf, v)?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct Struct(pub i16 /* version */);

impl<T: Decodable> Decoder<T> for Struct {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<T> {
        T::decode(buf, self.0)
    }
}

impl<T: Encodable> Encoder<&T> for Struct {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &T) -> io::Result<()> {
        value.encode(buf, self.0)
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct Uuid;

impl Decoder<uuid::Uuid> for Uuid {
    fn decode<B: Buf>(&self, buf: &mut B) -> io::Result<uuid::Uuid> {
        let bs = read_exact_bytes_of(buf, 16, "uuid")?;
        let uuid = uuid::Uuid::from_slice(bs.as_ref()).map_err(err_io_other)?;
        Ok(uuid)
    }
}

impl Encoder<uuid::Uuid> for Uuid {
    fn encode<B: BufMut>(&self, buf: &mut B, value: uuid::Uuid) -> io::Result<()> {
        if buf.remaining_mut() >= 16 {
            buf.put_slice(value.as_bytes());
            Ok(())
        } else {
            Err(err_codec_message(format!(
                "no enough bytes when encode uuid (remaining: {})",
                buf.remaining_mut()
            )))
        }
    }
}

fn varint_zigzag(i: i32) -> i32 {
    (((i as u32) >> 1) as i32) ^ -(i & 1)
}

fn varlong_zigzag(i: i64) -> i64 {
    (((i as u64) >> 1) as i64) ^ -(i & 1)
}

const OFFSET_OFFSET: usize = 0;
const OFFSET_LENGTH: usize = 8;
const SIZE_OFFSET: usize = OFFSET_OFFSET + OFFSET_LENGTH;
const SIZE_LENGTH: usize = 4;
const LOG_OVERHEAD: usize = SIZE_OFFSET + SIZE_LENGTH;
/// The magic offset is at the same offset for all current message formats, but the 4 bytes
/// between the size and the magic is dependent on the version.
const MAGIC_OFFSET: usize = LOG_OVERHEAD + 4;
const MAGIC_LENGTH: usize = 1;
const HEADER_SIZE_UP_TO_MAGIC: usize = MAGIC_OFFSET + MAGIC_LENGTH;

#[derive(Debug, Copy, Clone)]
pub struct Records;

impl Records {
    pub fn decode_batches(&self, buf: &mut bytes::Bytes) -> io::Result<Vec<RecordBatch>> {
        let mut records = vec![];

        while buf.has_remaining() {
            if buf.remaining() < HEADER_SIZE_UP_TO_MAGIC {
                Err(err_codec_message(format!(
                    "no enough bytes when decode record batch (remaining: {})",
                    buf.remaining()
                )))?
            }

            let record_size = buf.slice(SIZE_OFFSET..).get_i32();
            let batch_size = record_size as usize + LOG_OVERHEAD;
            if buf.remaining() < batch_size {
                Err(err_codec_message(format!(
                    "no enough bytes when decode record batch (remaining: {})",
                    buf.remaining()
                )))?
            }

            let record = match buf.slice(MAGIC_OFFSET..).get_i8() {
                2 => Records.decode_batch(&mut buf.copy_to_bytes(batch_size))?,
                v => unimplemented!("batch encode version {}", v),
            };

            records.push(record);
        }

        Ok(records)
    }

    fn decode_batch(&self, buf: &mut bytes::Bytes) -> io::Result<RecordBatch> {
        let mut batch = RecordBatch {
            base_offset: Int64.decode(buf)?,
            batch_len: Int32.decode(buf)?,
            partition_leader_epoch: Int32.decode(buf)?,
            magic: Int8.decode(buf)?,
            crc: Int32.decode(buf)?,
            attributes: Int16.decode(buf)?,
            last_offset_delta: Int32.decode(buf)?,
            base_timestamp: Int64.decode(buf)?,
            max_timestamp: Int64.decode(buf)?,
            producer_id: Int64.decode(buf)?,
            producer_epoch: Int16.decode(buf)?,
            base_sequence: Int32.decode(buf)?,
            records: vec![],
        };

        // records
        let records_cnt = Int32.decode(buf)?;
        for _ in 0..records_cnt {
            let mut record = Record {
                len: varint_zigzag(VarInt.decode(buf)?),
                attributes: Int8.decode(buf)?,
                timestamp_delta: varlong_zigzag(VarLong.decode(buf)?),
                offset_delta: varint_zigzag(VarInt.decode(buf)?),
                ..Default::default()
            };
            {
                let len = varint_zigzag(VarInt.decode(buf)?);
                record.key_len = len;
                record.key = read_nullable_bytes(buf, len, "bytes")?;
            }
            {
                let len = varint_zigzag(VarInt.decode(buf)?);
                record.value_len = len;
                record.value = read_nullable_bytes(buf, len, "bytes")?;
            }
            let headers_cnt = varint_zigzag(VarInt.decode(buf)?);
            for _ in 0..headers_cnt {
                record.headers.push(Header {
                    key_len: varint_zigzag(VarInt.decode(buf)?),
                    key: read_nullable_bytes(buf, record.key_len, "bytes")?,
                    value_len: varint_zigzag(VarInt.decode(buf)?),
                    value: read_nullable_bytes(buf, record.value_len, "bytes")?,
                });
            }
            batch.records.push(record);
        }

        Ok(batch)
    }
}
