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

use bytes::BufMut;

pub use crate::codec::readable::Readable;
use crate::{
    bytebuffer::ByteBuffer,
    err_codec_message,
    record::{Header, Record, Records},
};

pub mod readable;

pub trait Decoder<T: Sized> {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<T>;
}

pub trait Encoder<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, value: T) -> io::Result<()>;
    fn calculate_size(&self, value: T) -> usize;
}

pub trait FixedSizeEncoder {
    const SIZE: usize;
}

pub trait Deserializable: Sized {
    fn read<B: Readable>(buf: &mut B, version: i16) -> io::Result<Self>;
}

pub trait Serializable: Sized {
    fn write<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()>;

    fn calculate_size(&self, version: i16) -> usize {
        let _ = version;
        todo!("calculate size for responses")
    }
}

#[derive(Debug, Default, Clone)]
pub struct RawTaggedField {
    pub tag: i32,
    pub data: ByteBuffer,
}

#[derive(Debug, Copy, Clone)]
pub(super) struct RawTaggedFieldWriter;

impl RawTaggedFieldWriter {
    pub(super) fn write_field<
        B: BufMut,
        T: Copy, // primitive or reference
        E: Encoder<T>,
    >(
        &self,
        buf: &mut B,
        tag: i32,
        encoder: E,
        value: T,
    ) -> io::Result<()> {
        VarInt.encode(buf, tag)?;
        VarInt.encode(buf, encoder.calculate_size(value) as i32)?;
        encoder.encode(buf, value)?;
        Ok(())
    }

    pub(super) fn calculate_field_size<T, E: Encoder<T>>(
        &self,
        tag: i32,
        encoder: E,
        value: T,
    ) -> usize {
        let size = encoder.calculate_size(value);
        let mut res = 0;
        res += VarInt.calculate_size(tag);
        res += VarInt.calculate_size(size as i32);
        res + size
    }

    fn write_bytes<B: BufMut>(&self, buf: &mut B, tag: i32, bs: &[u8]) -> io::Result<()> {
        VarInt.encode(buf, tag)?;
        VarInt.encode(buf, bs.len() as i32)?;
        buf.put_slice(bs);
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct RawTaggedFieldList;

impl RawTaggedFieldList {
    pub(super) fn decode_with<B: Readable, F>(
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
                if buf.remaining() >= size {
                    res.push(buf.read_unknown_tagged_field(tag, size));
                } else {
                    return Err(err_codec_message(format!(
                        "no enough {n} bytes when decode tagged field (remaining: {})",
                        buf.remaining()
                    )));
                }
            }
        }
        Ok(res)
    }

    pub(super) fn encode_with<B: BufMut, F>(
        &self,
        buf: &mut B,
        n: usize, // extra fields
        fields: &[RawTaggedField],
        mut f: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut B) -> io::Result<()>,
    {
        VarInt.encode(buf, (fields.len() + n) as i32)?;
        f(buf)?;
        for field in fields {
            RawTaggedFieldWriter.write_bytes(buf, field.tag, field.data.as_ref())?;
        }
        Ok(())
    }

    pub(super) fn calculate_size_with(
        &self,
        n: usize,  // extra fields
        bs: usize, // extra bytes
        fields: &[RawTaggedField],
    ) -> usize {
        let mut res = 0;
        res += VarInt.calculate_size((fields.len() + n) as i32);
        for field in fields {
            res += VarInt.calculate_size(field.tag);
            res += VarInt.calculate_size(field.data.len() as i32);
            res += field.data.len();
        }
        res + bs
    }
}

impl Decoder<Vec<RawTaggedField>> for RawTaggedFieldList {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<Vec<RawTaggedField>> {
        RawTaggedFieldList.decode_with(buf, |_, _, _| Ok(false))
    }
}

impl Encoder<&[RawTaggedField]> for RawTaggedFieldList {
    fn encode<B: BufMut>(&self, buf: &mut B, fields: &[RawTaggedField]) -> io::Result<()> {
        self.encode_with(buf, 0, fields, |_| Ok(()))
    }

    fn calculate_size(&self, fields: &[RawTaggedField]) -> usize {
        self.calculate_size_with(0, 0, fields)
    }
}

macro_rules! define_ints_codec {
    ($name:ident, $ty:ty, $put:ident, $read:ident) => {
        #[derive(Debug, Copy, Clone)]
        pub(super) struct $name;

        impl Decoder<$ty> for $name {
            fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<$ty> {
                if buf.remaining() >= size_of::<$ty>() {
                    Ok(buf.$read())
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

            #[inline]
            fn calculate_size(&self, _: $ty) -> usize {
                size_of::<$ty>()
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

            #[inline]
            fn calculate_size(&self, _: &$ty) -> usize {
                size_of::<$ty>()
            }
        }

        impl FixedSizeEncoder for $name {
            const SIZE: usize = size_of::<$ty>();
        }
    };
}

define_ints_codec!(Int8, i8, put_i8, read_i8);
define_ints_codec!(Int16, i16, put_i16, read_i16);
define_ints_codec!(Int32, i32, put_i32, read_i32);
define_ints_codec!(Int64, i64, put_i64, read_i64);
define_ints_codec!(UInt8, u8, put_u8, read_u8);
define_ints_codec!(UInt16, u16, put_u16, read_u16);
define_ints_codec!(UInt32, u32, put_u32, read_u32);
define_ints_codec!(UInt64, u64, put_u64, read_u64);
define_ints_codec!(Float32, f32, put_f32, read_f32);
define_ints_codec!(Float64, f64, put_f64, read_f64);

#[derive(Debug, Copy, Clone)]
pub(super) struct Bool;

impl Decoder<bool> for Bool {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<bool> {
        if buf.remaining() >= size_of::<u8>() {
            Ok(buf.read_u8() != 0)
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

    fn calculate_size(&self, _: bool) -> usize {
        size_of::<bool>()
    }
}

impl FixedSizeEncoder for Bool {
    const SIZE: usize = size_of::<bool>();
}

#[derive(Debug, Copy, Clone)]
pub(super) struct VarInt;

impl Decoder<i32> for VarInt {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<i32> {
        buf.read_unsigned_varint()
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

    fn calculate_size(&self, value: i32) -> usize {
        let mut res = 1;
        let mut v = value;
        while v >= 0x80 {
            res += 1;
            v >>= 7;
        }
        debug_assert!(v <= 5);
        res
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct VarLong;

impl Decoder<i64> for VarLong {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<i64> {
        buf.read_unsigned_varlong()
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

    fn calculate_size(&self, value: i64) -> usize {
        let mut res = 1;
        let mut v = value;
        while v >= 0x80 {
            res += 1;
            v >>= 7;
        }
        debug_assert!(v <= 10);
        res
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct NullableString(pub bool /* flexible */);

impl Decoder<Option<String>> for NullableString {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<Option<String>> {
        let len = if self.0 {
            VarInt.decode(buf)? - 1
        } else {
            Int16.decode(buf)? as i32
        };

        match len {
            -1 => Ok(None),
            n if n >= 0 => {
                let n = n as usize;
                if buf.remaining() >= n {
                    Ok(Some(buf.read_string(n)))
                } else {
                    Err(err_codec_message(format!(
                        "no enough {n} bytes when decode string (remaining: {})",
                        buf.remaining()
                    )))
                }
            }
            n => Err(err_codec_message(format!(
                "illegal length {n} when decode string"
            ))),
        }
    }
}

impl Encoder<Option<&str>> for NullableString {
    fn encode<B: BufMut>(&self, buf: &mut B, value: Option<&str>) -> io::Result<()> {
        write_slice(buf, value.map(|s| s.as_bytes()), self.0)
    }

    fn calculate_size(&self, value: Option<&str>) -> usize {
        slice_size(value.map(|s| s.as_bytes()), self.0)
    }
}

impl Encoder<&str> for NullableString {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &str) -> io::Result<()> {
        write_slice(buf, Some(value.as_bytes()), self.0)
    }

    fn calculate_size(&self, value: &str) -> usize {
        slice_size(Some(value.as_bytes()), self.0)
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct NullableRecords(pub bool /* flexible */);

impl Decoder<Option<Records>> for NullableRecords {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<Option<Records>> {
        match if self.0 {
            VarInt.decode(buf)? - 1
        } else {
            Int16.decode(buf)? as i32
        } {
            -1 => Ok(None),
            n if n >= 0 => {
                let n = n as usize;
                if buf.remaining() >= n {
                    Ok(Some(buf.read_records(n)))
                } else {
                    Err(err_codec_message(format!(
                        "no enough {n} bytes when decode records (remaining: {})",
                        buf.remaining()
                    )))
                }
            }
            n => Err(err_codec_message(format!(
                "illegal length {n} when decode records"
            ))),
        }
    }
}

impl Encoder<Option<&Records>> for NullableRecords {
    fn encode<B: BufMut>(&self, buf: &mut B, value: Option<&Records>) -> io::Result<()> {
        write_slice(buf, value.map(|bs| bs.as_ref()), self.0)
    }

    fn calculate_size(&self, value: Option<&Records>) -> usize {
        slice_size(value.map(|bs| bs.as_ref()), self.0)
    }
}

impl Encoder<&Records> for NullableRecords {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &Records) -> io::Result<()> {
        write_slice(buf, Some(value.as_ref()), self.0)
    }

    fn calculate_size(&self, value: &Records) -> usize {
        slice_size(Some(value.as_ref()), self.0)
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct NullableBytes(pub bool /* flexible */);

impl Decoder<Option<ByteBuffer>> for NullableBytes {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<Option<ByteBuffer>> {
        let len = if self.0 {
            VarInt.decode(buf)? - 1
        } else {
            Int16.decode(buf)? as i32
        };
        read_nullable_bytes(buf, len, "bytes")
    }
}

impl Encoder<Option<&ByteBuffer>> for NullableBytes {
    fn encode<B: BufMut>(&self, buf: &mut B, value: Option<&ByteBuffer>) -> io::Result<()> {
        write_slice(buf, value.map(|bs| bs.as_ref()), self.0)
    }

    fn calculate_size(&self, value: Option<&ByteBuffer>) -> usize {
        slice_size(value.map(|bs| bs.as_ref()), self.0)
    }
}

impl Encoder<&ByteBuffer> for NullableBytes {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &ByteBuffer) -> io::Result<()> {
        write_slice(buf, Some(value.as_ref()), self.0)
    }

    fn calculate_size(&self, value: &ByteBuffer) -> usize {
        slice_size(Some(value.as_ref()), self.0)
    }
}

impl Encoder<Option<&[u8]>> for NullableBytes {
    fn encode<B: BufMut>(&self, buf: &mut B, value: Option<&[u8]>) -> io::Result<()> {
        write_slice(buf, value, self.0)
    }

    fn calculate_size(&self, value: Option<&[u8]>) -> usize {
        slice_size(value, self.0)
    }
}

impl Encoder<&[u8]> for NullableBytes {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &[u8]) -> io::Result<()> {
        write_slice(buf, Some(value), self.0)
    }

    fn calculate_size(&self, value: &[u8]) -> usize {
        slice_size(Some(value), self.0)
    }
}

fn slice_size(slice: Option<&[u8]>, flexible: bool) -> usize {
    match slice {
        None => 1,
        Some(bs) => {
            let len = bs.len();
            len + if flexible {
                VarInt.calculate_size(len as i32 + 1)
            } else {
                Int16.calculate_size(len as i16)
            }
        }
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

fn read_nullable_bytes<B: Readable>(
    buf: &mut B,
    len: i32,
    ty: &str,
) -> io::Result<Option<ByteBuffer>> {
    match len {
        -1 => Ok(None),
        n if n >= 0 => {
            let n = n as usize;
            if buf.remaining() >= n {
                Ok(Some(buf.read_bytes(n)))
            } else {
                Err(err_codec_message(format!(
                    "no enough {n} bytes when decode {ty:?} (remaining: {})",
                    buf.remaining()
                )))
            }
        }
        n => Err(err_codec_message(format!(
            "illegal length {n} when decode {ty}"
        ))),
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct NullableArray<E>(pub E, pub bool /* flexible */);

impl<T, E: Decoder<T>> Decoder<Option<Vec<T>>> for NullableArray<E> {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<Option<Vec<T>>> {
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

    fn calculate_size(&self, value: Option<&[T]>) -> usize {
        match value {
            None => 1,
            Some(ns) => {
                let mut res = 0;
                res += if self.1 {
                    VarInt.calculate_size(ns.len() as i32 + 1)
                } else {
                    Int32.calculate_size(ns.len() as i32)
                };
                for n in ns {
                    res += self.0.calculate_size(n);
                }
                res
            }
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

    fn calculate_size(&self, value: &[T]) -> usize {
        self.calculate_size(Some(value))
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct Struct(pub i16 /* version */);

impl<T: Deserializable> Decoder<T> for Struct {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<T> {
        T::read(buf, self.0)
    }
}

impl<T: Serializable> Encoder<&T> for Struct {
    fn encode<B: BufMut>(&self, buf: &mut B, value: &T) -> io::Result<()> {
        value.write(buf, self.0)
    }

    fn calculate_size(&self, value: &T) -> usize {
        value.calculate_size(self.0)
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct Uuid;

impl Decoder<uuid::Uuid> for Uuid {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<uuid::Uuid> {
        if buf.remaining() >= 16 {
            Ok(buf.read_uuid())
        } else {
            Err(err_codec_message(format!(
                "no enough bytes when decode uuid (remaining: {})",
                buf.remaining()
            )))
        }
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

    fn calculate_size(&self, _: uuid::Uuid) -> usize {
        16
    }
}

impl FixedSizeEncoder for Uuid {
    const SIZE: usize = 16;
}

#[derive(Debug, Copy, Clone)]
pub(super) struct RecordList;

impl Decoder<Vec<Record>> for RecordList {
    fn decode<B: Readable>(&self, buf: &mut B) -> io::Result<Vec<Record>> {
        let cnt = Int32.decode(buf)?;
        let mut records = vec![];
        for _ in 0..cnt {
            let mut record = Record {
                len: buf.read_varint()?,
                attributes: Int8.decode(buf)?,
                timestamp_delta: buf.read_varlong()?,
                offset_delta: buf.read_varint()?,
                ..Default::default()
            };
            {
                let len = buf.read_varint()?;
                record.key_len = len;
                record.key = read_nullable_bytes(buf, len, "bytes")?;
            }
            {
                let len = buf.read_varint()?;
                record.value_len = len;
                record.value = read_nullable_bytes(buf, len, "bytes")?;
            }
            let headers_cnt = buf.read_varint()?;
            for _ in 0..headers_cnt {
                let mut header = Header::default();
                {
                    let len = buf.read_varint()?;
                    header.key_len = len;
                    header.key = read_nullable_bytes(buf, len, "bytes")?;
                }
                {
                    let len = buf.read_varint()?;
                    header.value_len = len;
                    header.value = read_nullable_bytes(buf, len, "bytes")?;
                }
                record.headers.push(header);
            }
            records.push(record);
        }
        Ok(records)
    }
}
