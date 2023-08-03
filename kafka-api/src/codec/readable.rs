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

use bytes::Buf;

use crate::{bytebuffer::ByteBuffer, err_codec_message, record::MutableRecords, RawTaggedField};

fn varint_zigzag(i: i32) -> i32 {
    (((i as u32) >> 1) as i32) ^ -(i & 1)
}

fn varlong_zigzag(i: i64) -> i64 {
    (((i as u64) >> 1) as i64) ^ -(i & 1)
}

pub trait Readable {
    fn remaining(&self) -> usize;
    fn read_i8(&mut self) -> i8;
    fn read_i16(&mut self) -> i16;
    fn read_i32(&mut self) -> i32;
    fn read_i64(&mut self) -> i64;
    fn read_u8(&mut self) -> u8;
    fn read_u16(&mut self) -> u16;
    fn read_u32(&mut self) -> u32;
    fn read_u64(&mut self) -> u64;
    fn read_f32(&mut self) -> f32;
    fn read_f64(&mut self) -> f64;

    fn read_bytes(&mut self, len: usize) -> ByteBuffer;

    fn read_string(&mut self, len: usize) -> String {
        let bs = self.read_bytes(len);
        unsafe { String::from_utf8_unchecked(bs.to_vec()) }
    }

    fn read_unknown_tagged_field(&mut self, tag: i32, size: usize) -> RawTaggedField {
        let data = self.read_bytes(size);
        RawTaggedField { tag, data }
    }

    fn read_records(&mut self, len: usize) -> MutableRecords {
        MutableRecords::new(self.read_bytes(len))
    }

    fn read_uuid(&mut self) -> uuid::Uuid {
        let msb = self.read_u64();
        let lsb = self.read_u64();
        uuid::Uuid::from_u64_pair(msb, lsb)
    }

    fn read_unsigned_varint(&mut self) -> io::Result<i32> {
        let mut res = 0;
        for i in 0.. {
            debug_assert!(i < 5); // no larger than i32
            if self.remaining() >= size_of::<u8>() {
                let next = self.read_u8() as i32;
                res |= (next & 0x7F) << (i * 7);
                if next < 0x80 {
                    break;
                }
            } else {
                return Err(err_codec_message(format!(
                    "no enough bytes when decode varint (res: {res}, remaining: {})",
                    self.remaining()
                )));
            }
        }
        Ok(res)
    }

    fn read_unsigned_varlong(&mut self) -> io::Result<i64> {
        let mut res = 0;
        for i in 0.. {
            debug_assert!(i < 10); // no larger than i64
            if self.remaining() >= size_of::<u8>() {
                let next = self.read_u8() as i64;
                res |= (next & 0x7F) << (i * 7);
                if next < 0x80 {
                    break;
                }
            } else {
                return Err(err_codec_message(format!(
                    "no enough bytes when decode varlong (res: {res}, remaining: {})",
                    self.remaining()
                )));
            }
        }
        Ok(res)
    }

    fn read_varint(&mut self) -> io::Result<i32> {
        self.read_unsigned_varint().map(varint_zigzag)
    }

    fn read_varlong(&mut self) -> io::Result<i64> {
        self.read_unsigned_varlong().map(varlong_zigzag)
    }
}

macro_rules! delegate_forward_buf {
    () => {
        fn remaining(&self) -> usize {
            Buf::remaining(self)
        }

        fn read_i8(&mut self) -> i8 {
            self.get_i8()
        }

        fn read_i16(&mut self) -> i16 {
            self.get_i16()
        }

        fn read_i32(&mut self) -> i32 {
            self.get_i32()
        }

        fn read_i64(&mut self) -> i64 {
            self.get_i64()
        }

        fn read_u8(&mut self) -> u8 {
            self.get_u8()
        }

        fn read_u16(&mut self) -> u16 {
            self.get_u16()
        }

        fn read_u32(&mut self) -> u32 {
            self.get_u32()
        }

        fn read_u64(&mut self) -> u64 {
            self.get_u64()
        }

        fn read_f32(&mut self) -> f32 {
            self.get_f32()
        }

        fn read_f64(&mut self) -> f64 {
            self.get_f64()
        }
    };
}

impl Readable for &[u8] {
    delegate_forward_buf!();

    fn read_bytes(&mut self, _: usize) -> ByteBuffer {
        unreachable!("this implementation is only for peeking size")
    }
}

impl Readable for ByteBuffer {
    delegate_forward_buf!();

    fn read_bytes(&mut self, len: usize) -> ByteBuffer {
        self.split_to(len)
    }
}
