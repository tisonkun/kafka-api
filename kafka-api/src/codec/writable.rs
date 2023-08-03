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

use crate::{bytebuffer::ByteBuffer, record::ReadOnlyRecords};

pub trait Writable<'a> {
    fn write_i8(&mut self, n: i8) -> io::Result<()>;
    fn write_i16(&mut self, n: i16) -> io::Result<()>;
    fn write_i32(&mut self, n: i32) -> io::Result<()>;
    fn write_i64(&mut self, n: i64) -> io::Result<()>;
    fn write_u8(&mut self, n: u8) -> io::Result<()>;
    fn write_u16(&mut self, n: u16) -> io::Result<()>;
    fn write_u32(&mut self, n: u32) -> io::Result<()>;
    fn write_u64(&mut self, n: u64) -> io::Result<()>;
    fn write_f32(&mut self, n: f32) -> io::Result<()>;
    fn write_f64(&mut self, n: f64) -> io::Result<()>;
    fn write_slice(&mut self, src: &[u8]) -> io::Result<()>;
    fn write_bytes(&mut self, buf: &ByteBuffer) -> io::Result<()>;
    fn write_records(&mut self, r: &'a ReadOnlyRecords) -> io::Result<()>;

    fn write_uuid(&mut self, n: uuid::Uuid) -> io::Result<()> {
        self.write_slice(n.as_ref())
    }

    fn write_unsigned_varint(&mut self, n: i32) -> io::Result<()> {
        let mut v = n;
        while v >= 0x80 {
            self.write_u8((v as u8) | 0x80)?;
            v >>= 7;
        }
        self.write_u8(v as u8)
    }

    fn write_unsigned_varlong(&mut self, n: i64) -> io::Result<()> {
        let mut v = n;
        while v >= 0x80 {
            self.write_u8((v as u8) | 0x80)?;
            v >>= 7;
        }
        self.write_u8(v as u8)
    }
}
