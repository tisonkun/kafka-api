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

use bytes::BufMut;

use crate::{bytebuffer::ByteBuffer, codec::writable::Writable, record::Records};

pub struct SendBuilder<'a> {
    sends: Vec<Sendable<'a>>,
    bs: bytes::BytesMut,
}

impl<'a> Writable<'a> for SendBuilder<'a> {
    fn write_i8(&mut self, n: i8) -> io::Result<()> {
        self.bs.put_i8(n);
        Ok(())
    }

    fn write_i16(&mut self, n: i16) -> io::Result<()> {
        self.bs.put_i16(n);
        Ok(())
    }

    fn write_i32(&mut self, n: i32) -> io::Result<()> {
        self.bs.put_i32(n);
        Ok(())
    }

    fn write_i64(&mut self, n: i64) -> io::Result<()> {
        self.bs.put_i64(n);
        Ok(())
    }

    fn write_u8(&mut self, n: u8) -> io::Result<()> {
        self.bs.put_u8(n);
        Ok(())
    }

    fn write_u16(&mut self, n: u16) -> io::Result<()> {
        self.bs.put_u16(n);
        Ok(())
    }

    fn write_u32(&mut self, n: u32) -> io::Result<()> {
        self.bs.put_u32(n);
        Ok(())
    }

    fn write_u64(&mut self, n: u64) -> io::Result<()> {
        self.bs.put_u64(n);
        Ok(())
    }

    fn write_f32(&mut self, n: f32) -> io::Result<()> {
        self.bs.put_f32(n);
        Ok(())
    }

    fn write_f64(&mut self, n: f64) -> io::Result<()> {
        self.bs.put_f64(n);
        Ok(())
    }

    fn write_slice(&mut self, src: &[u8]) -> io::Result<()> {
        self.bs.put_slice(src);
        Ok(())
    }

    fn write_bytes(&mut self, buf: &ByteBuffer) -> io::Result<()> {
        self.flush_bytes();
        self.sends.push(Sendable::ByteBuffer(buf.clone()));
        Ok(())
    }

    fn write_records(&mut self, r: &'a Records) -> io::Result<()> {
        self.flush_bytes();
        self.sends.push(Sendable::Records(r));
        Ok(())
    }
}

impl<'a> Default for SendBuilder<'a> {
    fn default() -> Self {
        SendBuilder::new()
    }
}

impl<'a> SendBuilder<'a> {
    pub fn new() -> Self {
        SendBuilder {
            sends: vec![],
            bs: bytes::BytesMut::new(),
        }
    }

    pub fn finish(mut self) -> Vec<Sendable<'a>> {
        self.flush_bytes();
        self.sends
    }

    fn flush_bytes(&mut self) {
        if !self.bs.is_empty() {
            let bs = self.bs.split().freeze();
            self.sends.push(Sendable::Bytes(bs));
        }
    }
}

pub enum Sendable<'a> {
    Bytes(bytes::Bytes),
    ByteBuffer(ByteBuffer),
    Records(&'a Records),
}

impl Sendable<'_> {
    // io::Write cannot leverage the sendfile syscall if we want to copy bytes from a file to
    // socket. Rust seems doesn't have a good solution so we keep use io::Write here but open to
    // any other solution.
    pub fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Sendable::Bytes(bs) => writer.write_all(bs.as_ref()),
            Sendable::ByteBuffer(buf) => writer.write_all(buf.as_bytes()),
            Sendable::Records(r) => writer.write_all(r.as_bytes()),
        }
    }
}
