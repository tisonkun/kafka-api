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

use bytes::Buf;

use crate::{
    codec::{Decoder, Int16, VarInt},
    err_codec_message,
    record::Records,
};

pub trait Readable: Buf + Sized {
    fn read_records(&mut self, flexible: bool) -> io::Result<Option<Records>>;
}

impl<T: Buf> Readable for T {
    default fn read_records(&mut self, flexible: bool) -> io::Result<Option<Records>> {
        match if flexible {
            VarInt.decode(self)? - 1
        } else {
            Int16.decode(self)? as i32
        } {
            -1 => Ok(None),
            n if n >= 0 => {
                let n = n as usize;
                let bs = if self.remaining() >= n {
                    Ok(self.copy_to_bytes(n))
                } else {
                    Err(err_codec_message(format!(
                        "no enough {n} bytes when decode records (remaining: {})",
                        self.remaining()
                    )))
                }?;
                Ok(Some(Records::new(bytes::BytesMut::from(&bs[..]))))
            }
            n => Err(err_codec_message(format!(
                "illegal length {n} when decode records"
            ))),
        }
    }
}

impl Readable for bytes::BytesMut {
    fn read_records(&mut self, flexible: bool) -> io::Result<Option<Records>> {
        match if flexible {
            VarInt.decode(self)? - 1
        } else {
            Int16.decode(self)? as i32
        } {
            -1 => Ok(None),
            n if n >= 0 => {
                let n = n as usize;
                let bs = if self.remaining() >= n {
                    Ok(self.split_to(n))
                } else {
                    Err(err_codec_message(format!(
                        "no enough {n} bytes when decode records (remaining: {})",
                        self.remaining()
                    )))
                }?;
                Ok(Some(Records::new(bs)))
            }
            n => Err(err_codec_message(format!(
                "illegal length {n} when decode records"
            ))),
        }
    }
}
