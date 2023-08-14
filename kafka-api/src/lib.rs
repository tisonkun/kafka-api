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

extern crate core;

use std::{fmt::Display, io};

pub use codec::{Deserializable, RawTaggedField, Readable, Serializable, Writable};
pub use schemata::*;

pub mod apikey;
pub mod bytebuffer;
pub(crate) mod codec;
pub mod error;
pub mod records;
mod schemata;
pub mod sendable;

fn err_codec_message(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn err_decode_message_unsupported(version: i16, schemata: &str) -> io::Error {
    err_codec_message(format!("Cannot read version {version} of {schemata}"))
}

fn err_encode_message_unsupported(version: i16, schemata: &str) -> io::Error {
    err_codec_message(format!("Cannot write version {version} of {schemata}"))
}

fn err_decode_message_null(field: impl Display) -> io::Error {
    err_codec_message(format!("non-nullable field {field} was serialized as null"))
}

fn err_encode_message_null(field: impl Display) -> io::Error {
    err_codec_message(format!(
        "non-nullable field {field} to be serialized as null"
    ))
}
