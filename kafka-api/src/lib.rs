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

#![feature(io_error_other)]

use std::{error, fmt::Display, io};

pub use codec::{Decodable, Encodable, RawTaggedField};

pub mod apikey;
pub(crate) mod codec;

fn err_io_other<E>(error: E) -> io::Error
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, error.into())
}

fn err_decode_message(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn err_decode_message_non_null(field: impl Display) -> io::Error {
    err_decode_message(format!(
        "non-nullable field {} was serialized as null",
        field
    ))
}
