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

use std::{io, io::Cursor};

use crate::{
    apikey::ApiMessageType,
    codec::{Decoder, Int16},
    request_header::RequestHeader,
    Decodable,
};

pub fn read_request_header<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> io::Result<RequestHeader> {
    let pos = cursor.position();
    let api_key = Int16.decode(cursor)?;
    let api_version = Int16.decode(cursor)?;
    let header_version = ApiMessageType::try_from(api_key)?.request_header_version(api_version);

    cursor.set_position(pos);
    RequestHeader::decode(cursor, header_version)
}
