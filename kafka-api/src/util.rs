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
