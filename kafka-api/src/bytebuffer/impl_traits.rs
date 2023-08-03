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

use std::{borrow::Borrow, cmp, hash, ops::Deref};

use crate::bytebuffer::ByteBuffer;

// impl Hash

impl hash::Hash for ByteBuffer {
    fn hash<H>(&self, state: &mut H)
    where
        H: hash::Hasher,
    {
        self.as_bytes().hash(state);
    }
}

// impl Refs

impl AsRef<[u8]> for ByteBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for ByteBuffer {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Deref for ByteBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

// impl Eq

impl PartialEq for ByteBuffer {
    fn eq(&self, other: &ByteBuffer) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialOrd for ByteBuffer {
    fn partial_cmp(&self, other: &ByteBuffer) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ByteBuffer {
    fn cmp(&self, other: &ByteBuffer) -> cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Eq for ByteBuffer {}

impl PartialEq<[u8]> for ByteBuffer {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}

impl PartialOrd<[u8]> for ByteBuffer {
    fn partial_cmp(&self, other: &[u8]) -> Option<cmp::Ordering> {
        self.as_bytes().partial_cmp(other)
    }
}

impl PartialEq<ByteBuffer> for [u8] {
    fn eq(&self, other: &ByteBuffer) -> bool {
        *other == *self
    }
}

impl PartialOrd<ByteBuffer> for [u8] {
    fn partial_cmp(&self, other: &ByteBuffer) -> Option<cmp::Ordering> {
        <[u8] as PartialOrd<[u8]>>::partial_cmp(self, other)
    }
}

impl PartialEq<str> for ByteBuffer {
    fn eq(&self, other: &str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialOrd<str> for ByteBuffer {
    fn partial_cmp(&self, other: &str) -> Option<cmp::Ordering> {
        self.as_bytes().partial_cmp(other.as_bytes())
    }
}

impl PartialEq<ByteBuffer> for str {
    fn eq(&self, other: &ByteBuffer) -> bool {
        *other == *self
    }
}

impl PartialOrd<ByteBuffer> for str {
    fn partial_cmp(&self, other: &ByteBuffer) -> Option<cmp::Ordering> {
        <[u8] as PartialOrd<[u8]>>::partial_cmp(self.as_bytes(), other)
    }
}

impl PartialEq<Vec<u8>> for ByteBuffer {
    fn eq(&self, other: &Vec<u8>) -> bool {
        *self == other[..]
    }
}

impl PartialOrd<Vec<u8>> for ByteBuffer {
    fn partial_cmp(&self, other: &Vec<u8>) -> Option<cmp::Ordering> {
        self.as_bytes().partial_cmp(&other[..])
    }
}

impl PartialEq<ByteBuffer> for Vec<u8> {
    fn eq(&self, other: &ByteBuffer) -> bool {
        *other == *self
    }
}

impl PartialOrd<ByteBuffer> for Vec<u8> {
    fn partial_cmp(&self, other: &ByteBuffer) -> Option<cmp::Ordering> {
        <[u8] as PartialOrd<[u8]>>::partial_cmp(self, other)
    }
}

impl PartialEq<String> for ByteBuffer {
    fn eq(&self, other: &String) -> bool {
        *self == other[..]
    }
}

impl PartialOrd<String> for ByteBuffer {
    fn partial_cmp(&self, other: &String) -> Option<cmp::Ordering> {
        self.as_bytes().partial_cmp(other.as_bytes())
    }
}

impl PartialEq<ByteBuffer> for String {
    fn eq(&self, other: &ByteBuffer) -> bool {
        *other == *self
    }
}

impl PartialOrd<ByteBuffer> for String {
    fn partial_cmp(&self, other: &ByteBuffer) -> Option<cmp::Ordering> {
        <[u8] as PartialOrd<[u8]>>::partial_cmp(self.as_bytes(), other)
    }
}

impl PartialEq<ByteBuffer> for &[u8] {
    fn eq(&self, other: &ByteBuffer) -> bool {
        *other == *self
    }
}

impl PartialOrd<ByteBuffer> for &[u8] {
    fn partial_cmp(&self, other: &ByteBuffer) -> Option<cmp::Ordering> {
        <[u8] as PartialOrd<[u8]>>::partial_cmp(self, other)
    }
}

impl PartialEq<ByteBuffer> for &str {
    fn eq(&self, other: &ByteBuffer) -> bool {
        *other == *self
    }
}

impl PartialOrd<ByteBuffer> for &str {
    fn partial_cmp(&self, other: &ByteBuffer) -> Option<cmp::Ordering> {
        <[u8] as PartialOrd<[u8]>>::partial_cmp(self.as_bytes(), other)
    }
}

impl<'a, T: ?Sized> PartialEq<&'a T> for ByteBuffer
where
    ByteBuffer: PartialEq<T>,
{
    fn eq(&self, other: &&'a T) -> bool {
        *self == **other
    }
}

impl<'a, T: ?Sized> PartialOrd<&'a T> for ByteBuffer
where
    ByteBuffer: PartialOrd<T>,
{
    fn partial_cmp(&self, other: &&'a T) -> Option<cmp::Ordering> {
        self.partial_cmp(&**other)
    }
}
