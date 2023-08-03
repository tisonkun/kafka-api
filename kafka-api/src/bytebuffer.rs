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

use core::slice;
use std::{mem::ManuallyDrop, ops::RangeBounds, ptr::drop_in_place, sync::Arc};

use bytes::Buf;

#[derive(Debug, Clone)]
pub struct ByteBuffer {
    start: usize,
    end: usize,
    shared: Shared,
}

impl ByteBuffer {
    #[must_use = "consider ByteBuffer::advance if you don't need the other half"]
    pub fn split_to(&mut self, at: usize) -> ByteBuffer {
        assert!(
            at <= self.len(),
            "split_to out of bounds: {:?} <= {:?}",
            at,
            self.len(),
        );

        let start = self.start;
        let end = self.start + at;
        self.start = end;

        ByteBuffer {
            start,
            end,
            shared: self.shared.clone(),
        }
    }

    #[must_use = "consider ByteBuffer::truncate if you don't need the other half"]
    pub fn split_off(&mut self, at: usize) -> ByteBuffer {
        assert!(
            at <= self.len(),
            "split_off out of bounds: {:?} <= {:?}",
            at,
            self.len(),
        );

        let start = self.start + at;
        let end = self.end;
        self.end = start;

        ByteBuffer {
            start,
            end,
            shared: self.shared.clone(),
        }
    }

    pub fn truncate(&mut self, len: usize) {
        if len <= self.len() {
            self.end = self.start + len;
        }
    }

    // SAFETY - modifications are nonoverlapping
    pub fn slice(&self, range: impl RangeBounds<usize>) -> ByteBuffer {
        use core::ops::Bound;

        let len = self.len();

        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };

        assert!(
            begin <= end,
            "range start must not be greater than end: {:?} <= {:?}",
            begin,
            end,
        );
        assert!(
            end <= len,
            "range end out of bounds: {:?} <= {:?}",
            end,
            len,
        );

        ByteBuffer {
            start: self.start + begin,
            end: self.start + end,
            shared: self.shared.clone(),
        }
    }

    // SAFETY - modifications are nonoverlapping
    pub(crate) fn chunk_mut(&self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr(), self.len()) }
    }

    unsafe fn ptr(&self) -> *mut u8 {
        self.shared.ptr.offset(self.start as isize)
    }

    fn len(&self) -> usize {
        self.end - self.start
    }
}

impl Buf for ByteBuffer {
    fn remaining(&self) -> usize {
        self.len()
    }

    fn chunk(&self) -> &[u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr(), self.len()) }
    }

    fn advance(&mut self, cnt: usize) {
        self.start += cnt
    }
}

#[derive(Debug, Clone)]
struct Shared {
    ptr: *mut u8,
    #[allow(unused)]
    guard: Arc<DeallocateGuard>,
}

#[derive(Debug)]
struct DeallocateGuard {
    ptr: *mut u8,
}

impl Drop for DeallocateGuard {
    fn drop(&mut self) {
        println!("dropped");
        unsafe { drop_in_place(self.ptr) }
    }
}

impl ByteBuffer {
    pub fn new(v: Vec<u8>) -> Self {
        let mut me = ManuallyDrop::new(v);
        let (ptr, end) = (me.as_mut_ptr(), me.len());
        let start = 0;
        let shared = Shared {
            ptr: ptr.clone(),
            guard: Arc::new(DeallocateGuard { ptr }),
        };
        ByteBuffer { start, end, shared }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use crate::bytebuffer::ByteBuffer;

    #[test]
    fn test_raii() {
        let v = vec![1, 2, 3];
        let mut bb = ByteBuffer::new(v);
        println!("{:?}", bb);
        println!("{:?}", bb.slice(1..=2));
        while bb.has_remaining() {
            println!("{}", bb.get_u8());
        }
    }
}
