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
use std::{
    fmt::Debug,
    mem::ManuallyDrop,
    ops::{Deref, RangeBounds},
    ptr::drop_in_place,
    sync::Arc,
};

use bytes::Buf;

mod format;

#[derive(Clone)]
pub struct ByteBuffer {
    start: usize,
    end: usize,
    shared: Arc<Shared>,
}

#[derive(Debug, Clone)]
struct Shared {
    ptr: *mut u8,
}

unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

impl Drop for Shared {
    fn drop(&mut self) {
        unsafe { drop_in_place(self.ptr) }
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

impl ByteBuffer {
    pub fn new(v: Vec<u8>) -> Self {
        let mut me = ManuallyDrop::new(v);
        let (ptr, end) = (me.as_mut_ptr(), me.len());
        let start = 0;
        let shared = Arc::new(Shared { ptr });
        ByteBuffer { start, end, shared }
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.end <= self.start
    }

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

    pub fn slice(&self, range: impl RangeBounds<usize>) -> ByteBuffer {
        let (begin, end) = self.check_range(range);
        ByteBuffer {
            start: self.start + begin,
            end: self.start + end,
            shared: self.shared.clone(),
        }
    }

    // SAFETY - modifications are nonoverlapping
    //
    // We cannot implement AsMut / DerefMut for this conventions, cause impl trait will be public
    // visible, but we need to narrow the mutations within this crate (for in place mutate memory
    // batches).
    pub(crate) fn chunk_mut_in(&mut self, range: impl RangeBounds<usize>) -> &mut [u8] {
        let (begin, end) = self.check_range(range);
        &mut (unsafe { slice::from_raw_parts_mut(self.ptr(), self.len()) }[begin..end])
    }

    fn check_range(&self, range: impl RangeBounds<usize>) -> (usize, usize) {
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

        (begin, end)
    }

    unsafe fn ptr(&self) -> *mut u8 {
        self.shared.ptr.add(self.start)
    }
}

impl Default for ByteBuffer {
    fn default() -> Self {
        ByteBuffer::new(vec![])
    }
}

impl AsRef<[u8]> for ByteBuffer {
    fn as_ref(&self) -> &[u8] {
        self.chunk()
    }
}

impl Deref for ByteBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.chunk()
    }
}

impl From<&str> for ByteBuffer {
    fn from(value: &str) -> Self {
        ByteBuffer::new(value.as_bytes().to_vec())
    }
}

impl PartialEq for ByteBuffer {
    fn eq(&self, other: &ByteBuffer) -> bool {
        self.as_ref() == other.as_ref()
    }
}
