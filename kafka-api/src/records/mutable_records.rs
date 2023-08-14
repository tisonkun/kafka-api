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

use std::{
    cell::OnceCell,
    fmt::{Debug, Formatter},
    slice::{Iter, IterMut},
};

use tracing::warn;

use crate::{bytebuffer::ByteBuffer, records::*};

#[derive(Default)]
pub struct MutableRecords {
    buf: ByteBuffer,
    batches: OnceCell<Vec<RecordBatch>>,
}

impl Clone for MutableRecords {
    /// ATTENTION - Cloning Records is a heavy operation.
    ///
    /// MutableRecords is a public struct and it has a [MutableRecords::mut_batches] method that
    /// modifies the underlying [ByteBuffer]. If we only do a shallow clone, then two MutableRecords
    /// that doesn't have any ownership overlapping can modify the same underlying bytes.
    ///
    /// Generally, MutableRecords users iterate over batches with [MutableRecords::batches] or
    /// [MutableRecords::mut_batches], and pass ownership instead of clone. This clone behavior is
    /// similar to clone a [Vec].
    ///
    /// To produce a read-only view without copy, use [MutableRecords::freeze] instead.
    fn clone(&self) -> Self {
        warn!("Cloning mutable records will copy bytes and is not encouraged; try MutableRecords::freeze.");
        MutableRecords {
            buf: ByteBuffer::new(self.buf.to_vec()),
            batches: OnceCell::new(),
        }
    }
}

impl Debug for MutableRecords {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.batches.get_or_init(|| load_batches(&self.buf)), f)
    }
}

impl MutableRecords {
    pub fn new(buf: ByteBuffer) -> Self {
        let batches = OnceCell::new();
        MutableRecords { buf, batches }
    }

    pub fn freeze(self) -> ReadOnlyRecords {
        ReadOnlyRecords::ByteBuffer(ByteBufferRecords::new(self.buf))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_bytes()
    }

    pub fn mut_batches(&mut self) -> IterMut<'_, RecordBatch> {
        self.batches.get_or_init(|| load_batches(&self.buf));
        // SAFETY - init above
        unsafe { self.batches.get_mut().unwrap_unchecked() }.iter_mut()
    }

    pub fn batches(&self) -> Iter<'_, RecordBatch> {
        self.batches.get_or_init(|| load_batches(&self.buf)).iter()
    }
}
