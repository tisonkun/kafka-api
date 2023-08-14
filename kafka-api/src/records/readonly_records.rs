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

use crate::records::*;

#[derive(Debug, Default, Clone)]
pub enum ReadOnlyRecords {
    #[default]
    None,
    ByteBuffer(ByteBufferRecords),
}

impl ReadOnlyRecords {
    pub fn size(&self) -> usize {
        match self {
            ReadOnlyRecords::None => 0,
            ReadOnlyRecords::ByteBuffer(r) => r.buf.len(),
        }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        match self {
            ReadOnlyRecords::None => &[],
            ReadOnlyRecords::ByteBuffer(r) => r.batches(),
        }
    }

    pub fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            ReadOnlyRecords::None => writer.write_all(&[]),
            ReadOnlyRecords::ByteBuffer(r) => writer.write_all(r.buf.as_bytes()),
        }
    }
}

#[derive(Default)]
pub struct ByteBufferRecords {
    buf: ByteBuffer,
    batches: OnceCell<Vec<RecordBatch>>,
}

impl Clone for ByteBufferRecords {
    fn clone(&self) -> Self {
        ByteBufferRecords {
            buf: self.buf.clone(),
            batches: OnceCell::new(),
        }
    }
}

impl Debug for ByteBufferRecords {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.batches(), f)
    }
}

impl ByteBufferRecords {
    pub(super) fn new(buf: ByteBuffer) -> ByteBufferRecords {
        let batches = OnceCell::new();
        ByteBufferRecords { buf, batches }
    }

    fn batches(&self) -> &[RecordBatch] {
        self.batches.get_or_init(|| load_batches(&self.buf))
    }
}
