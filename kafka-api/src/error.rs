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

#[derive(Debug, Clone)]
pub struct Error {
    code: i16,
    message: &'static str,
    retryable: bool,
}

#[allow(non_upper_case_globals)]
impl Error {
    pub const UnknownMemberId: Error =
        Error::new(25, "The coordinator is not aware of this member.", false);

    const fn new(code: i16, message: &'static str, retryable: bool) -> Error {
        Error {
            code,
            message,
            retryable,
        }
    }

    pub fn code(&self) -> i16 {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }

    pub fn retryable(&self) -> bool {
        self.retryable
    }
}
