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
    io,
    net::{TcpListener, TcpStream},
};

use tracing::{error, error_span, info, Level};

fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let addr = "127.0.0.1:9092";
    let listener = TcpListener::bind(addr)?;
    info!("Starting Kafka Simple Server at {}", addr);

    loop {
        let (socket, addr) = listener.accept()?;
        std::thread::spawn(move || {
            let addr = addr.to_string();
            error_span!("connection", addr).in_scope(|| {
                info!("Accept socket on {}", addr);
                match dispatch(socket) {
                    Ok(()) => {
                        info!("connection closed");
                    }
                    Err(err) => {
                        error!(?err, "connection failed");
                    }
                }
            })
        });
    }
}

fn dispatch(_socket: TcpStream) -> io::Result<()> {
    Ok(())
}
