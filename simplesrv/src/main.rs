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
    io::Read,
    mem::size_of,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

use bytes::Buf;
use kafka_api::{bytebuffer::ByteBuffer, sendable::SendBuilder, Request};
use simplesrv::{Broker, BrokerMeta, ClientInfo, ClusterMeta};
use tracing::{debug, error, error_span, info, trace, Level};

fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let addr: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let listener = TcpListener::bind(addr)?;
    info!("Starting Kafka Simple Server at {}", addr);

    let broker_meta = BrokerMeta {
        node_id: 1,
        host: addr.ip().to_string(),
        port: addr.port() as i32,
    };
    let cluster_meta = ClusterMeta {
        cluster_id: "Kafka Simple Server".to_string(),
        controller_id: 1,
        brokers: vec![broker_meta.clone()],
    };
    let broker = Arc::new(Mutex::new(Broker::new(broker_meta, cluster_meta)));

    loop {
        let (socket, addr) = listener.accept()?;
        let broker = broker.clone();
        std::thread::spawn(move || {
            let addr = addr.to_string();
            error_span!("connection", addr).in_scope(|| {
                info!("Accept socket on {}", addr);
                match dispatch(socket, broker) {
                    Ok(()) => {
                        info!("connection closed");
                    }
                    Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        info!("connection closed by client")
                    }
                    Err(err) => {
                        error!(?err, "connection failed");
                    }
                }
            })
        });
    }
}

fn dispatch(mut socket: TcpStream, broker: Arc<Mutex<Broker>>) -> io::Result<()> {
    let client_host = socket.peer_addr()?;

    loop {
        let n = {
            let mut buf = [0; size_of::<i32>()];
            socket.read_exact(&mut buf)?;
            i32::from_be_bytes(buf) as usize
        };

        let mut buf = {
            let mut buf = vec![0u8; n];
            socket.read_exact(&mut buf)?;
            ByteBuffer::new(buf)
        };

        let (header, request) = Request::decode(&mut buf)?;
        assert!(!buf.has_remaining(), "remaining bytes unparsed");
        debug!("Receive request {request:?}");

        let response = {
            let client_info = ClientInfo {
                client_id: header.client_id.clone(),
                client_host: client_host.to_string(),
            };
            let mut broker = broker.lock().unwrap();
            broker.reply(client_info, header.clone(), request)
        };
        let mut builder = SendBuilder::new();
        response.encode(header, &mut builder)?;
        let sends = builder.finish();
        trace!("ToSend {sends:?}");
        for send in sends {
            send.write_to(&mut socket)?;
        }
    }
}
