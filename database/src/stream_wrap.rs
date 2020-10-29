use async_trait::async_trait;

use futures::stream::{Stream,StreamExt};
use tokio::net::UdpSocket;
use std::net::SocketAddr;
use std::pin::Pin;

use serde::Serialize;
use serde::de::DeserializeOwned;

use std::collections::VecDeque;
use std::fmt::Debug;


/*
TODO
1) static lifetime for StreamWrap<T> for Stream<T> - maybe too restrictive
2) initialize UdpBind - find better ways than using option (UdpSocket can only be initialized asynchronously)
*/

#[async_trait]
pub trait StreamWrap<T> {
    async fn next(&mut self) -> Option<T>;
}

#[async_trait]
impl<T> StreamWrap<T> for Iterator<Item=T> + Send where T: Send {
    async fn next(&mut self) -> Option<T> {
        self.next()
    }
}

// Suport Streams (local/possibly remote clients)
#[async_trait]
impl<T: 'static,U> StreamWrap<T> for U where U: Stream<Item=T> + Unpin + Send {
    async fn next(&mut self) -> Option<T> {
        StreamExt::next(self).await // Use StreamExt trait
    }
}


// Support UdpSocket

// Need separate struct for internal data
pub struct UdpEndpoint<T> {
    port: u16,
	socket: Option<UdpSocket>,
	addr: SocketAddr,
	byte_buffer: [u8; 4096],
	buffer: VecDeque<T>,
}

impl<T> UdpEndpoint<T> {
    pub fn new(port: u16, buffer_size: usize) -> UdpEndpoint<T> {
        // Tokio UdpSocket only allows bind in async runtime, so we create std first and convert
        let addr = format!("0.0.0.0:{}", port).as_str().parse::<SocketAddr>().unwrap();
        //let mut socket_std = std::net::UdpSocket::bind(addr).unwrap();

        let buffer: VecDeque<T> = VecDeque::with_capacity(buffer_size);

		UdpEndpoint {
			port: port,
			//socket: UdpSocket::from_std(socket_std).unwrap(),
			socket: None,
			addr: addr,
			byte_buffer: [0; 4096],
			buffer: buffer,
		}
	}
}

// TODO investigate possibility of serializing into VecDeque, and use unsafe/self-referential code
// This allows zero copy ingestion - but may require some work with Unpin
impl<T> Unpin for UdpEndpoint<T> where T: Unpin {}

#[async_trait]
impl<T> StreamWrap<T> for UdpEndpoint<T> 
    where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32> + Serialize + Unpin
{
    async fn next(&mut self) -> Option<T> {
		if self.socket.is_none() {
			self.socket = Some(UdpSocket::bind(self.addr).await.unwrap())
		}
		let activated_socket = self.socket.as_ref().unwrap();
        loop {
            match self.buffer.pop_front() {
                None => (),
                x => { return x; }
            }
            match activated_socket.recv(&mut self.byte_buffer).await {
                Ok(recv_size) => {
                    if recv_size <= 1 {
                        return None; // Termination Condition
                    }
                    else {
                        // Deserialization
                        match bincode::deserialize::<Vec<T>>(&self.byte_buffer[0..recv_size]) {
                            Ok(data) => {
                                for x in data {
                                    self.buffer.push_back(x); // Push to internal queue, loop back and return item
                                }
                                continue;
                            }
                            Err(e) => {
                                println!("{:?}", e); // Deserialization error... do something robust here?
                            }
                        }
                    }
                }
                Err(_e) => {
                    println!("Error: UdpEndpoint could not process received data");
                    continue;
                }
            }
        }
    }
}
