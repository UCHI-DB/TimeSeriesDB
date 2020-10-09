extern crate bincode;

use std::fmt::Debug;
use std::net::SocketAddr;

use serde::Serialize;
use serde::de::DeserializeOwned;

use std::collections::VecDeque;
use std::sync::Arc;

// Tokio stuff
use tokio::stream::Stream;
use tokio::net::TcpListener;
use tokio::net::UdpSocket;
use std::pin::Pin;
use std::task::{Context, Poll}; // Added in updating tokio
use std::ops::{Deref, DerefMut};


/*
 * Struct that implements the Stream trait
 * Receives TCP input, deserializes it, and sends it back
 */

pub struct TcpEndpoint<T> 
{
	port: u16,
	cx: Arc<zmq::Context>, // ZMQ Context
	router_wrapped: Option<zmq::Socket>, // ZMQ Socket
	id_buffer: [u8; 5],
	byte_buffer: [u8; 4096],
	buffer: VecDeque<T>,
}

impl<T> TcpEndpoint<T>
{
	pub fn new(port: u16, buffer_size: usize, cx: Arc<zmq::Context>) -> TcpEndpoint<T> {
		let addr = format!("0.0.0.0:{}", port).as_str().parse::<SocketAddr>().unwrap();
		let buffer: VecDeque<T> = VecDeque::with_capacity(buffer_size);
		TcpEndpoint {
			cx: cx,
			port: port,
			router_wrapped: None,
			id_buffer: [0; 5],
			byte_buffer: [0; 4096],
			buffer: buffer,
		}
	}
}

unsafe impl<T> Send for TcpEndpoint<T> {}
unsafe impl<T> Sync for TcpEndpoint<T> {}

impl<T> Unpin for TcpEndpoint<T> where T: Unpin {}

impl<T> Stream for TcpEndpoint<T>
	where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32> + Serialize + Unpin
{
	type Item = T;
	
	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mutable_self = self.get_mut();
		loop {
			match mutable_self.buffer.pop_front() {
				Some(x) => {
					return Poll::Ready(Some(x));
				}
				None => {
					unsafe {
						match mutable_self.router_wrapped {
							Some(ref mut r) => (),
							None => {
								let x = mutable_self.cx.socket(zmq::ROUTER).unwrap();
								assert!(x.bind(format!("tcp://*:{}", mutable_self.port).as_str()).is_ok());
								mutable_self.router_wrapped = Some(x);
							}
						};
						
						let router = mutable_self.router_wrapped.as_ref().unwrap();
	
						let mut id_size: usize;

						match router.recv_into(&mut mutable_self.id_buffer, 0) {
							Ok(size) => {
								id_size = size;
							}
							Err(e) => {
								println!("{:?}", e);
								continue;
							}
						}
						// Dummy data strategy
						match router.recv_into(&mut mutable_self.byte_buffer, 0) {
							Ok(recv_size) => {
								if recv_size <= 1 {
									// We receive only when buffer is empty, so we can exit now
									return Poll::Ready(None);
								}
								// END TESTING
								else {
									// Deserialization
									match bincode::deserialize::<Vec<T>>(&mutable_self.byte_buffer[0..recv_size]) {
										Ok(data) => {
											// TODO no copy deserialization possible?
											for x in data {
												mutable_self.buffer.push_back(x);
												// Loop back and return item
											}
											continue;
										}
										Err(e) => {
											// Deserialization error... do something robust here?
											println!("{:?}", e);
											cx.waker().wake_by_ref(); // Wakes immediately
											return Poll::Pending;
										}
									}
								}
							}
							Err(_e) => {
								println!("Error: dispatcher could not process received data");
								continue;
							}
						}
					}
					
				}
			}
		}
	}
}

/* https://stackoverflow.com/questions/61295176/how-to-implement-a-future-stream-that-polls-async-fnmut-self */ 