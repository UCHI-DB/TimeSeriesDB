extern crate bincode;

use std::time::Instant;
use tokio::prelude::*;
use std::fmt::Debug;

use futures::stream::Stream;
use std::net::SocketAddr;
//use tokio::net::{TcpListener, TcpStream};
use std::net::{TcpListener, TcpStream};
use std::io::{Write, Read};

use serde::Serialize;
use serde::de::DeserializeOwned;

use std::collections::VecDeque;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;

use std::ffi::c_void;
/*
 * Struct that implements the Stream trait
 * Receives TCP input, deserializes it, and sends it back
 */

/* USING STD TCPSTREAM/TCPLISTEN */
/* SEE COMMENTED SECTION FOR TOKIO - for some reason TcpListener is always not ready... */


pub struct TcpEndpoint<T> 
{
	port: u16,
	cx: Arc<zmq::Context>,
	//router_wrapped: Option<*mut c_void>,
	router_wrapped: Option<zmq::Socket>,
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
	thread_local! {
        // Could add pub to make it public to whatever Foo already is public to.
        static FOO: RefCell<Option<zmq::Socket>> = RefCell::new(None);
    }
}

unsafe impl<T> Send for TcpEndpoint<T> {}
unsafe impl<T> Sync for TcpEndpoint<T> {}

impl<T> Stream for TcpEndpoint<T>
	where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32> + Serialize
{
	type Item = T;
	type Error = ();
	
	
	fn poll(&mut self) -> Poll<Option<T>,()> {
		
		loop {
			match self.buffer.pop_front() {
				Some(x) => {
					return Ok(Async::Ready(Some(x)));
				}
				None => {
					unsafe {
						match self.router_wrapped {
							Some(ref mut r) => (),
							None => {
								let x = self.cx.socket(zmq::ROUTER).unwrap();
								assert!(x.bind(format!("tcp://*:{}", self.port).as_str()).is_ok());
								self.router_wrapped = Some(x);
								//let mut x = Rc::new(self.cx.socket(zmq::ROUTER).unwrap());
								//assert!(x.bind(format!("tcp://*:{}", self.port).as_str()).is_ok());
								//let raw = x.into_raw();
								//self.router_wrapped = Some(raw.clone());
								//println!("ptr: {:?}", raw);
								//zmq::Socket::from_raw(raw)
								
							}
						};
						
						let router = self.router_wrapped.as_ref().unwrap();
						//let router = self.cx.socket(zmq::ROUTER).unwrap();
						//assert!(router.connect(format!("tcp://*:{}", self.port).as_str()).is_ok());
						
	
						let mut id_size: usize;
						
						match router.recv_into(&mut self.id_buffer, 0) {
							Ok(size) => {
								id_size = size;
							}
							Err(e) => {
								println!("{:?}", e);
								continue;
							}
						}
						// Dummy data strategy
						match router.recv_into(&mut self.byte_buffer, 0) {
							Ok(recv_size) => {
								if recv_size <= 1 {
									// We receive only when buffer is empty, so we can exit now
									return Ok(Async::Ready(None));
								}
								// END TESTING
								else {
									// Deserialization
									match bincode::deserialize::<Vec<T>>(&self.byte_buffer[0..recv_size]) {
										Ok(data) => {
											// TODO no copy deserialization possible?
											for x in data {
												self.buffer.push_back(x);
												// Loop back and return item
											}
											continue;
										}
										Err(e) => {
											// Deserialization error... do something robust here?
											println!("{:?}", e); 
											return Ok(Async::NotReady);
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


/*
pub struct TcpEndpoint<T> 
{
	port: u16,
	listener: TcpListener,
	byte_buffer: [u8; 4096],
	buffer: VecDeque<T>,
}

impl<T> TcpEndpoint<T>
{
	pub fn new(port: u16, buffer_size: usize) -> TcpEndpoint<T> {
		let addr = format!("127.0.0.1:{}", port).as_str().parse::<SocketAddr>().unwrap();
		let listener = TcpListener::bind(&addr).unwrap();
		let buffer: VecDeque<T> = VecDeque::with_capacity(buffer_size);
		TcpEndpoint {
			port: port,
			listener: listener,
			byte_buffer: [0; 4096],
			buffer: buffer,
		}
	}
}


/* 
 * TODO deserialization no-copy possible? (Currently puts in byte buffer, deserializes,
 * 	and copies contents of resulting vector into VecDeque buffer)
 * 
 * TODO recv_size exceeds buffer size handling
 */
impl<T> Stream for TcpEndpoint<T>
	where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32> + Serialize
{
	type Item = T;
	type Error = ();
	
	
	fn poll(&mut self) -> Poll<Option<T>,()> {
		loop {
			match self.buffer.pop_front() {
				Some(x) => {
					println!("buffer pop");
					return Ok(Async::Ready(Some(x)));
				}
				None => {
					// Buffer empty, receive from remote client
					
					match self.listener.poll_accept() {
						Ok(Async::Ready((ref mut stream, _addr))) => {
							loop {
								// TODO Loop until TCPStream has something - efficiency/blocking issues?
								match stream.poll_read(&mut self.byte_buffer) {
									Ok(Async::Ready(recv_size)) => {
										// FOR TESTING termination condition
										println!("{}", recv_size);
										if recv_size <= 1 {
											// We receive only when buffer is empty, so we can exit now
											return Ok(Async::Ready(None));
										}
										// END TESTING
										// Deserialization
										match bincode::deserialize::<Vec<T>>(&self.byte_buffer[0..recv_size]) {
											Ok(data) => {
												// TODO no copy deserialization possible?
												for x in data {
													self.buffer.push_back(x);
													// Loop back and return item
												}
												break;
											}
											Err(e) => {
												// Deserialization error... do something robust here?
												println!("{:?}", e); 
												return Ok(Async::NotReady);
											}
										}
									}
									Ok(Async::NotReady) => {
										println!("Got TCPStream but data not ready");
									}
									Err(e) => {
										println!("{:?}", e);
										return Ok(Async::NotReady);
									}
								}
							}
						}
						Ok(Async::NotReady) => {
							//println!("listener not ready");
							return Ok(Async::NotReady);
						}
						Err(e) => {
							println!("{:?}", e);
							return Ok(Async::NotReady);
						}
					}
				}
			}
		}
	}
}
*/
