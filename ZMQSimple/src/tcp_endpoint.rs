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

/*
 * Struct that implements the Stream trait
 * Receives TCP input, deserializes it, and sends it back
 */

/* USING STD TCPSTREAM/TCPLISTEN */
/* SEE COMMENTED SECTION FOR TOKIO - for some reason TcpListener is always not ready... */


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
		let addr = format!("0.0.0.0:{}", port).as_str().parse::<SocketAddr>().unwrap();
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
					match self.listener.accept() {
						Ok((ref mut stream, _addr)) => {
							loop {
								// TODO Loop until TCPStream has something - efficiency/blocking issues?
								match stream.read(&mut self.byte_buffer) {
									Ok(recv_size) => {
										// FOR TESTING termination condition
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
													break;
												}
												Err(e) => {
													// Deserialization error... do something robust here?
													println!("{:?}", e); 
													return Ok(Async::NotReady);
												}
											}
										}
									}
									Err(e) => {
										println!("{:?}", e);
										return Ok(Async::NotReady);
									}
								}
							}
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
		let addr = format!("0.0.0.0:{}", port).as_str().parse::<SocketAddr>().unwrap();
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
