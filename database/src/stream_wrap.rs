use async_trait::async_trait;

use futures::stream::{Stream,StreamExt};
use tokio::stream::StreamMap;
use tokio::net::{UdpSocket,TcpListener,TcpStream};
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio::sync::mpsc::{channel,Receiver};
use tokio::sync::mpsc::error::TryRecvError;
use std::net::SocketAddr;
use std::pin::Pin;

use serde::Serialize;
use serde::de::DeserializeOwned;

use std::collections::VecDeque;
use std::fmt::Debug;

const DEFAULT_CLIENT_CHANNEL_BUFFER: usize = 128;

/*
TODO
1) static lifetime for StreamWrap<T> for Stream<T> - maybe too restrictive
2) initialize UdpBind - find better ways than using option (UdpSocket can only be initialized asynchronously)
3) Tcp - detect when TcpStream is disconnected. Currently no way to check disconnect
4) Termination condition - currently terminates everything if ANY client sends sth less than 2 bytes!
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
        let addr = format!("0.0.0.0:{}", port).as_str().parse::<SocketAddr>().unwrap();
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


// Need separate struct for internal data
pub struct TcpEndpoint<T> {
    port: u16,
    clients: StreamMap<usize,Framed<TcpStream,LengthDelimitedCodec>>,
    client_channel: Option<Receiver<Framed<TcpStream,LengthDelimitedCodec>>>,
	addr: SocketAddr,
	//byte_buffer: [u8; 4096],
	buffer: VecDeque<T>,
	next_id: usize
}

impl<T> TcpEndpoint<T> {
    pub fn new(port: u16, buffer_size: usize) -> TcpEndpoint<T> {
        let addr = format!("0.0.0.0:{}", port).as_str().parse::<SocketAddr>().unwrap();
        let buffer: VecDeque<T> = VecDeque::with_capacity(buffer_size);

		TcpEndpoint {
			port: port,
            clients: StreamMap::new(),
            client_channel: None,
			addr: addr,
			//byte_buffer: [0; 4096],
			buffer: buffer,
			next_id: 0
		}
    }
    
    // Function to get connected clients (TcpStream) from separately spawned TcpListener
    fn get_connected_clients(&mut self) {
        let client_channel_ref = self.client_channel.as_mut().unwrap();
        loop {
            match client_channel_ref.try_recv() {
                Ok(new_client) => {
					// IMPORTANT to maintain always increasing next_id
					// or else it will overwrite existing connections
					// Perhaps this can be used to limit connections
                    self.clients.insert(self.next_id, new_client);
					self.next_id += 1;
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
                Err(TryRecvError::Closed) => {
                    // TODO listener is closed... what to do?
                    println!("TcpListener for this signal is closed");
                    break;
                }
            }
        }
    }
}


impl<T> Unpin for TcpEndpoint<T> where T: Unpin {}

#[async_trait]
impl<T> StreamWrap<T> for TcpEndpoint<T> 
    where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32> + Serialize + Unpin
{
    async fn next(&mut self) -> Option<T> {
		if self.client_channel.is_none() {
            // Initial binding & listener setup
            println!("Initialization");
            let listener = TcpListener::bind(self.addr).await.unwrap();
            let (sender, receiver) = channel::<Framed<TcpStream,LengthDelimitedCodec>>(DEFAULT_CLIENT_CHANNEL_BUFFER);
            self.client_channel = Some(receiver);
            println!("Bind complete");
            tokio::spawn(async move {
                loop {
                    // TODO safe shutdown of listener
                    match listener.accept().await {
                        Ok((client, _)) => {
                            println!("Listener accepted new client");
                            match sender.send(Framed::new(client, LengthDelimitedCodec::new())).await {
                                Ok(()) => (),
                                Err(e) => {
                                    // TODO error handling for failing to send TcpStream from listener to main task
                                    println!("Listener could not send new connection stream: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            // TODO error handling for listening
                            println!("Error in accepting new client - {:?}", e);
                        }
                    }
                }
            });
            
            // Asynchronously wait until we have at least one client
            match self.client_channel.as_mut().unwrap().recv().await {
                Some(new_client) => {
                    self.clients.insert(self.next_id, new_client);
					self.next_id += 1;
                }
                None => {
                    // Return as TcpListener closed channel for some reason
                    return None;
                }
            }
		}
        
        let return_data: Option<T>;
        loop {
            match self.buffer.pop_front() {
                None => (),
                x => { 
                    return_data = x;
                    break;
                }
            };

            // TODO add support for removed/disonnected clients
            match futures::stream::StreamExt::next(&mut self.clients).await {
                // Receive as BytesMut
                Some((_clientid, recv)) => {
                    match recv {
                        Ok(bytes) => {
                            if bytes.len() <= 1 {
                                return_data = None; // Termination Condition
                                return None;
                            }
                            else {
                                // Deserialization
                                //match bincode::deserialize::<Vec<T>>(&self.byte_buffer[0..recv_size]) {
                                match bincode::deserialize::<Vec<T>>(&bytes) {
                                    Ok(data) => {
                                        for x in data {
                                            self.buffer.push_back(x); // Push to internal queue, loop back and return item
                                        }
                                    }
                                    Err(e) => {
                                        println!("Deserialization error - {:?}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error in receiving from client {:?}", e);
                        }
                    } 
                }
                None => {
                    // TODO error handling in receiving data from client
                    // This should never happen - the Framed should keep on receiving data
                    println!("Unexpected halt in getting data from client");
                }
            }
        }

        // TODO handle removing disconnected clients

        // Check new connections
        self.get_connected_clients();
        return return_data;
    }
}
