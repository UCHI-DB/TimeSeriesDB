extern crate bincode;

// Stopping dispatcher
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
// Hash Map
use std::collections::HashMap;
use fnv::{FnvHashMap, FnvBuildHasher};
// Std Net
use std::io::{Write, Read};
use std::net::{TcpListener, TcpStream, Shutdown};
// T Traits
use std::fmt::Debug;
use serde::Serialize;
use serde::de::DeserializeOwned;

// For sending stuff
use bincode::serialize;
use std::any::type_name;


/*
 * Server that sends 1) data type and 2) the signal ID-to-port mapping
 * Does not use Tokio, but runs on a separate thread
 */

pub struct MappingServer
{
    mapping: HashMap<u64, u16, FnvBuildHasher>,
    port: u16,
}

impl MappingServer
    
{
    pub fn new(mapping: HashMap<u64, u16, FnvBuildHasher>, port: u16) -> MappingServer {
        MappingServer {
            mapping: mapping,
            port: port,
        }
    }
    
    // CHECK TODO
    pub fn run<T>(&mut self, continue_status: Arc<AtomicBool>)
        where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32> + Serialize
    {
        // From TCP Listener
        let addr = format!("0.0.0.0:{}", self.port);
        let mut listener = TcpListener::bind(&addr.as_str()).unwrap();

        // Serialize content
        let type_name = type_name::<T>(); // TODO unsafe code
        let vector = serialize(&(type_name, self.mapping.clone())).unwrap();
        let serialized = vector.as_slice();

        println!("Ready to send mapping!");

        let mut data = [0; 4];

        // Process requests
        //for ref mut stream_result in listener.incoming() {
        while (*continue_status).load(Ordering::Acquire) {
            // TODO as listener is blocking, the atomic is useless
            // the joining part in lib.rs is also commented out for now
            match listener.accept() {
                Ok((ref mut stream, _addr)) => {
                    match stream.read(&mut data) {
                        Ok(_size) => {
                            stream.write(&serialized).unwrap();
                        },
                        Err(_) => {
                            println!("An error occurred, terminating connection with {}", stream.peer_addr().unwrap());
                        }
                    }
                }
                Err(e) => {
                    println!("{:?}", e);
                }
            }
        }
    }
}


