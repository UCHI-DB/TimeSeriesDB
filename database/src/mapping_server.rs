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
    pub fn run<T>(&mut self, context: Arc<zmq::Context>, continue_status: Arc<AtomicBool>)
        where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32> + Serialize
    {
        let router = context.socket(zmq::ROUTER).unwrap();
        assert!(router.bind(format!("tcp://*:{}", self.port).as_str()).is_ok());

        // Serialize content
        let type_name = type_name::<T>(); // TODO unsafe code
        let serialized = serialize(&(type_name, self.mapping.clone())).unwrap();
        println!("Ready to send mapping!");

        let mut identity: [u8; 5] = [0; 5]; // Length of default ZMQ identity
        let mut data: [u8; 4] = [0; 4];
        let mut id_size: usize;
        let mut recv_size: usize;
	
        while (*continue_status).load(Ordering::Acquire) {
            // Receive data
            match router.recv_into(&mut identity, 0) {
                Ok(size) => {
                    id_size = size;
                }
                Err(_e) => {
                    println!("Error: dispatcher could not process received data");
                    continue;
                }
            }
            match router.recv_into(&mut data, 0) {
                Ok(size) => {
                    router.send(&identity[0..id_size], zmq::SNDMORE).unwrap();
                    router.send(&serialized, 0).unwrap();
                }
                Err(_e) => {
                    println!("Error: dispatcher could not process received data");
                    continue;
                }
            }
        }
    }
}


