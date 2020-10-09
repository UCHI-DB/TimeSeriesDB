extern crate bincode;

// Queue communication
use futures::channel::mpsc::Sender;
// Stopping dispatcher
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
// ZMQ client handling
use zmq::SNDMORE;
use std::collections::{HashMap,HashSet};
use fnv::{FnvHashMap, FnvBuildHasher};
use std::collections::hash_map::RandomState;
// T Traits
use std::fmt::Debug;
use serde::Serialize;
use serde::de::DeserializeOwned;

use std::any::type_name;




pub struct ZMQDispatcher<T>
{
    id: u64,
    stream_queues: HashMap<u64, Sender<T>, FnvBuildHasher>,
    port: u16,
}

impl<T> ZMQDispatcher<T>
    where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32> + Serialize
{
    pub fn new(id: u64, stream_queues: HashMap<u64, Sender<T>, FnvBuildHasher>, port: u16) -> ZMQDispatcher<T> {
        ZMQDispatcher {
            id: id,
            stream_queues: stream_queues,
            port: port,
        }
    }
    
    fn push_to_queue(&mut self, data: &[u8], recv_size: usize) {
        // TODO handle recv_size: what if larger than 1024?
        match bincode::deserialize::<Vec<(u64, T)>>(&data[0..recv_size]) {
            Ok(data) => {
                //println!("Dispatcher received at port {}: {:?}", self.port, data); // For testing
                for (sig_id, x) in data {
                    match self.stream_queues.get_mut(&sig_id) {
                        Some(ref mut sender) => {
                            loop {
                                match sender.try_send(x) {
                                    Ok(()) => { break; },
                                    Err(e) => {
                                        // Send error
                                        if e.is_full() {
                                            continue; // Wait till exhausted
                                        } else if e.is_disconnected() {
                                            println!("Dispatcher cannot connect to queue {:?}", sig_id);
                                            break;
                                        } else {
                                            println!("{:?}", e);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            // Unknown signal ID... do something robust here?
                            println!("Unknown signal ID {:?}", sig_id)
                        }
                    }
                }
            }
            Err(e) => {
                // Deserialization error... do something robust here?
                println!("{:?}", e); 
            }
        }
    }

    
    pub fn run(&mut self, continue_status: Arc<AtomicBool>) {
        let context = zmq::Context::new();
        let router = context.socket(zmq::ROUTER).unwrap();
        assert!(router.bind(format!("tcp://*:{}", self.port).as_str()).is_ok());
        
        let mut clients: HashSet<[u8; 5]> = HashSet::new();
    
        let mut identity: [u8; 5] = [0; 5]; // Length of default ZMQ identity
        let mut data: [u8; 4096] = [0; 4096];
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
            // Dummy data strategy
            match router.recv_into(&mut data, 0) {
                Ok(size) => {
                    if size <= 0 {
                        // Initialization
                        router.send(&identity[0..id_size], SNDMORE).unwrap();
                        router.send(type_name::<T>(), 0).unwrap();
                    } else if size <= 1 {
                        // ONLY FOR PERFORMANCE TESTING - in a multiple client scenario, we cannot just close the signals
                        // Requires single dispatcher, in multiple case all dispatchers must be closed
                        // Finish
                        // Most recent version of futures should use close_channel
                        for (sig_id, sender_opt) in self.stream_queues.iter_mut() {
                            drop(sender_opt);
                            /* match self.stream_queues.remove(&sig_id) {
                                Some(ref mut sender) => { drop(sender); }
                                None => { () }
                            } */
                        }
                        return;
                    } else if size > 4096 {
                        println!("Dispatcher {}: Truncation", self.id);
                    } else {
                        self.push_to_queue(&data, size);
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


