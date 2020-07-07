extern crate bincode;

// Queue communication
use futures::sync::mpsc::Sender;
// Stopping dispatcher
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
// ZMQ client handling
use zmq::SNDMORE;
use std::collections::{HashMap,HashSet};
use std::collections::hash_map::RandomState;
// T Traits
use std::fmt::Debug;
use serde::de::DeserializeOwned;

use std::any::type_name;




pub struct ZMQDispatcher<T>
{
    id: u64,
    stream_queues: HashMap<u64, Sender<T>, RandomState>,
    port: u16,
}

impl<T> ZMQDispatcher<T>
    where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32>
{
    pub fn new(id: u64, stream_queues: HashMap<u64, Sender<T>, RandomState>, port: u16) -> ZMQDispatcher<T> {
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
                println!("Dispatcher received at port {}: {:?}", self.port, data); // For testing
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
        let mut data: [u8; 1024] = [0; 1024];
        let mut id_size: usize;
        let mut recv_size: usize;
        
        /* TODO
         * Don't forget to uncomment the 2 lines in lib.rs!
         * 
         * 0) Fix result / null returning --> Best behavior would be to gracefully handle errors, so that is implemented
         * 1) Various client communications.
         *      Right now, client ID is checked to send back initial message... should this be removed?
         *      1-1) Currently just sends data type.
         *      1-2) How much data will client send? Right now limited to 1024 bytes.
         *          - Behavior when data is truncated?
         * 2) Floating point precision lost - maybe due to From<f32>?
         * 3) Various error handling cases - remove unwrap() and make it robust
         * 4) Dispatcher end - has to check for AtomicBool everytime, what is the cost of that?
         * 5) Efficiency of multipart message & SENDMORE signal to reduce number of data frames sent?
         *      - More data deserialized at once = better?
         *      - Is there a specific scheme planned for how client is going to send data? (ex. how often, how much, etc.)
         */
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
            /*
            // 1) Client hashing strategy
            match router.recv_into(&mut data, 0) {
                Ok(size) => {
                    recv_size = size;
                }
                Err(_e) => {
                    println!("Error: dispatcher could not process received data");
                    continue;
                }
            }
            
            // Figure out identity
            {
                if clients.contains(&identity) {
                    // Process incoming data (push to queue)
                    if recv_size > 1024 {
                        // TODO do something more substantial for truncated
                        println!("Dispatcher {}: Truncation", self.id);
                    }
                    self.push_to_queue(&data, recv_size);
                }
                else {
                    // New client
                    clients.insert(identity);
                    router.send(&identity[0..id_size], SNDMORE).unwrap();
                    router.send(type_name::<T>(), 0).unwrap();
                }
            }  */
            // Dummy data strategy
            match router.recv_into(&mut data, 0) {
                Ok(size) => {
                    if size <= 1 {
                        router.send(&identity[0..id_size], SNDMORE).unwrap();
                        router.send(type_name::<T>(), 0).unwrap();
                    } else if size > 1024 {
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


