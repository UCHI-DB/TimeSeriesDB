extern crate bincode;

// Queue communication
use futures::sync::mpsc::Sender;
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
	/*	
	let test_vec = vec![(10 as u64, T::from(0.6942098025)),
		(10 as u64, T::from(0.2363603626)), 
		(10 as u64, T::from(0.7426826748)), 
		(10 as u64, T::from(0.6549429643)), 
		(10 as u64, T::from(0.6570997322)), 
		(10 as u64, T::from(0.9376189143)), 
		(10 as u64, T::from(0.3027290404)), 
		(10 as u64, T::from(0.9612739095)), 
		(10 as u64, T::from(0.4048058438)), 
		(10 as u64, T::from(0.0791666921)), 
		(10 as u64, T::from(0.5480497558)), 
		(10 as u64, T::from(0.0791558033)), 
		(10 as u64, T::from(0.0927286445)), 
		(10 as u64, T::from(0.0916129725)), 
		(10 as u64, T::from(0.8259950942)), 
		(10 as u64, T::from(0.7093062181)), 
		(10 as u64, T::from(0.0629796595)), 
		(10 as u64, T::from(0.0633001504)), 
		(10 as u64, T::from(0.3604035231)), 
		(10 as u64, T::from(0.8389628904)), 
		(10 as u64, T::from(0.7397309258)), 
		(10 as u64, T::from(0.1714681204)), 
		(10 as u64, T::from(0.3353240721)), 
		(10 as u64, T::from(0.2571632450)), 
		(10 as u64, T::from(0.7458096100)), 
		(10 as u64, T::from(0.3890291011)), 
		(10 as u64, T::from(0.8550782610)), 
		(10 as u64, T::from(0.2099220916)), 
		(10 as u64, T::from(0.6091594252)), 
		(10 as u64, T::from(0.9053960369))];
		
		for i in 0..3333333 {
	                for (sig_id, x) in &test_vec {
	                    match self.stream_queues.get_mut(&sig_id) {
	                        Some(ref mut sender) => {
	                            loop {
	                                match sender.try_send(*x) {
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
	
	let test_data = bincode::serialize(&test_vec).unwrap();
	let test_size = test_data.len();
	let mut counter: u64 = 0;
	*/
	
        
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
	/*
	while (*continue_status).load(Ordering::Acquire) {
		if counter < 3333333 {
			self.push_to_queue(&test_data, test_size);
			counter += 1;
		}
		else {
                        for (sig_id, sender_opt) in self.stream_queues.iter_mut() {
                            drop(sender_opt);
			}
			return;	
		}
	};
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


