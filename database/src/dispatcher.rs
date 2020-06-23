use crate::client::*;
use crate::remote_stream::{RemoteStream, remote_stream_from_receiver};
extern crate bincode;

// Initialization of Receivers
use std::time::{Duration, Instant};
use tokio::timer::Interval;
// Queue communication
use std::sync::mpsc::{channel, Sender};
// Stopping dispatcher
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
// ZMQ client handling
use zmq::SNDMORE;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
// T Traits
use std::fmt::Debug;
use serde::de::DeserializeOwned;




pub struct ZMQDispatcher<T>
{
    num_clients: usize,
    stream_queues: Vec<Sender<T>>,
    port: u16,
}

fn copy_params(amount: &Amount, run_period: &RunPeriod, frequency: &Frequency,
                freq_start: &Option<Instant>, freq_interval: &Option<Duration>)
    -> (Amount, RunPeriod, Frequency)
{
    let copied_amount = match amount {
        Amount::Limited(value) => Amount::Limited(*value),
        Amount::Unlimited => Amount::Unlimited
    };

    let copied_period = match run_period {
        RunPeriod::Finite(dur) => RunPeriod::Finite(*dur),
        RunPeriod::Indefinite => RunPeriod::Indefinite
    };

    let copied_freq = match frequency {
        Frequency::Immediate => Frequency::Immediate,
        Frequency::Delayed(_int) => match (freq_start, freq_interval) {
            // TODO: no guarantee that _int = Interval::new(inst.clone(), dur.clone())
            (Some(inst), Some(dur)) => Frequency::Delayed(Interval::new(*inst, *dur)),
            (_, _) => {
                panic!("Frequency is not immediate but frequency start/interval passed as None for making ZMQ")
            }
        }
    };

    (copied_amount, copied_period, copied_freq)
}

pub fn make_remote_streams<T>(num_clients: usize, port: u16, amount: Amount, run_period: RunPeriod,
                frequency: Frequency, freq_start: Option<Instant>, freq_interval: Option<Duration>)
                -> (ZMQDispatcher<T>, Vec<Box<RemoteStream<T>>>)
{
    let mut stream_queues: Vec<Sender<T>> = Vec::with_capacity(num_clients);
    let mut clients: Vec<Box<RemoteStream<T>>> = Vec::with_capacity(num_clients);
    for _i in 0..num_clients {
        let (send, recv) = channel::<T>();
        stream_queues.push(send);
        let (copied_amount, copied_period, copied_freq) = copy_params(&amount, &run_period, &frequency, &freq_start, &freq_interval);
        clients.push(Box::new(remote_stream_from_receiver::<T>(recv, copied_amount, copied_period, copied_freq)));
    }
    let dispatcher = ZMQDispatcher {
        num_clients: num_clients,
        stream_queues: stream_queues,
        port: port,
    };
    (dispatcher, clients)
}

impl<T> ZMQDispatcher<T>
    where T: Copy + Send + Sync + DeserializeOwned + Debug + From<f32>
{
    // TODO throw the error up - return Ok() or Err()
    fn push_to_queue(&self, index: usize, data: &[u8], recv_size: usize) {
        // TODO handle recv_size: what if larger than 1024?
        match bincode::deserialize::<Vec<T>>(&data[0..recv_size]) {
            Ok(data) => {
                println!("{:?}", data); // For testing
                for x in data {
                    match self.stream_queues[index].send(x) {
                        Ok(()) => (),
                        Err(e) => {
                            // Send error... do something robust here?
                            println!("{:?}", e); 
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

    pub fn run(&self, continue_status: Arc<AtomicBool>) {
        let context = zmq::Context::new();
        let router = context.socket(zmq::ROUTER).unwrap();
        assert!(router.bind(format!("tcp://*:{}", self.port).as_str()).is_ok());
        
        let mut count_clients: usize = 0;
        let mut clients: HashMap<[u8; 5], usize, RandomState> = HashMap::with_capacity(self.num_clients);
    
        let mut identity: [u8; 5] = [0; 5]; // Length of default ZMQ identity
        let mut data: [u8; 1024] = [0; 1024];
        let mut id_size: usize;
        let mut recv_size: usize;
        
        /* TODO
         * Don't forget to uncomment the 2 lines in lib.rs!
         * 
         * -1) Fix 
         * 
         * 0) The copy_params is very unnecessary. Possible to get around by just implementing Copy trait
         *      in client.rs. Right now tokio::Interval does not implement Copy trait, so we do it this way
         * 1) Various client communications.
         *      1-1) Initial client "reception" and sending confirmation - do something useful
         *          - ex) type data send, data limit send, etc.
         *      1-2) How much data will client send? Right now limited to 1024 bytes.
         *          - Behavior when data is truncated?
         * 2) Floating point precision lost - maybe due to From<f32>?
         * 3) Various error handling cases - remove unwrap() and make it robust
         * 4) Dispatcher end - has to check for AtomicBool everytime, what is the cost of that?
         * 5) Efficiency of multipart message & SENDMORE signal to reduce number of data frames sent?
         *      - More data deserialized at once = better?
         *      - Is there a specific scheme planned for how client is going to send data? (ex. how often, how much, etc.)
         * 6) Rust programming pattern: continuing inside match - how to do without mutable variables?
         *      - ex) the first match in loop: can't use "let id_size = match ..." because we have to continue
         * 7) Identity hashing scheme: use faster hashing scheme?
         * 8) Identity size: right now id_size doesn't do anything as id is fixed to 5 bytes
         */
        while continue_status.load(Ordering::Acquire) {
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
                    recv_size = size;
                }
                Err(_e) => {
                    println!("Error: dispatcher could not process received data");
                    continue;
                }
            }
            
            // Figure out identity
            {
                match clients.get(&identity) {
                    Some(index) => {
                        // Process incoming data (push to queue)
                        if recv_size > 1024 {
                            // TODO do something more substantial for truncated
                            println!("Dispatcher: Truncation");
                        }
                        {
                            self.push_to_queue(*index, &data, recv_size);
                        }
                    }
                    None => {
                        // New client
                        // TODO ensure that clients.len() == count_clients + 1;
                        if clients.len() < self.num_clients {
                            // Can accept new client - send configuration data
                            clients.insert(identity, count_clients);
                            count_clients += 1;
                            router.send(&identity[0..id_size], SNDMORE).unwrap();
                            router.send("Client accepted", 0).unwrap();
                        }
                        else {
                            println!("Error: dispatcher has max number of clients");
                            continue;
                        }
                    }
                }  
            } 
        }
    }
}


