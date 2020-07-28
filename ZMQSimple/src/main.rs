use std::time::{Duration, Instant};
use futures::future::join;
use futures::stream::StreamExt;
use futures::sink::SinkExt;
use futures::executor::block_on;
use futures::task::{Spawn, SpawnExt};
use tokio::runtime::Builder;
use futures::executor::ThreadPool;
use futures::executor::ThreadPoolBuilder;
use futures::channel::mpsc::{TryRecvError, Receiver, Sender, channel};
use std::collections::{HashMap,HashSet};
use fnv::{FnvHashMap, FnvBuildHasher};
use std::thread;
use zmq::Socket;

/* Dummy server for ZMQClient that deserialize, hashes, and sends to appropriate MPSC queue 
 * to figure out where the bottle neck is
 * Use with argument "a" for testing async, "t" for testing 2-thread setup
 */

/* Deserial-only test */

fn run_deserial_only(router: Socket) -> u64 {

	let mut id = [0; 5];
	let mut msg = [0; 4096];
	let mut zmq_count: u64 = 0;

	// Initial ZMQ Connection
	router.recv_into(&mut id, 0).unwrap();
	router.recv_into(&mut msg, 0).unwrap();
	router.send(&id[0..5], zmq::SNDMORE).unwrap();
	router.send("f32", 0).unwrap();
	
	// Main loop
	loop {
		router.recv_into(&mut id, 0).unwrap();
		let recv_size = router.recv_into(&mut msg, 0).unwrap();
		if recv_size <= 1 {
			break;
		}
			
		else {
			// Deserialize
			match bincode::deserialize::<Vec<(u64, f32)>>(&msg[0..recv_size]) {
				Ok(data) => {
					for (sig_id, x) in data {
						zmq_count += 1;
					}
				}
				Err(e) => {
					println!("Deserialization error: {:?}", e)
				}
			}
		}
	}

	zmq_count
}

/* Hash-only test */

fn run_hash_only(mut receiver: Receiver<f32>, remote_clients: &mut FnvHashMap<u64, Sender<f32>>, router: Socket) -> u64 {

	let mut id = [0; 5];
	let mut msg = [0; 4096];
	let mut zmq_count: u64 = 0;

	// Initial ZMQ Connection
	router.recv_into(&mut id, 0).unwrap();
	router.recv_into(&mut msg, 0).unwrap();
	router.send(&id[0..5], zmq::SNDMORE).unwrap();
	router.send("f32", 0).unwrap();
	
	// Main loop
	loop {
		router.recv_into(&mut id, 0).unwrap();
		let recv_size = router.recv_into(&mut msg, 0).unwrap();
		if recv_size <= 1 {
			break;
		}
			
		else {
			// Deserialize and hash
			match bincode::deserialize::<Vec<(u64, f32)>>(&msg[0..recv_size]) {
				Ok(data) => {
					for (sig_id, x) in data {
						zmq_count += 1;
						match remote_clients.get_mut(&sig_id) {
							Some(ref mut _sender) => (),
							None => { println!("Signal ID not found!"); }
						}
					}
				}
				Err(e) => {
					println!("Deserialization error: {:?}", e)
				}
			}
		}
	}
	// Clear remote_clients to drop all senders so that the MPSC receiving thread will end
	remote_clients.clear();

	zmq_count
}

/* 2-thread MPSC queue setup */

fn recv_sync(mut receiver: Receiver<f32>) -> u64 {
	let mut counter: u64 = 0;
	loop {
		match receiver.try_next() {
			Ok(Some(_)) => { counter += 1; }
			Ok(None) => { return counter; }
			Err(e) => ()
		}
	}
}

fn run_sync(mut receiver: Receiver<f32>, remote_clients: &mut FnvHashMap<u64, Sender<f32>>, router: Socket) -> u64 {
	let mut id = [0; 5];
	let mut msg = [0; 4096];
	let mut zmq_count: u64 = 0;

	// Initial ZMQ Connection
	router.recv_into(&mut id, 0).unwrap();
	router.recv_into(&mut msg, 0).unwrap();
	router.send(&id[0..5], zmq::SNDMORE).unwrap();
	router.send("f32", 0).unwrap();
	
	// MPSC receiving thread
	let recv_t = thread::spawn(move || {
		recv_sync(receiver)
	});
	
	// ZMQ/MPSC sending thread
	loop {
		router.recv_into(&mut id, 0).unwrap();
		let recv_size = router.recv_into(&mut msg, 0).unwrap();
		if recv_size <= 1 {
			break;
		}
			
		else {
			// Deserialize and hash
			match bincode::deserialize::<Vec<(u64, f32)>>(&msg[0..recv_size]) {
				Ok(data) => {
					for (sig_id, x) in data {
						zmq_count += 1;
						match remote_clients.get_mut(&sig_id) {
							Some(ref mut sender) => {
								loop {
									match sender.try_send(x) {
										Ok(()) => {break;}
										Err(e) => ()
									}
								}
							}
							None => { println!("Signal ID not found!"); }
						}
					}
				}
				Err(e) => {
					println!("Deserialization error: {:?}", e)
				}
			}
		}
	}
	// Clear remote_clients to drop all senders so that the MPSC receiving thread will end
	remote_clients.clear();

	let queue_count: u64 = recv_t.join().unwrap();
	if queue_count != zmq_count {
		println!("WARNING: Queue did not receive all data!");
	}
	zmq_count
	
}

/* futures-async/await MPSC queue setup */

async fn recv_async(mut receiver: Receiver<f32>) -> u64 {
	let mut counter: u64 = 0;
	while let Some(_x) = receiver.next().await {
		/*
		if counter % 100 == 0 {
			println!("recv tid: {:?}", thread::current().id());
		}
		*/
		
		counter += 1;
	}
	counter
}

async fn send_async(mut remote_clients: FnvHashMap<u64, Sender<f32>>, router: Socket) -> u64 {
	let mut id = [0; 5];
	let mut msg = [0; 4096];
	let mut zmq_count: u64 = 0;

	// Initial ZMQ connection
	router.recv_into(&mut id, 0).unwrap();
	router.recv_into(&mut msg, 0).unwrap();
	router.send(&id[0..5], zmq::SNDMORE).unwrap();
	router.send("f32", 0).unwrap();
	
	// Main ZMQ/send loop
	loop {
		/*
		if zmq_count % 100 == 0 {
			println!("send tid: {:?}", thread::current().id());
		}
		*/
		
		router.recv_into(&mut id, 0).unwrap();
		let recv_size = router.recv_into(&mut msg, 0).unwrap();
		// End condition
		if recv_size <= 1 {
			break;
		}
			
		else {
			// Deserialize and hash
			match bincode::deserialize::<Vec<(u64, f32)>>(&msg[0..recv_size]) {
				Ok(data) => {
					for (sig_id, x) in data {
						zmq_count += 1;
						match remote_clients.get_mut(&sig_id) {
							// TODO sender.send(x) flushes everytime... not good for performance!
							Some(ref mut sender) => { sender.send(x).await; }
							None => { println!("Signal ID not found!"); }
						}
					}
				}
				Err(e) => { println!("Deserialization error: {:?}", e); }
			}
		}
	}
	// Drop all senders so that recv_async will stop
	remote_clients.clear();
	zmq_count
}

async fn run_async(mut receiver: Receiver<f32>, mut remote_clients: FnvHashMap<u64, Sender<f32>>, router: Socket) -> u64 {
	let recv_t = recv_async(receiver);
	let send_t = send_async(remote_clients, router);
	let (queue_count, zmq_count) =  join(recv_t, send_t).await;
	if queue_count != zmq_count {
		println!("WARNING: Queue not fully receiving?");
	}
	zmq_count
}


fn main() {
	let args: Vec<String> = std::env::args().collect();
	
	// ZMQ setup
	let context = zmq::Context::new();
	let router = context.socket(zmq::ROUTER).unwrap();
	assert!(router.bind("tcp://*:5555").is_ok());
	
	// Initialize Hash
	let (mut sender, mut receiver) = channel::<f32>(2000);
	let mut remote_clients = FnvHashMap::default();
	remote_clients.insert(10, sender);
	remote_clients.shrink_to_fit();
	let mut recv_counter: u64 = 0;
	
	let now = Instant::now();
	let recv_counter = match args[1].as_str() {
		"a" => {
			let mut rt = Builder::new().threaded_scheduler().core_threads(2).build().unwrap();
			let queue_count_f = rt.spawn(recv_async(receiver));
			let zmq_count = rt.block_on(send_async(remote_clients, router));

			let queue_count = block_on(queue_count_f).unwrap();
			if queue_count != zmq_count {
				println!("Warning! MPSC receiver not receiving everything?");
			}
			zmq_count
		}
		"t" => {
			run_sync(receiver, &mut remote_clients, router)
		}
		"h" => {
			run_hash_only(receiver, &mut remote_clients, router)
		}
		"d" => {
			run_deserial_only(router)
		}
		_ => { panic!("Unsupported argument!"); }
	};
	
	let time = now.elapsed();
	println!("{:?} secs, received {}", time, recv_counter);
	println!("Throughput: {}", ((recv_counter as f64) / ((time.as_nanos() as f64) / (1_000_000_000 as f64))));
	println!("{},{:?},{}", recv_counter, time, ((recv_counter as f64) / ((time.as_nanos() as f64) / (1_000_000_000 as f64))));
	
	
}
