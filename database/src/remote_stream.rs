use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::Mutex;
use std::time::Instant;
use tokio::prelude::*;
use std::fmt::Debug;

use crate::client::*;

/* DEPRECATED */

pub fn remote_stream_from_receiver<T>(queue: Receiver<T>, amount: Amount, run_period: RunPeriod)
                -> RemoteStream<T>
{
	let produced = match amount {
		Amount::Limited(_) => Some(0),
		Amount::Unlimited  => None,
	};

	RemoteStream { 
		queue: Mutex::new(queue),
		//queue: queue,
		amount: amount,
		run_period: run_period,
		start: Instant::now(),
		produced: produced,
	}
}

/*
 * TODO
 * 1) Useless Mutex. Right now mutex is necessary because MPSC queue
 * 		doesn't allow Receiver to be shared by threads.
 * 		One solution is to implement a separate SPSC queue with Sync trait
 * 		Question would be how to implement non-blocking stuff in Rust (because simultaneous access is forbidden?)?
 */
pub struct RemoteStream<T> 
{
	queue: Mutex<Receiver<T>>,
	amount: Amount,
	run_period: RunPeriod,
	start: Instant,
	produced: Option<u64>,
}

/* Max time limit/amount handling, almost the same as client.rs
 * Currently the CLIENT (the futures with queues) handles when to exit
 * and the main function is expected to tell dispatcher to exit */
impl<T> Stream for RemoteStream<T> where T: Debug
{
	type Item = T;
	type Error = ();

	fn poll(&mut self) -> Poll<Option<T>,()> {
		/* Terminate stream if hit time-limit */
		if let RunPeriod::Finite(dur) = self.run_period {
			let now = Instant::now();
			let time = now.duration_since(self.start); 
			if time >= dur { return Ok(Async::Ready(None)) }
		}

		/* Terminate stream if hit max production */
		// TODO remove
		if let Amount::Limited(max_items) = self.amount {
			if let Some(items) = self.produced {
				if items >= max_items { return Ok(Async::Ready(None)) }
			}
		}

		/* Either poll to determine if enough time has passed or
		 * immediately get the value depending on Frequency Mode
		 * Must call poll on the stream within the client
		 */
		
		
		match self.queue.try_lock() {
			// It should NEVER block, because no other thread should own the lock
			Ok(queue) => {
				match (*queue).try_recv() {
					Ok(item) => { 
						Ok(Async::Ready(Some(item)))
					},
					Err(TryRecvError::Empty) => {
						Ok(Async::NotReady)
					},
					Err(e) => { 
						println!("{:?}", e); 
						Err(())
					}
				}
			}
			Err(_e) => {
				println!("Unable to get lock for receiver");
				Ok(Async::NotReady)
			}
		}
		/* match (self.queue).try_recv() {
			Ok(item) => { 
				println!("Has item! {:?}", item);
				Ok(Poll::Ready(Some(item)))
			},
			Err(TryRecvError::Empty) => {
				Ok(Poll::Pending)
			},
			Err(e) => { 
				println!("{:?}", e); 
				Err(())
			}
		} */

	}
}