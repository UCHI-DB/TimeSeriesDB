extern crate tokio;

use std::cmp::min;
use std::str::FromStr;
use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::file_handler::FileManager;
use crate::client::{construct_file_client_skip_newline,Amount,RunPeriod,Frequency};
use std::sync::{Arc,Mutex};
use crate::buffer_pool::{SegmentBuffer,ClockBuffer};
use crate::segment::{Segment,SegmentKey};
use std::time::SystemTime;
use std::time::{Duration,Instant};
use std::{mem, thread};
use crate::query::{Count, Max, Sum, Average};
use ndarray::Array2;
use nalgebra::Matrix2;
use crate::kernel::Kernel;
use rustfft::FFTnum;
use num::Float;
use ndarray_linalg::Lapack;
use std::ptr::null;
use std::sync::atomic::{Ordering};

use tokio::runtime::Builder; // Used for testing
use tokio::time::sleep;
use tokio::stream::Stream;
use futures::prelude::Future;
use std::pin::Pin;
use std::task::{Context, Poll}; // Added in updating tokio
use std::ops::{Deref, DerefMut};

use crate::stream_wrap::StreamWrap;

pub type SignalId = u64;
const DEFAULT_BATCH_SIZE: usize = 50;
const BACKOFF_SLEEP_MS: u64 = 100;
const MAX_SLEEP_MS: u64 = 100000;

/*
TODO
Updated to most recent version of Tokio/Future.
Needs revision regarding Unpin requirements
*/

pub struct BufferedSignalReduced<T> 
	where T: Copy + Send,
{
	seg_size: usize,
	signal_id: SignalId,
	signal: Box<StreamWrap<T> + Send>, // Relies on structural pinning
	buffer: Arc<Mutex<dyn SegmentBuffer<T> + Send + Sync>>,
	split_decider: Box<Fn(usize,usize) -> bool + Send>,
	compress_func: Box<Fn(&mut Segment<T>) + Send>,
	compress_on_segmentation: bool,
	kernel: Option<Kernel<T>>
}

impl<T> BufferedSignalReduced<T> 
	where T: Copy + Send+ FFTnum + Float + Lapack,
{

	pub fn new(signal_id: u64, signal: Box<StreamWrap<T> + Send>, seg_size: usize, 
		buffer: Arc<Mutex<dyn SegmentBuffer<T> + Send + Sync>>,
		split_decider: Box<Fn(usize,usize) -> bool + Send>, compress_func: Box<Fn(&mut Segment<T>) + Send>, 
		compress_on_segmentation: bool, dict: Option<Array2<T>>)
		-> BufferedSignalReduced<T> 
	{
		let mut kernel:Option<Kernel<T>>= match dict {
			// The division was valid
			Some(x) => {
				let mut kernel_dict = Kernel::new(x.clone(), 1, 4, DEFAULT_BATCH_SIZE);
				kernel_dict.dict_pre_process();
				Some(kernel_dict)
			},
			// The division was invalid
			None => None,
		};

		BufferedSignalReduced {
			seg_size: seg_size,
			signal_id: signal_id,
			signal: signal,
			buffer: buffer,
			split_decider: split_decider,
			compress_func: compress_func,
			compress_on_segmentation: compress_on_segmentation,
			kernel: kernel,
		}
	}
}

pub async fn run_buffered_signal<T>(mut bs: BufferedSignalReduced<T>)
		-> Option<SystemTime>
	where T: Copy + Send + Float + FFTnum + Lapack + Unpin,
{
	// Interal variables to manage state
	let mut timestamp: Option<SystemTime> = None;
	let mut prev_seg_offset: Option<SystemTime> = None;
	let mut data: Vec<T> = Vec::with_capacity(bs.seg_size);
	let mut time_lapse: Vec<Duration> = Vec::with_capacity(bs.seg_size);
	let mut compression_percentage: f64 = 0.0;
	let mut segments_produced: u32 = 0;
	let mut batch_vec: Vec<T> = Vec::new();
	let mut bsize = 0;
	
	let buffer_full = match bs.buffer.lock() {
		Ok(mut buf) => buf.get_full_status_semaphore(),
		Err(_)  => panic!("Failed to acquire buffer write lock"), // Currently panics if can't get it
	}; 

	let start = Instant::now();
	let mut timestamp = SystemTime::now();

	// Actual running
	loop {
		match bs.signal.next().await {
			None => {
				let elapse: Duration = start.elapsed();
				if bs.compress_on_segmentation {
					let percentage = compression_percentage / (segments_produced as f64);
					println!("Signal: {}\n Segments produced: {}\n Compression percentage: {}\n Time: {:?}", bs.signal_id, segments_produced, percentage, elapse);
				} else {
					//self.buffer.lock().unwrap().flush();
					println!("Signal: {}\n Segments produced: {}\n Data points in total {} \n Time: {:?}\n Throughput: {:?} points/second", bs.signal_id, (segments_produced as usize)/bs.seg_size, segments_produced, elapse, (segments_produced as f64) / ((elapse.as_nanos() as f64) / (1_000_000_000 as f64)));
					println!("{},{:?},{:?}", segments_produced, elapse, (segments_produced as f64) / ((elapse.as_nanos() as f64) / (1_000_000_000 as f64)));
				}
                match bs.buffer.lock() {
                    Ok(mut buf) => { buf.is_done(); }
                    Err(_) => ()
                }
				return prev_seg_offset;
			}
			/* Err(e) => {
				println!("The client signal produced an error: {:?}", e);
				/* Implement an error log to indicate a dropped value */
				/* Continue to run and silence the error for now */
				return Err(e);
			} */
			Some(value) => {
				let cur_time = SystemTime::now();

				/* case where the value reaches split size */
				let seg_size = bs.seg_size;
				if (bs.split_decider)(data.len(), seg_size) {
					let old_data = mem::replace(&mut data, Vec::with_capacity(seg_size));
					let old_time_lapse = mem::replace(&mut time_lapse, Vec::with_capacity(seg_size));
					let old_timestamp = mem::replace(&mut timestamp, cur_time);
					let old_prev_seg_offset = mem::replace(&mut prev_seg_offset, Some(old_timestamp));
					let dur_offset = match old_prev_seg_offset {
						Some(t) => match old_timestamp.duration_since(t) {
							Ok(d) => Some(d),
							Err(_) => panic!("Hard Failure, since messes up implicit chain"),
						}
						None => None,
					};
					//todo: adjust logics here to fix kernel method.
					if bsize<DEFAULT_BATCH_SIZE{
						//batch_vec.extend(&data);
						//bsize= bsize+1;
					}
					else {
						bsize = 0;
						let belesize = batch_vec.len();
						println!("vec for matrix length: {}", belesize);
						let mut x = Array2::from_shape_vec((DEFAULT_BATCH_SIZE,bs.seg_size),mem::replace(&mut batch_vec, Vec::with_capacity(belesize))).unwrap();
						println!("matrix shape: {} * {}", x.rows(), x.cols());
						match &bs.kernel{
							Some(kn) => kn.run(x),
							None => (),
						};
						println!("new vec for matrix length: {}", batch_vec.len());
					}

					let mut seg = Segment::new(None,old_timestamp,bs.signal_id,
											old_data, Some(old_time_lapse), dur_offset);
					
					if bs.compress_on_segmentation {
						let before = data.len() as f64;
						(bs.compress_func)(&mut seg);
						let after = data.len() as f64;
						compression_percentage += after/before;
					}


					/*
					* This part is commented out, as we use no-data-being-dropped for performance testing
					match bs.buffer.lock() {
						Ok(mut buf) => match buf.put(seg) {
							Ok(()) => (),
							Err(e) => panic!("Failed to put segment in buffer: {:?}", e),
						},
						Err(_)  => panic!("Failed to acquire buffer write lock"),
					}; /* Currently panics if can't get it */
					*/
					
					// Delete below and uncomment above if we want to drop data (i.e. allow evict_no_saving() to be called in the buf.put())
					
					loop {
                        /*
                        // This code is for exponential back off
						let mut sleep_time = BACKOFF_SLEEP_MS;
						while buffer_full.load(Ordering::Relaxed) {
							sleep(Duration::from_micros(sleep_time)).await;
							sleep_time = min(sleep_time * 2, MAX_SLEEP_MS);
						}
                        */
						match bs.buffer.lock() {
							Ok(mut buf) => {
								//if !(buffer_full.load(Ordering::Acquire)) {
                                if !(buf.is_full()) {
                                // if (true) {
									match buf.put(seg) {
										Ok(()) => (),
										Err(e) => panic!("Failed to put segment in buffer: {:?}", e),
									}
									break;
								}
							}
							Err(_)  => panic!("Failed to acquire buffer write lock"),
						}; /* Currently panics if can't get it */
					}
				}

				/* Always add the newly received data  */
				//println!("ID {} receieved {:?}", self.signal_id, value);
				data.push(value);
				segments_produced += 1;
				match cur_time.duration_since(timestamp) {
					Ok(d)  => time_lapse.push(d),
					Err(_) => time_lapse.push(Duration::default()),
				}
			}
		}	
	}
}

pub struct BufferedSignal<T,U,F,G> 
	where T: Copy + Send,
	      U: Stream<Item=T>,	
	      F: Fn(usize,usize) -> bool,
	      G: Fn(&mut Segment<T>)
{
	start: Option<Instant>,
	timestamp: Option<SystemTime>,
	prev_seg_offset: Option<SystemTime>,
	seg_size: usize,
	signal_id: SignalId,
	data: Vec<T>,
	time_lapse: Vec<Duration>,
	signal: U, // Relies on structural pinning
	buffer: Arc<Mutex<dyn SegmentBuffer<T> + Send + Sync>>,
	split_decider: F,
	compress_func: G,
	compress_on_segmentation: bool,
	compression_percentage: f64,
	segments_produced: u32,
	kernel: Option<Kernel<T>>
}

/* Fix the buffer to not require broad locking it */
impl<T,U,F,G> BufferedSignal<T,U,F,G> 
	where T: Copy + Send+ FFTnum + Float + Lapack,
		  U: Stream<Item=T>,
		  F: Fn(usize,usize) -> bool,
		  G: Fn(&mut Segment<T>)
{

	pub fn new(signal_id: u64, signal: U, seg_size: usize, 
		buffer: Arc<Mutex<dyn SegmentBuffer<T> + Send + Sync>>,
		split_decider: F, compress_func: G, 
		compress_on_segmentation: bool, dict: Option<Array2<T>>)
		-> BufferedSignal<T,U,F,G> 
	{
		let mut kernel:Option<Kernel<T>>= match dict {
			// The division was valid
			Some(x) => {
				let mut kernel_dict = Kernel::new(x.clone(), 1, 4, DEFAULT_BATCH_SIZE);
				kernel_dict.dict_pre_process();
				Some(kernel_dict)
			},
			// The division was invalid
			None    => None,
		};


		BufferedSignal {
			start: None,
			timestamp: None,
			prev_seg_offset: None,
			seg_size: seg_size,
			signal_id: signal_id,
			data: Vec::with_capacity(seg_size),
			time_lapse: Vec::with_capacity(seg_size),
			signal: signal,
			buffer: buffer,
			split_decider: split_decider,
			compress_func: compress_func,
			compress_on_segmentation: compress_on_segmentation,
			compression_percentage: 0.0,
			segments_produced: 0,
			kernel: kernel,
		}
	}
}

/* Currently just creates the segment and writes it to a buffer,
   Potential improvements:
   		1. Allow a method to be passed, that will be immediately applied to data
   		2. Allow optional collection of time data for each value
   		3. Allow a function that when passed a segment returns a boolean
   			to indicate that it should be written immediately to file or buffer pool
   		4. Allow function that determines what method to apply,
   			Like a hashmap from signal id to a method enum that should
   				be applied for that signal
   		5. Allow early return/way for user to kill a signal without 
   			having the signal neeed to exhaust the stream
 */
impl<T,U,F,G> Future for BufferedSignal<T,U,F,G> 
	where T: Copy + Send + FFTnum + Float + Lapack + Unpin,
		  U: Stream<Item=T> + Unpin,
		  F: Fn(usize,usize) -> bool + Unpin,
		  G: Fn(&mut Segment<T>) + Unpin
{
	type Output  = Option<SystemTime>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let mut batch_vec: Vec<T> = Vec::new();
		let mut bsize = 0;
		self.start = Some(Instant::now());
		loop {
			let pinned_signal = Pin::new(&mut self.signal);
			match pinned_signal.poll_next(cx) {
				Poll::Pending => { return Poll::Pending; }
				Poll::Ready(None) => {
					let elapse: Duration = self.start.unwrap().elapsed();
					if self.compress_on_segmentation {
						let percentage = self.compression_percentage / (self.segments_produced as f64);
						println!("Signal: {}\n Segments produced: {}\n Compression percentage: {}\n Time: {:?}", self.signal_id, self.segments_produced, percentage, elapse);
					} else {
						//self.buffer.lock().unwrap().flush();
						println!("Signal: {}\n Segments produced: {}\n Data points in total {} \n Time: {:?}\n Throughput: {:?} points/second", self.signal_id, (self.segments_produced as usize)/self.seg_size, self.segments_produced, elapse, (self.segments_produced as f64) / ((elapse.as_nanos() as f64) / (1_000_000_000 as f64)));
						println!("{},{:?},{:?}", self.segments_produced, elapse, (self.segments_produced as f64) / ((elapse.as_nanos() as f64) / (1_000_000_000 as f64)));
					}
					
					return Poll::Ready(self.prev_seg_offset)
				}
				/* Err(e) => {
					println!("The client signal produced an error: {:?}", e);
					/* Implement an error log to indicate a dropped value */
					/* Continue to run and silence the error for now */
					return Err(e);
				} */
				Poll::Ready(Some(value)) => {

					let cur_time    = SystemTime::now();
					if let None = self.timestamp {
						self.start = Some(Instant::now());
						self.timestamp = Some(cur_time);
					};

					/* case where the value reaches split size */
					let seg_size = self.seg_size;
					if (self.split_decider)(self.data.len(), seg_size) {
						let data = mem::replace(&mut self.data, Vec::with_capacity(seg_size));
						let time_lapse = mem::replace(&mut self.time_lapse, Vec::with_capacity(seg_size));
						let old_timestamp = mem::replace(&mut self.timestamp, Some(cur_time));
						let prev_seg_offset = mem::replace(&mut self.prev_seg_offset, old_timestamp);
						let dur_offset = match prev_seg_offset {
							Some(t) => match old_timestamp.unwrap().duration_since(t) {
								Ok(d) => Some(d),
								Err(_) => panic!("Hard Failure, since messes up implicit chain"),
							}
							None => None,
						};
						//todo: adjust logics here to fix kernel method.
						if bsize<DEFAULT_BATCH_SIZE{
							//batch_vec.extend(&data);
							//bsize= bsize+1;
						}
						else {
							bsize = 0;
							let belesize = batch_vec.len();
							println!("vec for matrix length: {}", belesize);
							let mut x = Array2::from_shape_vec((DEFAULT_BATCH_SIZE,self.seg_size),mem::replace(&mut batch_vec, Vec::with_capacity(belesize))).unwrap();
							println!("matrix shape: {} * {}", x.rows(), x.cols());
							match &self.kernel{
								Some(kn) => kn.run(x),
								None => (),
							};
							println!("new vec for matrix length: {}", batch_vec.len());
						}

						let mut seg = Segment::new(None,old_timestamp.unwrap(),self.signal_id,
											   data, Some(time_lapse), dur_offset);
						
						if self.compress_on_segmentation {
							let before = self.data.len() as f64;
							(self.compress_func)(&mut seg);
							let after = self.data.len() as f64;
							self.compression_percentage += after/before;
						}


						match self.buffer.lock() {
							Ok(mut buf) => match buf.put(seg) {
								Ok(()) => (),
								Err(e) => panic!("Failed to put segment in buffer: {:?}", e),
							},
							Err(_)  => panic!("Failed to acquire buffer write lock"),
						}; /* Currently panics if can't get it */

					}

					/* Always add the newly received data  */
					//println!("ID {} receieved {:?}", self.signal_id, value);
					self.data.push(value);
					self.segments_produced += 1;
					match cur_time.duration_since(self.timestamp.unwrap()) {
						Ok(d)  => self.time_lapse.push(d),
						Err(_) => self.time_lapse.push(Duration::default()),
					}
				}
			}	
		}
	}
}

pub struct NonStoredSignal<T,U,F,G> 
	where T: Copy + Send,
	      U: Stream<Item=T>,
	      F: Fn(usize,usize) -> bool,
	      G: Fn(&mut Segment<T>)
{
	timestamp: Option<SystemTime>,
	prev_seg_offset: Option<SystemTime>,
	seg_size: usize,
	signal_id: SignalId,
	data: Vec<T>,
	time_lapse: Vec<Duration>,
	signal: U,
	split_decider: F,
	compress_func: G,
	compress_on_segmentation: bool,
	compression_percentage: f64,
	segments_produced: u64,
}

/* Fix the buffer to not require broad locking it */
impl<T,U,F,G> NonStoredSignal<T,U,F,G> 
	where T: Copy + Send,
		  U: Stream<Item=T>,
		  F: Fn(usize,usize) -> bool,
		  G: Fn(&mut Segment<T>)
{

	pub fn new(signal_id: u64, signal: U, seg_size: usize, 
		split_decider: F, compress_func: G, compress_on_segmentation: bool) 
		-> NonStoredSignal<T,U,F,G> 
	{
		NonStoredSignal {
			timestamp: None,
			prev_seg_offset: None,
			seg_size: seg_size,
			signal_id: signal_id,
			data: Vec::with_capacity(seg_size),
			time_lapse: Vec::with_capacity(seg_size),
			signal: signal,
			split_decider: split_decider,
			compress_func: compress_func,
			compress_on_segmentation: compress_on_segmentation,
			segments_produced: 0,
			compression_percentage: 0.0,
		}
	}
}

/* Currently just creates the segment and writes it to a buffer,
   Potential improvements:
   		1. Allow a method to be passed, that will be immediately applied to data
   		2. Allow optional collection of time data for each value
   		3. Allow a function that when passed a segment returns a boolean
   			to indicate that it should be written immediately to file or buffer pool
   		4. Allow function that determines what method to apply,
   			Like a hashmap from signal id to a method enum that should
   				be applied for that signal
   		5. Allow early return/way for user to kill a signal without 
   			having the signal neeed to exhaust the stream
 */
impl<T,U,F,G> Future for NonStoredSignal<T,U,F,G> 
	where T: Copy + Send + Unpin,
		  U: Stream<Item=T> + Unpin,
		  F: Fn(usize,usize) -> bool + Unpin,
		  G: Fn(&mut Segment<T>) + Unpin
{
	type Output = Option<SystemTime>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		loop {
			let pinned_signal = unsafe { Pin::new(&mut self.signal) };
			match pinned_signal.poll_next(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => {
					if self.compress_on_segmentation {
						let percentage = self.compression_percentage / (self.segments_produced as f64);
						println!("Signal {} produced {} segments with a compression percentage of {}", self.signal_id, self.segments_produced, percentage);
					} else {
						println!("Signal {} produced {} segments", self.signal_id, (self.segments_produced as usize)/self.seg_size);
					}
					
					return Poll::Ready(self.prev_seg_offset)
				}
				/* Err(e) => {
					println!("The client signal produced an error: {:?}", e);
					/* Implement an error log to indicate a dropped value */
					/* Continue to run and silence the error for now */
				} */
				Poll::Ready(Some(value)) => {

					let cur_time    = SystemTime::now();
					if let None = self.timestamp {
						self.timestamp = Some(cur_time);
					};

					/* case where the value reaches split size */
					let seg_size = self.seg_size;
					if (self.split_decider)(self.data.len(), self.seg_size) {
						let data = mem::replace(&mut self.data, Vec::with_capacity(seg_size));
						let time_lapse = mem::replace(&mut self.time_lapse, Vec::with_capacity(seg_size));
						let old_timestamp = mem::replace(&mut self.timestamp, Some(cur_time));
						let prev_seg_offset = mem::replace(&mut self.prev_seg_offset, old_timestamp);
						let dur_offset = match prev_seg_offset {
							Some(t) => match old_timestamp.unwrap().duration_since(t) {
								Ok(d) => Some(d),
								Err(_) => panic!("Hard Failure, since messes up implicit chain"),
							}
							None => None,
						};

						let mut seg = Segment::new(None,old_timestamp.unwrap(),self.signal_id,
											   data, Some(time_lapse), dur_offset);

						if self.compress_on_segmentation {
							let before = self.data.len() as f64;
							(self.compress_func)(&mut seg);
							let after = self.data.len() as f64;
							self.compression_percentage += after/before;
						}
					}

					/* Always add the newly received data  */
					self.data.push(value);
					self.segments_produced += 1;
					match cur_time.duration_since(self.timestamp.unwrap()) {
						Ok(d)  => self.time_lapse.push(d),
						Err(_) => self.time_lapse.push(Duration::default()),
					}
				}
			}	
		}
	}
}

pub struct StoredSignal<T,U,F,G,V> 
	where T: Copy + Send + Serialize + DeserializeOwned,
	      U: Stream,
	      F: Fn(usize,usize) -> bool,
	      G: Fn(&mut Segment<T>),
	      V: AsRef<[u8]>,
{
	timestamp: Option<SystemTime>,
	prev_seg_offset: Option<SystemTime>,
	seg_size: usize,
	signal_id: SignalId,
	data: Vec<T>,
	time_lapse: Vec<Duration>,
	signal: U,
	fm: Arc<Mutex<FileManager<Vec<u8>,V> + Send + Sync>>,
	split_decider: F,
	compress_func: G,
	compress_on_segmentation: bool,
	compression_percentage: f64,
	segments_produced: u64,
}

/* Fix the buffer to not reuqire broad locking it */
impl<T,U,F,G,V> StoredSignal<T,U,F,G,V> 
	where T: Copy + Send + Serialize + DeserializeOwned,
		  U: Stream,
		  F: Fn(usize,usize) -> bool,
		  G: Fn(&mut Segment<T>),
		  V: AsRef<[u8]>,
{

	pub fn new(signal_id: u64, signal: U, seg_size: usize, 
		fm: Arc<Mutex<FileManager<Vec<u8>,V> + Send + Sync>>,
		split_decider: F, compress_func: G, 
		compress_on_segmentation: bool) 
		-> StoredSignal<T,U,F,G,V> 
	{
		StoredSignal {
			timestamp: None,
			prev_seg_offset: None,
			seg_size: seg_size,
			signal_id: signal_id,
			data: Vec::with_capacity(seg_size),
			time_lapse: Vec::with_capacity(seg_size),
			signal: signal,
			fm: fm,
			split_decider: split_decider,
			compress_func: compress_func,
			compress_on_segmentation: compress_on_segmentation,
			compression_percentage: 0.0,
			segments_produced: 0,
		}
	}
}

/* Currently just creates the segment and writes it to a buffer,
   Potential improvements:
   		1. Allow a method to be passed, that will be immediately applied to data
   		2. Allow optional collection of time data for each value
   		3. Allow a function that when passed a segment returns a boolean
   			to indicate that it should be written immediately to file or buffer pool
   		4. Allow function that determines what method to apply,
   			Like a hashmap from signal id to a method enum that should
   				be applied for that signal
   		5. Allow early return/way for user to kill a signal without 
   			having the signal neeed to exhaust the stream
 */
impl<T,U,F,G,V> Future for StoredSignal<T,U,F,G,V> 
	where T: Copy + Send + Serialize + DeserializeOwned + FromStr + Unpin,
		  U: Stream<Item=T> + Unpin,
		  F: Fn(usize,usize) -> bool + Unpin,
		  G: Fn(&mut Segment<T>) + Unpin,
		  V: AsRef<[u8]> + Unpin,
{
	type Output = Option<SystemTime>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		loop {
			//let pinned_signal = unsafe { self.map_unchecked_mut(|this| &mut this.signal) };
			let pinned_signal = Pin::new(&mut self.signal);
			match pinned_signal.poll_next(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => {
					if self.compress_on_segmentation {
						let percentage = self.compression_percentage / (self.segments_produced as f64);
						println!("Signal {} produced {} segments with a compression percentage of {}", self.signal_id, self.segments_produced, percentage);
					} else {
						println!("Signal {} produced {} segments", self.signal_id, (self.segments_produced as usize)/self.seg_size);
					}
					
					return Poll::Ready(self.prev_seg_offset)
				}
				/* Err(e) => {
					println!("The client signal produced an error: {:?}", e);
					/* Implement an error log to indicate a dropped value */
					/* Continue to run and silence the error for now */
				} */
				Poll::Ready(Some(value)) => {

					let cur_time    = SystemTime::now();
					if let None = self.timestamp {
						self.timestamp = Some(cur_time);
					};

					/* case where the value reaches split size */
					let seg_size = self.seg_size;
					if (self.split_decider)(self.data.len(), self.seg_size) {
						let data = mem::replace(&mut self.data, Vec::with_capacity(seg_size));
						let time_lapse = mem::replace(&mut self.time_lapse, Vec::with_capacity(seg_size));
						let old_timestamp = mem::replace(&mut self.timestamp, Some(cur_time));
						let prev_seg_offset = mem::replace(&mut self.prev_seg_offset, old_timestamp);
						let dur_offset = match prev_seg_offset {
							Some(t) => match old_timestamp.unwrap().duration_since(t) {
								Ok(d) => Some(d),
								Err(_) => panic!("Hard Failure, since messes up implicit chain"),
							}
							None => None,
						};

						let mut seg = Segment::new(None,old_timestamp.unwrap(),self.signal_id,
											   data, Some(time_lapse), dur_offset);
						
						if self.compress_on_segmentation {
							let before = self.data.len() as f64;
							(self.compress_func)(&mut seg);
							let after = self.data.len() as f64;
							self.compression_percentage += after/before;
						}

						match self.fm.lock() {
							Ok(fm) => {
								let key_bytes = seg.get_key().convert_to_bytes().expect("The segment key should be byte convertible");
								let seg_bytes = seg.convert_to_bytes().expect("The segment should be byte convertible");
								match fm.fm_write(key_bytes, seg_bytes) {
									Ok(()) => (),
									Err(e) => panic!("Failed to put segment in buffer: {:?}", e),
								}
							}
							Err(_)  => panic!("Failed to acquire buffer write lock"),
						}; /* Currently panics if can't get it */
					}

					/* Always add the newly received data  */
					self.data.push(value);
					self.segments_produced += 1;
					match cur_time.duration_since(self.timestamp.unwrap()) {
						Ok(d)  => self.time_lapse.push(d),
						Err(_) => self.time_lapse.push(Duration::default()),
					}
				}
			}	
		}
	}
}

#[test]
fn run_dual_signals() {
	let mut db_opts = rocksdb::Options::default();
	db_opts.create_if_missing(true);
	let fm = match rocksdb::DB::open(&db_opts, "../rocksdb") {
		Ok(x) => x,
		Err(e) => panic!("Failed to create database: {:?}", e),
	};

	let buffer: Arc<Mutex<ClockBuffer<f32,rocksdb::DB>>>  = Arc::new(Mutex::new(ClockBuffer::new(50,fm)));
	let client1 = match construct_file_client_skip_newline::<f32>(
						"../UCRArchive2018/Ham/Ham_TEST", 1, ',',
						 Amount::Unlimited, RunPeriod::Indefinite, Frequency::Immediate)
	{
		Ok(x) => x,
		Err(_) => panic!("Failed to create client1"),
	};
	let client2 = match construct_file_client_skip_newline::<f32>(
						"../UCRArchive2018/Fish/Fish_TEST", 1, ',',
						 Amount::Unlimited, RunPeriod::Indefinite, Frequency::Immediate) 
	{
		Ok(x) => x,
		Err(_) => panic!("Failed to create client2"),
	};

	let sig1 = BufferedSignalReduced::new(1, Box::new(client1), 400, buffer.clone(), Box::new(|i,j| i >= j), Box::new(|_| ()), false,None);
	let sig2 = BufferedSignalReduced::new(2, Box::new(client2), 600, buffer.clone(), Box::new(|i,j| i >= j), Box::new(|_| ()), false,None);

	let mut rt = match Builder::new_multi_thread().build() {
		Ok(rt) => rt,
		_ => panic!("Failed to build runtime"),
	};


	let result = rt.block_on(async move {
        let sig1_handle = run_buffered_signal(sig1);
        let sig2_handle = run_buffered_signal(sig2);
		join!(sig1_handle, sig2_handle)
	});

	let (seg_key1,seg_key2) = match result {
	    (Some(time1),Some(time2)) => (SegmentKey::new(time1,1),SegmentKey::new(time2,2)),
		_ => panic!("Failed to get the last system time for signal1 or signal2"),
	};
	/*
	Since we use block_on, is it necessary to shutdown at all?

	match rt.shutdown_on_idle().wait() {
		Ok(_) => (),
		Err(_) => panic!("Failed to shutdown properly"),
	} */

	let mut buf = match Arc::try_unwrap(buffer) {
		Ok(lock) => match lock.into_inner() {
			Ok(buf) => buf,
			Err(_)  => panic!("Failed to get value in lock"),
		},
		Err(_)   => panic!("Failed to get inner Arc value"),
	};



	let mut seg1: &Segment<f32> = match buf.get(seg_key1).unwrap() {
		Some(seg) => seg,
		None => panic!("Buffer lost track of the last value"),
	};


	let mut counter1 = 1;
	while let Some(key) = seg1.get_prev_key() {
		seg1 = match buf.get(key).unwrap() {
			Some(seg) => seg,
			None  => panic!(format!("Failed to get and remove segment from buffer, {}", counter1)),
		};
		counter1 += 1;
	}

	assert!(counter1 == 113 || counter1 == 135);

	let mut seg2: &Segment<f32> = match buf.get(seg_key2).unwrap() {
		Some(seg) => seg,
		None => panic!("Buffer lost track of the last value"),
	};

	let mut counter2 = 1;
	while let Some(key) = seg2.get_prev_key() {
		seg2 = match buf.get(key).unwrap() {
			Some(seg) => seg,
			None  => panic!(format!("Failed to get and remove segment from buffer, {}", counter1)),
		};
		counter2 += 1;
	}

	match counter1 {
		113 => assert!(counter2 == 135),
		135 => assert!(counter2 == 113),
		_   => panic!("Incorrect number of segments produced"),
	}

}





#[test]
fn run_single_signals() {
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    let fm = match rocksdb::DB::open(&db_opts, "../rocksdb") {
        Ok(x) => x,
        Err(e) => panic!("Failed to create database: {:?}", e),
    };

    let buffer: Arc<Mutex<ClockBuffer<f32,rocksdb::DB>>>  = Arc::new(Mutex::new(ClockBuffer::new(50,fm)));
    let client1 = match construct_file_client_skip_newline::<f32>(
        "../UCRArchive2018/Kernel/randomwalkdatasample1k-1k", 1, ',',
        Amount::Unlimited, RunPeriod::Indefinite, Frequency::Immediate)
        {
            Ok(x) => x,
            Err(_) => panic!("Failed to create client1"),
        };
    let client2 = match construct_file_client_skip_newline::<f32>(
		"../UCRArchive2018/Kernel/randomwalkdatasample1k-1k", 1, ',',
        Amount::Unlimited, RunPeriod::Indefinite, Frequency::Immediate)
        {
            Ok(x) => x,
            Err(_) => panic!("Failed to create client2"),
        };
	let start = Instant::now();
	let sig1 = BufferedSignalReduced::new(1, Box::new(client1), 1000, buffer.clone(), Box::new(|i,j| i >= j), Box::new(|_| ()), false,None);
	let sig2 = BufferedSignalReduced::new(2, Box::new(client2), 1000, buffer.clone(), Box::new(|i,j| i >= j), Box::new(|_| ()), false,None);

    let mut rt = match Builder::new_multi_thread().build() {
        Ok(rt) => rt,
        _ => panic!("Failed to build runtime"),
    };


//	let handle1 = thread::spawn( move || {
//		println!("Run ingestion demon 1" );
//		oneshot::spawn(sig1, &rt1.executor());
//	});
//
//	let handle2 = thread::spawn( move || {
//		println!("Run ingestion demon 2" );
//		oneshot::spawn(sig2, &rt2.executor());
//	});

	let result = rt.block_on(async move {
        let sig1_handle = run_buffered_signal(sig1);
        let sig2_handle = run_buffered_signal(sig2);
		tokio::join!(sig1_handle, sig2_handle)
	});
	let (seg_key1,seg_key2) = match result {
		(Some(time1),Some(time2)) => (SegmentKey::new(time1,1),SegmentKey::new(time2,2)),
		_ => panic!("Failed to get the last system time for signal1 or signal2"),
	};
	let duration = start.elapsed();
//	handle2.join().unwrap();
//	handle1.join().unwrap();
	println!("Time elapsed in ingestion function() is: {:?}", duration);
//
//	let (seg_key1) = match rt.block_on(sig1) {
//        Ok((Some(time1))) => (SegmentKey::new(time1,1)),
//        _ => panic!("Failed to get the last system time for signal1 or signal2"),
//    };



    let mut buf = match Arc::try_unwrap(buffer) {
        Ok(lock) => match lock.into_inner() {
            Ok(buf) => buf,
            Err(_)  => panic!("Failed to get value in lock"),
        },
        Err(_)   => panic!("Failed to get inner Arc value"),
    };


    /* Add query test */
//    let count = Max::run(&buf);
//    println!("total count: {}", count);

//    let mut seg1: &Segment<f32> = match buf.get(seg_key1).unwrap() {
//        Some(seg) => seg,
//        None => panic!("Buffer lost track of the last value"),
//    };
//
//
//    let mut counter1 = 1;
//    while let Some(key) = seg1.get_prev_key() {
//        seg1 = match buf.get(key).unwrap() {
//            Some(seg) => seg,
//            None  => panic!(format!("Failed to get and remove segment from buffer, {}", counter1)),
//        };
//        counter1 += 1;
//    }
//
//    assert!(counter1 == 113 || counter1 == 135);
//
//    let mut seg2: &Segment<f32> = match buf.get(seg_key2).unwrap() {
//        Some(seg) => seg,
//        None => panic!("Buffer lost track of the last value"),
//    };
//
//    let mut counter2 = 1;
//    while let Some(key) = seg2.get_prev_key() {
//        seg2 = match buf.get(key).unwrap() {
//            Some(seg) => seg,
//            None  => panic!(format!("Failed to get and remove segment from buffer, {}", counter1)),
//        };
//        counter2 += 1;
//    }
//
//    match counter1 {
//        113 => assert!(counter2 == 135),
//        135 => assert!(counter2 == 113),
//        _   => panic!("Incorrect number of segments produced"),
//    }

}

