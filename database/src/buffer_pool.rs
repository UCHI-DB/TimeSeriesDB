use std::collections::BTreeMap;
use std::fmt::Debug;
use serde::{Serialize};
use serde::de::DeserializeOwned;
use rocksdb::DBVector;
use crate::file_handler::{FileManager};
use std::collections::vec_deque::Drain;
use std::collections::hash_map::{HashMap,Entry};
use std::ops::Add;
use num::{FromPrimitive, Num, zero};

use crate::{CompressionMethod, GZipCompress, PAACompress, segment, SnappyCompress, SprintzDoubleCompress};

use segment::{Segment,SegmentKey};
use crate::compress::rrd_sample::RRDsample;
use crate::compress::split_double::SplitBDDoubleCompress;
use crate::methods::{IsLossless, Methods};

/* 
 * Overview:
 * This is the API for a buffer pool iplementation. 
 * The goal is to construct a trait for immutable buffers and then 
 * provide an extension to make mutable (element-wise) buffers. 
 * This constructs a general framework that makes future
 * implementations easier to integrate.
 *
 * Design Choice:
 * The buffer design is broken up into two parts
 * 1. A trait that must be implemented to create an immutable 
 *    (element-wise) buffer.
 * 2. A trait that can then extend the buffer to be mutable
 *    at an element-wise level.
 *
 * Current Implementations:
 * There are two basic implementations created to mainly show
 * how these traits could be implemented as well as create a 
 * structure that would make the implementation easy.
 */


/***************************************************************
 **************************Buffer_Pool**************************
 ***************************************************************/

pub trait SegmentBuffer<T: Copy + Send> {

	/* If the index is valid it will return: Ok(ref T)
	 * Otherwise, it will return: Err(()) to indicate 
	 * that the index was invalid
	 */
	fn get(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>,BufErr>;

	/* If the index is valid it will return: Ok(mut ref T)
	 * Otherwise, it will return: Err(()) to indicate 
	 * that the index was invalid
	 */
	fn get_mut(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>,BufErr>;

	/* If the buffer succeeds it will return: Ok(index)
	 * Otherwise, it will return: Err(BufErr) to indicate 
	 * that some failure occured and the item couldn't be
	 * buffered.
	 */
	fn put(&mut self, item: Segment<T>) -> Result<(), BufErr>;


	/* Will return a Drain iterator that gains ownership over
	 * the items in the buffer, but will leave the allocation
	 * of the buffer untouched
	 */
	fn drain(&mut self) -> Drain<Segment<T>>;

	/* Will copy the buffer and collect it into a vector */
	fn copy(&self) -> Vec<Segment<T>>;

	/* Will lock the buffer and write everything to disk */
	fn persist(&self) -> Result<(),BufErr>;

	/* Will empty the buffer essentially clear */
	fn flush(&mut self);

	/* Returns true if the number of items in the buffer divided by 
	 * the maximum number of items the buffer can hold exceeds
	 * the provided threshold
	 */
	fn exceed_threshold(&self, threshold: f32) -> bool;

	/* Returns true if the number of items in the buffer exceeds
	 * the provided batchsize
	 */
	fn exceed_batch(&self, batchsize: usize) -> bool;

	/* Remove the segment from the buffer and return it */
	fn remove_segment(&mut self) -> Result<Segment<T>, BufErr>;

	/* Returns true if the number of items in the buffer divided by
	 * the maximum number of items the buffer can hold belows
	 * the provided idle threshold
	 */
	fn idle_threshold(&self, threshold: f32) -> bool;

	fn get_buffer_size(&self) -> usize;

	/* Signal done*/
	fn is_done(&self)->bool;
}


/* Designed to carry error information 
 * May be unnecessary
 */
#[derive(Debug)]
pub enum BufErr {
	NonUniqueKey,
	FailedSegKeySer,
	FailedSegSer,
	FileManagerErr,
	FailPut,
	ByteConvertFail,
	GetFail,
	GetMutFail,
	EvictFailure,
	BufEmpty,
	UnderThresh,
	RemoveFailure,
	CantGrabMutex,
}


/***************************************************************
 ************************VecDeque_Buffer************************
 ***************************************************************/
/* Look into fixed vec deque or concurrent crates if poor performance */

#[derive(Debug)]
pub struct ClockBuffer<T,U> 
	where T: Copy + Send,
		  U: FileManager<Vec<u8>,DBVector> + Sync + Send,
{
	hand: usize,
	tail: usize,
	buffer: HashMap<SegmentKey,Segment<T>>,
	clock: Vec<(SegmentKey,bool)>,
	clock_map: HashMap<SegmentKey,usize>,
	file_manager: U,
	buf_size: usize,
	done: bool,
}


impl<T,U> SegmentBuffer<T> for ClockBuffer<T,U> 
	where T: Copy + Send + Serialize + DeserializeOwned + Debug,
		  U: FileManager<Vec<u8>,DBVector> + Sync + Send,
{

	fn get(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>,BufErr> {		
		if self.retrieve(key)? {
			match self.buffer.get(&key) {
				Some(seg) => Ok(Some(seg)),
				None => Err(BufErr::GetFail),
			}
		} else {
			Ok(None)
		}
	}

	fn get_mut(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>,BufErr> {
		if self.retrieve(key)? {
			match self.buffer.get_mut(&key) {
				Some(seg) => Ok(Some(seg)),
				None => Err(BufErr::GetMutFail),
			}
		} else {
			Ok(None)
		}
	}


	fn is_done(&self)->bool{
		self.done
	}

	#[inline]
	fn put(&mut self, seg: Segment<T>) -> Result<(), BufErr> {
		let seg_key = seg.get_key();
		self.put_with_key(seg_key, seg)
	}


	fn drain(&mut self) -> Drain<Segment<T>> {
		unimplemented!()
	}

	fn copy(&self) -> Vec<Segment<T>> {
		self.buffer.values().map(|x| x.clone()).collect()
	}

	/* Write to file system */
	fn persist(&self) -> Result<(),BufErr> {
		for (seg_key,seg) in self.buffer.iter() {
			let seg_key_bytes = match seg_key.convert_to_bytes() {
				Ok(bytes) => bytes,
				Err(_) => return Err(BufErr::FailedSegKeySer),
			};
			let seg_bytes = match seg.convert_to_bytes() {
				Ok(bytes) => bytes,
				Err(_) => return Err(BufErr::FailedSegSer),
			};

			match self.file_manager.fm_write(seg_key_bytes,seg_bytes) {
				Err(_) => return Err(BufErr::FileManagerErr),
				_ => (),
			}
		}

		Ok(())
	}

	fn flush(&mut self) {
		self.buffer.clear();
		self.clock.clear();
		self.done = true;
	}


	fn exceed_threshold(&self, threshold: f32) -> bool {
		//println!("{}full, threshold:{}",(self.buffer.len() as f32 / self.buf_size as f32), threshold);
		return (self.buffer.len() as f32 / self.buf_size as f32) >= threshold;
	}

	fn idle_threshold(&self, threshold: f32) -> bool {
		unimplemented!()
	}

	fn get_buffer_size(&self) -> usize{
		unimplemented!()
	}

	fn remove_segment(&mut self) -> Result<Segment<T>,BufErr> {
		let mut counter = 0;
		loop {
			if let (seg_key,false) = self.clock[self.tail] {
				let seg = match self.buffer.remove(&seg_key) {
					Some(seg) => seg,
					None => {
						//println!("Failed to get segment from buffer.");
						self.update_tail();
						return Err(BufErr::EvictFailure)
					},
				};
				match self.clock_map.remove(&seg_key) {
					None => panic!("Non-unique key panic as clock map and buffer are desynced somehow"),
					_ => (),
				}
				//println!("fetch a segment from buffer.");
				return Ok(seg);
			} else {
				self.clock[self.tail].1 = false;
			} 

			self.update_tail();
			counter += 1;
			if counter > self.clock.len() {
				return Err(BufErr::BufEmpty); 
			}
		}
	}

	fn exceed_batch(&self, batchsize: usize) -> bool {
		return self.buffer.len() >= batchsize;
	}
}


impl<T,U> ClockBuffer<T,U> 
	where T: Copy + Send + Serialize + DeserializeOwned + Debug,
		  U: FileManager<Vec<u8>,DBVector> + Sync + Send,
{
	pub fn new(buf_size: usize, file_manager: U) -> ClockBuffer<T,U> {
		ClockBuffer {
			hand: 0,
			tail: 0,
			buffer: HashMap::with_capacity(buf_size),
			clock: Vec::with_capacity(buf_size),
			clock_map: HashMap::with_capacity(buf_size),
			file_manager: file_manager,
			buf_size: buf_size,
			done: false,
		}
	}

	/* Assumes that the segment is in memory and will panic otherwise */
	#[inline]
	fn update(&mut self, key: SegmentKey) {
		let key_idx: usize = *self.clock_map.get(&key).unwrap();
		self.clock[key_idx].1 = false;
	}

	#[inline]
	fn update_hand(&mut self) {
		self.hand = (self.hand + 1) % self.buf_size;
	}

	#[inline]
	fn update_tail(&mut self) {
		self.tail = (self.tail + 1) % self.clock.len();
	}

	fn put_with_key(&mut self, key: SegmentKey, seg: Segment<T>) -> Result<(), BufErr> {
		let slot = if self.buffer.len() >= self.buf_size {
			let slot = self.evict_no_saving()?;
			self.clock[slot] = (key,true);
			slot
		} else {
			let slot = self.hand;
			self.clock.push((key,true));
			self.update_hand();
			slot
		};

		
		match self.clock_map.insert(key,slot) {
			None => (),
			_ => return Err(BufErr::NonUniqueKey),
		}
		match self.buffer.entry(key) {
			Entry::Occupied(_) => panic!("Non-unique key panic as clock map and buffer are desynced somehow"),
			Entry::Vacant(vacancy) => {
				vacancy.insert(seg);
				Ok(())
			}
		}
	}

	/* Gets the segment from the filemanager and places it in */
	fn retrieve(&mut self, key: SegmentKey) -> Result<bool,BufErr> {
		if let Some(_) = self.buffer.get(&key) {
			println!("reading from the buffer");
			self.update(key);
			return Ok(true);
		}
		println!("reading from the file_manager");
		match key.convert_to_bytes() {
			Ok(key_bytes) => {
				match self.file_manager.fm_get(key_bytes) {
					Err(_) => Err(BufErr::FileManagerErr),
					Ok(None) => Ok(false),
					Ok(Some(bytes)) => {
						match Segment::convert_from_bytes(&bytes) {
							Ok(seg) => {
								self.put_with_key(key,seg)?;
								Ok(true)
							}
							Err(()) => Err(BufErr::ByteConvertFail),
						}
					}
				}				
			}
			Err(_) => Err(BufErr::ByteConvertFail)
		}
	}

	fn evict(&mut self) -> Result<usize,BufErr> {
		loop {
			if let (seg_key,false) = self.clock[self.hand] {
				let seg = match self.buffer.remove(&seg_key) {
					Some(seg) => seg,
					None => return Err(BufErr::EvictFailure),
				};
				match self.clock_map.remove(&seg_key) {
					None => panic!("Non-unique key panic as clock map and buffer are desynced somehow"),
					_ => (),
				}

				/* Write the segment to disk */
				let seg_key_bytes = match seg_key.convert_to_bytes() {
					Ok(bytes) => bytes,
					Err(()) => return Err(BufErr::FailedSegKeySer)
				};
				let seg_bytes = match seg.convert_to_bytes() {
					Ok(bytes) => bytes,
					Err(()) => return Err(BufErr::FailedSegSer),
				};

				match self.file_manager.fm_write(seg_key_bytes,seg_bytes) {
					Ok(()) => {
						self.tail = self.hand + 1;
						return Ok(self.hand)
					}
					Err(_) => return Err(BufErr::FileManagerErr),
				}

			} else {
				self.clock[self.hand].1 = false;
			} 

			self.update_hand();
		}
	}

	fn evict_no_saving(&mut self) -> Result<usize,BufErr> {
		loop {
			if let (seg_key,false) = self.clock[self.hand] {
				self.buffer.remove(&seg_key);
				self.clock_map.remove(&seg_key);
				return Ok(self.hand)
			} else {
				self.clock[self.hand].1 = false;
			}

			self.update_hand();
		}
	}
}


#[derive(Debug)]
pub struct NoFmClockBuffer<T> 
	where T: Copy + Send,
{
	hand: usize,
	tail: usize,
	buffer: HashMap<SegmentKey,Segment<T>>,
	clock: Vec<(SegmentKey,bool)>,
	clock_map: HashMap<SegmentKey,usize>,
	buf_size: usize,
	done: bool,
}


impl<T> SegmentBuffer<T> for NoFmClockBuffer<T> 
	where T: Copy + Send + Debug,
{

	fn get(&mut self, _key: SegmentKey) -> Result<Option<&Segment<T>>,BufErr> {		
		unimplemented!()
	}

	fn get_mut(&mut self, _key: SegmentKey) -> Result<Option<&Segment<T>>,BufErr> {
		unimplemented!()
	}

	fn is_done(&self)->bool{
		self.done
	}

	#[inline]
	fn put(&mut self, seg: Segment<T>) -> Result<(), BufErr> {
		let seg_key = seg.get_key();
		self.put_with_key(seg_key, seg)
	}


	fn drain(&mut self) -> Drain<Segment<T>> {
		unimplemented!()
	}

	fn copy(&self) -> Vec<Segment<T>> {
		self.buffer.values().map(|x| x.clone()).collect()
	}

	/* Write to file system */
	fn persist(&self) -> Result<(),BufErr> {
		unimplemented!()
	}

	fn flush(&mut self) {
		self.buffer.clear();
		self.clock.clear();
		self.done = true;
	}

	fn exceed_threshold(&self, threshold: f32) -> bool {
		return (self.buffer.len() as f32 / self.buf_size as f32) > threshold;
	}

	fn idle_threshold(&self, threshold: f32) -> bool {
		unimplemented!()
	}

	fn get_buffer_size(&self) -> usize{
		unimplemented!()
	}


	fn remove_segment(&mut self) -> Result<Segment<T>,BufErr> {
		let mut counter = 0;
		loop {
			if let (seg_key,false) = self.clock[self.hand] {
				let seg = match self.buffer.remove(&seg_key) {
					Some(seg) => seg,
					None => return Err(BufErr::EvictFailure),
				};
				match self.clock_map.remove(&seg_key) {
					None => panic!("Non-unique key panic as clock map and buffer are desynced somehow"),
					_ => (),
				}

				return Ok(seg);
			} else {
				self.clock[self.hand].1 = false;
			} 

			self.update_hand();
			counter += 1;
			if counter >= self.clock.len() {
				return Err(BufErr::BufEmpty);
			}
		}
	}

	fn exceed_batch(&self, batchsize: usize) -> bool {
		return self.buffer.len() >= batchsize;
	}
}


impl<T> NoFmClockBuffer<T> 
	where T: Copy + Send + Debug,
{
	pub fn new(buf_size: usize) -> NoFmClockBuffer<T> {
		NoFmClockBuffer {
			hand: 0,
			tail: 0,
			buffer: HashMap::with_capacity(buf_size),
			clock: Vec::with_capacity(buf_size),
			clock_map: HashMap::with_capacity(buf_size),
			buf_size: buf_size,
			done:false,
		}
	}

	/* Assumes that the segment is in memory and will panic otherwise */
	#[inline]
	fn update(&mut self, key: SegmentKey) {
		let key_idx: usize = *self.clock_map.get(&key).unwrap();
		self.clock[key_idx].1 = false;
	}

	#[inline]
	fn update_hand(&mut self) {
		self.hand = (self.hand + 1) % self.buf_size;
	}

	fn put_with_key(&mut self, key: SegmentKey, seg: Segment<T>) -> Result<(), BufErr> {
		let slot = if self.buffer.len() >= self.buf_size {
			let slot = self.evict()?;
			self.clock[slot] = (key,true);
			slot
		} else {
			let slot = self.hand;
			self.clock.push((key,true));
			self.update_hand();
			slot
		};

		
		match self.clock_map.insert(key,slot) {
			None => (),
			_ => return Err(BufErr::NonUniqueKey),
		}
		match self.buffer.entry(key) {
			Entry::Occupied(_) => panic!("Non-unique key panic as clock map and buffer are desynced somehow"),
			Entry::Vacant(vacancy) => {
				vacancy.insert(seg);
				Ok(())
			}
		}
	}

	fn evict(&mut self) -> Result<usize,BufErr> {
		loop {
			if let (seg_key,false) = self.clock[self.hand] {
				let _seg = match self.buffer.remove(&seg_key) {
					Some(seg) => seg,
					None => return Err(BufErr::EvictFailure),
				};
				match self.clock_map.remove(&seg_key) {
					None => panic!("Non-unique key panic as clock map and buffer are desynced somehow"),
					_ => (),
				}
			} else {
				self.clock[self.hand].1 = false;
			} 

			self.update_hand();
		}
	}
}

/***************************************************************
 ************************LRUcomp_Buffer************************
 ***************************************************************/
/* LRU based compression with limited space budget.
Use double linked list to achieve O(1) get and put time complexity*/

#[derive(Debug)]
pub struct LRUBuffer<T,U>
	where T: Copy + Send,
		  U: FileManager<Vec<u8>,DBVector> + Sync + Send,
{
	budget: usize,
	cur_size: usize,
	head: Option<SegmentKey>,
	tail: Option<SegmentKey>,
	buffer: BTreeMap<SegmentKey,Node<T>>,
	agg_stats: BTreeMap<SegmentKey,AggStats<T>>,
	file_manager: U,
	buf_size: usize,
	done: bool,
}

#[derive(Clone, Debug)]
struct Node<T>
	where T: Copy + Send,
{
	value: Segment<T>,
	next: Option<SegmentKey>,
	prev: Option<SegmentKey>,
}


#[derive(Debug)]
struct AggStats<T>
	where T: Copy + Send,{
	max: T,
	min: T,
	sum: T,
	count: usize
}

impl<T> AggStats<T> where T: Copy + Send, {
	pub fn new(max: T, min: T, sum: T, count: usize) -> Self {
		Self { max, min, sum, count }
	}
}

fn Get_AggStats<T:Num + FromPrimitive+ Copy + Send + Into<f64>+ PartialOrd+ Add<T, Output = T>> (seg: &Segment<T>) -> AggStats<T>{
	let mut vec = Vec::new();
	let size = seg.get_size();
	match seg.get_method().as_ref().unwrap() {
		Methods::Sprintz (scale) => {
			let pre = SprintzDoubleCompress::new(10, 20,*scale);
			vec= pre.decode_general(seg.get_comp());
		},
		Methods::Buff(scale)=> {
			let pre = SplitBDDoubleCompress::new(10,20, *scale);
			vec= pre.decode_general(seg.get_comp());

		},
		Methods::Snappy=> {
			let pre = SnappyCompress::new(10,20);
			vec = pre.decode(seg.get_comp());
		},
		Methods::Gzip => {
			let pre = GZipCompress::new(10,20);
			vec = pre.decode(seg.get_comp());
		},
		Methods::Paa(wsize) => {
			let cur = PAACompress::new(*wsize, 20);
			vec = cur.decodeVec(seg.get_data());
			vec.truncate(size);
		},
		Methods::Rrd_sample => {
			let cur = RRDsample::new(20);
			vec = cur.decode(seg);
		},
		_ => todo!()
	}

	let &min = vec.iter().fold(None, |min, x| match min {
		None => Some(x),
		Some(y) => Some(if x > y { y } else { x }),
	}).unwrap();
	let &max = vec.iter().fold(None, |max, x| match max {
		None => Some(x),
		Some(y) => Some(if x > y { x } else { y }),
	}).unwrap();

	let sum = vec.iter().fold(T::zero(), |sum, &i| sum + i);
	let count = vec.len();
	return AggStats::new(max,min,sum,count);
}


impl<T,U> SegmentBuffer<T> for LRUBuffer<T,U>
	where T: Copy + Send + Serialize + DeserializeOwned + Debug+Num + FromPrimitive + PartialOrd + Into<f64>,
		  U: FileManager<Vec<u8>,DBVector> + Sync + Send,
{

	fn get(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>,BufErr> {
		if self.retrieve(key)? {
			match self.buffer.get(&key) {
				Some(seg) => Ok(Some(&seg.value)),
				None => Err(BufErr::GetFail),
			}
		} else {
			Ok(None)
		}
	}

	fn get_mut(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>,BufErr> {
		if self.retrieve(key)? {
			match self.buffer.get_mut(&key) {
				Some(seg) => Ok(Some(&seg.value)),
				None => Err(BufErr::GetMutFail),
			}
		} else {
			Ok(None)
		}
	}


	fn is_done(&self)->bool{
		self.done
	}

	#[inline]
	fn put(&mut self, seg: Segment<T>) -> Result<(), BufErr> {
		let seg_key = seg.get_key();
		self.put_with_key(seg_key, seg)
	}


	fn drain(&mut self) -> Drain<Segment<T>> {
		unimplemented!()
	}

	fn copy(&self) -> Vec<Segment<T>> {
		self.buffer.values().map(|x| x.value.clone()).collect()
	}

	/* Write to file system */
	fn persist(&self) -> Result<(),BufErr> {
		for (seg_key,seg) in self.buffer.iter() {
			let seg_key_bytes = match seg_key.convert_to_bytes() {
				Ok(bytes) => bytes,
				Err(_) => return Err(BufErr::FailedSegKeySer),
			};
			let seg_bytes = match seg.value.convert_to_bytes() {
				Ok(bytes) => bytes,
				Err(_) => return Err(BufErr::FailedSegSer),
			};

			match self.file_manager.fm_write(seg_key_bytes,seg_bytes) {
				Err(_) => return Err(BufErr::FileManagerErr),
				_ => (),
			}
		}

		Ok(())
	}

	fn flush(&mut self) {
		self.buffer.clear();
		self.done = true;
	}


	fn exceed_threshold(&self, threshold: f32) -> bool {
		// println!("{}%full, threshold:{}",(self.cur_size as f32 / self.budget as f32 as f32), threshold);
		return (self.cur_size as f32 / self.budget as f32) >= threshold;
	}

	fn get_buffer_size(&self) -> usize{
		self.cur_size
	}


	fn remove_segment(&mut self) -> Result<Segment<T>,BufErr> {
		match self.head {
			None => {
				return Err(BufErr::BufEmpty)
			},
			Some(head) => {
				match self.get(head){
					Some(v) => {
						return Ok(v);
					}
					None => {
						return Err(BufErr::GetFail)
					}
				}
			},
		}
	}

	fn idle_threshold(&self, threshold: f32) -> bool {
		//println!("{}full, threshold:{}",(self.buffer.len() as f32 / self.buf_size as f32), threshold);
		return (self.cur_size as f32 / self.budget as f32) < threshold;
	}

	fn exceed_batch(&self, batchsize: usize) -> bool {
		return self.buffer.len() >= batchsize;
	}
}


impl<T,U> LRUBuffer<T,U>
	where T: Copy + Send + Serialize + DeserializeOwned + Debug + Num + FromPrimitive + PartialOrd + Into<f64>,
		  U: FileManager<Vec<u8>,DBVector> + Sync + Send,
{
	pub fn new(budget: usize, file_manager: U) -> LRUBuffer<T,U> {
		println!("created LRU comp buffer with budget {} bytes", budget);
		LRUBuffer {
			budget: budget,
			cur_size: 0,
			head: None,
			tail: None,
			buffer: BTreeMap::new(),
			agg_stats: BTreeMap::new(),
			file_manager: file_manager,
			buf_size: 0,
			done: false,
		}

	}

	fn push_back_node(&mut self, key: SegmentKey, value: Segment<T>) {
		let size = value.get_byte_size().unwrap();
		// update aggstats for query accuracy profiling
		if IsLossless(value.get_method().as_ref().unwrap()){
			println!("buffer size: {}, agg stats size: {}",self.buffer.len(), self.agg_stats.len() );
			self.agg_stats.insert(key, Get_AggStats(&value));
		}

		let mut node = Node {
			next: None,
			prev: self.tail,
			value,
		};
		match self.tail {
			None => self.head = Some(key),
			Some(tail) => self.buffer.get_mut(&tail).unwrap().next = Some(key),
		}
		self.tail = Some(key);
		self.buffer.insert(key, node);
		self.cur_size += size;
		self.buf_size += 1;
	}

	fn pop_front_node(&mut self) {
		self.head.map(|key| {
			let c = self.buffer.get(&key);
			let size = c.unwrap().value.get_byte_size().unwrap();
			self.head = c.unwrap().next;
			match self.head {
				None => {
					self.tail = None;
				}
				Some(head) => self.buffer.get_mut(&head).unwrap().prev = None,
			}
			self.cur_size -= size;
			self.buf_size -= 1;
			self.buffer.remove(&key);
			key
		});
	}


	fn unlink_node(&mut self, key: SegmentKey, c: &Node<T>) {
		match c.prev {
			Some(prev) => {
				let prev_cache = self.buffer.get_mut(&prev).unwrap();
				prev_cache.next = c.next;
			}
			None => self.head = c.next,
		};
		match c.next {
			Some(next) => {
				let next_cache = self.buffer.get_mut(&next).unwrap();
				next_cache.prev = c.prev;
			}
			None => self.tail = c.prev,
		};
		self.cur_size -= self.buffer.get(&key).unwrap().value.get_byte_size().unwrap();
		self.buf_size -= 1;
		self.buffer.remove(&key);

	}
	pub fn get(&mut self, key: SegmentKey) -> Option<Segment<T>> {
		match self.buffer.entry(key) {
			std::collections::btree_map::Entry::Occupied(mut x) => {

				let c = x.get().clone();
				self.unlink_node(key, &c);
				self.push_back_node(key, c.value.clone());
				Some(c.value)
			}
			std::collections::btree_map::Entry::Vacant(_) => None,
		}
	}

	pub fn put(&mut self, key: SegmentKey, value: Segment<T>) {
		match self.get(key) {
			Some(v) => {
				self.cur_size -= v.get_byte_size().unwrap();
				let size = value.get_byte_size().unwrap();
				self.buffer.get_mut(&key).unwrap().value = value;
				self.cur_size += size;
				return;
			}
			None => {
			}

		}
		self.push_back_node(key, value);
	}


	fn put_with_key(&mut self, key: SegmentKey, seg: Segment<T>) -> Result<(), BufErr> {

		match self.get(key) {
			Some(v) => {
				self.cur_size -= v.get_byte_size().unwrap();
				let size = &seg.get_byte_size().unwrap();
				self.buffer.get_mut(&key).unwrap().value = seg;
				self.cur_size += size;
				return Ok(());
			}
			None => {
			}

		}
		self.push_back_node(key, seg);
		Ok(())
	}

	/* Gets the segment from the filemanager and places it in */
	fn retrieve(&mut self, key: SegmentKey) -> Result<bool,BufErr> {
		if let Some(_) = self.get(key) {
			println!("reading from the buffer");
			return Ok(true);
		}
		println!("reading from the file_manager");
		match key.convert_to_bytes() {
			Ok(key_bytes) => {
				match self.file_manager.fm_get(key_bytes) {
					Err(_) => Err(BufErr::FileManagerErr),
					Ok(None) => Ok(false),
					Ok(Some(bytes)) => {
						match Segment::convert_from_bytes(&bytes) {
							Ok(seg) => {
								self.put_with_key(key,seg)?;
								Ok(true)
							}
							Err(()) => Err(BufErr::ByteConvertFail),
						}
					}
				}
			}
			Err(_) => Err(BufErr::ByteConvertFail)
		}
	}


}
