use rocksdb::DBVector;
use crate::buffer_pool::BufErr;
use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::segment::Segment;
use std::sync::{Arc,Mutex};
use crate::buffer_pool::{SegmentBuffer,ClockBuffer};
use crate::file_handler::{FileManager};
use crate::{file_handler, GorillaCompress, GZipCompress, SnappyCompress, SprintzDoubleCompress};
use crate::methods::compress::CompressionMethod;
use std::any::Any;
use std::time::{SystemTime, UNIX_EPOCH};
use num::FromPrimitive;
use rl_bandit::bandit::{Bandit, UpdateType};
use rl_bandit::bandits::egreedy::EGreedy;
use crate::buffer_pool::BufErr::BufEmpty;
use crate::compress::split_double::SplitBDDoubleCompress;

pub struct MABCompressionDaemon<T,U,F>
	where T: Copy + Send + Serialize + DeserializeOwned+FromPrimitive+Into<f64>,
	      U: FileManager<Vec<u8>,DBVector> + Sync + Send,
		  F: CompressionMethod<T>
{
	seg_buf: Arc<Mutex<SegmentBuffer<T> + Send + Sync>>,
	comp_seg_buf: Arc<Mutex<SegmentBuffer<T> + Send + Sync>>,
	file_manager: Option<U>,
	comp_threshold: f32,
	uncomp_threshold: f32,
	processed: usize,
	compress_method: F,
	mab: EGreedy
}


impl<T,U,F> MABCompressionDaemon<T,U,F>
	where T: Copy + Send + Serialize + DeserializeOwned+FromPrimitive+Into<f64>,
		  U: FileManager<Vec<u8>,DBVector> + Sync + Send,
		  F: CompressionMethod<T>
{
	pub fn new(seg_buf: Arc<Mutex<SegmentBuffer<T> + Send + Sync>>,
			   comp_seg_buf: Arc<Mutex<SegmentBuffer<T> + Send + Sync>>,
			   file_manager: Option<U>,
			   comp_threshold: f32, uncomp_threshold: f32, compress_method: F)
			   -> MABCompressionDaemon<T,U,F>
	{
		MABCompressionDaemon {
			seg_buf: seg_buf,
			comp_seg_buf: comp_seg_buf,
			file_manager: file_manager,
			comp_threshold: comp_threshold,
			uncomp_threshold: uncomp_threshold,
			processed: 0,
			compress_method: compress_method,
			mab: EGreedy::new(5, 0.1, 0.0, UpdateType::Average)
		}
	}

	fn get_seg_from_uncomp_buf(&self) -> Result<Vec<Segment<T>>,BufErr>
	{
		match self.seg_buf.lock() {
			Ok(mut buf) => {
				//println!("Lock aquired");
				if buf.exceed_threshold(self.uncomp_threshold) && buf.exceed_batch(self.compress_method.get_batch()) {
					let batch_size = self.compress_method.get_batch();
					//println!("Get segment for compression batch size:{}",batch_size);
					let mut segs = Vec::with_capacity(batch_size);
					loop {
						match buf.remove_segment(){
							Ok(seg) => {
								segs.push(seg);
								if segs.len() == batch_size{
									break;
								}
							},
							Err(_) => continue,
						}
					}
					Ok(segs)
				} else {
					Err(BufErr::UnderThresh)
				}
			}
			Err(_) => Err(BufErr::RemoveFailure)
		}

	}

	fn is_done_buf(&self) -> bool{
		self.seg_buf.lock().unwrap().is_done()
	}

	fn put_seg_in_comp_buf(&self, seg: Segment<T>) -> Result<(),BufErr>
	{
		match self.comp_seg_buf.lock() {
			Ok(mut buf) => buf.put(seg),
			Err(_) => Err(BufErr::CantGrabMutex),
		}
	}

	fn put_segs_in_comp_buf(&self, segs: Vec<Segment<T>>) -> Result<(),BufErr>
	{
		match self.comp_seg_buf.lock() {
			Ok(mut buf) => {
				for seg in segs{
					match buf.put(seg) {
						Ok(_) => continue,
						Err(_) => return Err(BufErr::FailPut),
					}
				}
				println!("buffer total bytes: {},{}",SystemTime::now().duration_since(UNIX_EPOCH)
					.unwrap().as_micros(),buf.get_buffer_size());
				return Ok(());
			}
			Err(_) => Err(BufErr::CantGrabMutex),
		}
	}

	pub fn get_processed(&self) -> usize{
		self.processed
	}

	pub fn update_mab(&mut self, segs: &Vec<Segment<T>>, arm: usize){
		for seg in segs{
			// hardcode the original file size as 80000 bytes
			self.mab.update(arm, 80000.0/seg.get_byte_size().unwrap() as f64);
		}
	}

	pub fn run(&mut self)
	{
		loop {
			match self.get_seg_from_uncomp_buf() {
				Ok(mut segs) => {
					self.processed = self.processed + segs.len();
					println!("segment compressed {}", self.processed);
					match &self.file_manager {
						Some(fm) => {
							for mut seg in segs{
								let key_bytes = match seg.get_key().convert_to_bytes() {
									Ok(bytes) => bytes,
									Err(_) => continue, /* silence failure to byte convert */
								};
								let seg_bytes = match seg.convert_to_bytes() {
									Ok(bytes) => bytes,
									Err(_) => continue, /* silence failure to byte convert */
								};
								match fm.fm_write(key_bytes, seg_bytes) {
									Ok(()) => continue,
									Err(_) => continue, /* currently silence error from fialed write */
								}
							}
						}
						None => {
							let action:usize = self.mab.choose();
							let batch = self.compress_method.get_batch();
							println!("MAB best arm: {}", action);
							match action {
								0 => { let ccomp = GorillaCompress::new(10, batch);
									ccomp.run_compress(&mut segs); },
								1 => { let ccomp = GZipCompress::new(10, batch);
									ccomp.run_compress(&mut segs);},
								2 => { let ccomp =  SnappyCompress::new(10, batch);
									ccomp.run_compress(&mut segs);},
								3 => { let ccomp = SprintzDoubleCompress::new(10, batch, 10000);
									ccomp.run_compress(&mut segs);},
								4 => { let ccomp = SplitBDDoubleCompress::new(10, batch, 10000);
									ccomp.run_compress(&mut segs);},
								_ => panic!("Compression index greater than 4 is not supported yet."),
							}
							self.update_mab(&segs, action);
							match self.put_segs_in_comp_buf(segs) {
								Ok(()) => continue,
								Err(_) => continue, /* Silence the failure to put the segment in */
							}
						}
					}
				},
				Err(BufErr::BufEmpty) => {
					println!("Buffer is empty. Compression process exit.");
					break
				},
				Err(e)=>{
					//println!("No segment is fetched, try again.");
					//println!("Application error: {:?}",e);
					continue
				},
			}
		}
	}

}