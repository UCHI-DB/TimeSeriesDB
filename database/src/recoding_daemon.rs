use rocksdb::DBVector;
use crate::buffer_pool::{BufErr};
use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::segment::{Segment, SegmentKey};
use std::sync::{Arc,Mutex};
use crate::buffer_pool::{SegmentBuffer,ClockBuffer};
use crate::file_handler::{FileManager};
use crate::{file_handler, GZipCompress, ZlibCompress, DeflateCompress, SnappyCompress, PAACompress, FourierCompress};
use crate::methods::compress::CompressionMethod;
use std::any::Any;
use std::{fs, thread};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use num::{FromPrimitive, Num};
use rand::Rng;
use rustfft::FFTnum;
use smartcore::api::Predictor;
use smartcore::cluster::kmeans::KMeans;
use smartcore::linalg::Matrix;
use smartcore::linalg::naive::dense_matrix::DenseMatrix;
use smartcore::math::num::RealNumber;
use crate::buffer_pool::BufErr::BufEmpty;
use crate::compress::buff_lossy::BUFFlossy;
use crate::methods::Methods;
use crate::compress::split_double::SplitBDDoubleCompress;
use crate::compress::sprintz::SprintzDoubleCompress;
use crate::compress::gorilla::{GorillaBDCompress, GorillaCompress};
use crate::compress::pla_lttb::PLACompress;
use crate::compress::rrd_sample::RRDsample;

pub fn GetMatrix<T:RealNumber> (seg: &Segment<T>) -> DenseMatrix<T>{
	let x = DenseMatrix::from_array(
		seg.get_size()/1000,
		1000,
		seg.get_data(),
	);
	x
}




pub struct RecodingDaemon<T,U>
	where T: Copy + Send + Serialize + DeserializeOwned + RealNumber,
	      U: FileManager<Vec<u8>,DBVector> + Sync + Send,
{
	seg_buf: Arc<Mutex<SegmentBuffer<T> + Send + Sync>>,
	comp_seg_buf: Arc<Mutex<SegmentBuffer<T> + Send + Sync>>,
	file_manager: Option<U>,
	comp_threshold: f32,
	uncomp_threshold: f32,
	processed: usize,
	batch: usize,
	lossy: Methods
}

impl<T,U> RecodingDaemon<T,U>
	where T: 'static + Copy + Send + Serialize + DeserializeOwned + FromPrimitive + Num+ FFTnum+Into<f64> + RealNumber,
		  U: FileManager<Vec<u8>,DBVector> + Sync + Send,
{
	pub fn new(seg_buf: Arc<Mutex<SegmentBuffer<T> + Send + Sync>>,
			   comp_seg_buf: Arc<Mutex<SegmentBuffer<T> + Send + Sync>>,
			   file_manager: Option<U>,
			   comp_threshold: f32,
			   uncomp_threshold: f32,
			   batch: usize,
			   lossy: Methods)
			   -> RecodingDaemon<T,U>
	{

		RecodingDaemon {
			seg_buf: seg_buf,
			comp_seg_buf: comp_seg_buf,
			file_manager: file_manager,
			comp_threshold: comp_threshold,
			uncomp_threshold: uncomp_threshold,
			processed: 0,
			batch: batch,
			lossy: lossy
		}
	}

	fn get_seg_from_uncomp_buf(&self) -> Result<Vec<Segment<T>>,BufErr>
	{
		match self.seg_buf.lock() {
			Ok(mut buf) => {
				//println!("Lock aquired");
				if buf.exceed_threshold(self.uncomp_threshold) {
					println!("Get segment for compression batch size:{}",self.batch);
					let mut segs = Vec::with_capacity(self.batch);
					loop {
						match buf.remove_segment(){
							Ok(seg) => {
								segs.push(seg);
								if segs.len() == self.batch {
									break;
								}
							},
							Err(_) => continue,
						}
					}

					Ok(segs)
				} else {
					// add random number check to avoid frequent query check
					let mut rng = rand::thread_rng();
					let n = rng.gen_range(0, 5);
					if n==1{
						buf.run_query();
					}
					Err(BufErr::UnderThresh)
				}
			}
			Err(_) => Err(BufErr::RemoveFailure)
		}

	}

	fn is_done_buf(&self) -> bool{
		self.seg_buf.lock().unwrap().is_done()
	}

	fn lossy_comp (&self, uncomp_seg: &mut Segment<T>) {
		match self.lossy {
			Methods::Paa(scale) => {
				let cur = PAACompress::new(4, self.batch);
				cur.run_single_compress(uncomp_seg);
			}
			Methods::Rrd_sample => {
				let cur = RRDsample::new(self.batch);
				cur.run_single_compress(uncomp_seg);
			}
			Methods::Bufflossy(_,_) => {
				// this is the dummy bits parameter
				let cur = BUFFlossy::new(self.batch,10000, 32);
				cur.run_single_compress(uncomp_seg);
			}
			Methods::Pla(ratio) => {
				let cur = PLACompress::new(self.batch,0.25);
				cur.run_single_compress(uncomp_seg);
			}
			Methods::Fourier(ratio) => {
				let cur = FourierCompress::new(4,self.batch,0.25);
				cur.run_single_compress(uncomp_seg);
				// let res =  cur.decodeVec(uncomp_seg.get_data(),uncomp_seg.get_size());
				// println!("segment size: {}, compress timm:{}, first segment: {:?}", uncomp_seg.get_data().len(),uncomp_seg.get_comp_times(),&res[..10]);

			}
			_ => {}
		}
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

	pub fn run(&mut self)
	{
		loop {
			match self.get_seg_from_uncomp_buf() {
				Ok(mut segs) => {
					self.processed = self.processed + segs.len();
					println!("segment recoded {}", self.processed);
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
							for seg in &mut segs {
								// println!("segment key: {:?} with comp time:{}",seg.get_key(),seg.get_comp_times());
								match seg.get_method().as_ref().unwrap() {
									Methods::Sprintz (scale) => {
										let pre = SprintzDoubleCompress::new(10, 20,*scale);
										pre.run_decompress(seg);

										self.lossy_comp(seg);

									},
									Methods::Buff(scale)=> {
										let pre = SplitBDDoubleCompress::new(10,20, *scale);
										pre.run_decompress(seg);

										self.lossy_comp(seg);
									},
									Methods::Snappy=> {
										let pre = SnappyCompress::new(10,20);
										pre.run_decompress(seg);

										self.lossy_comp(seg);
									},
									Methods::Gzip => {
										let pre = GZipCompress::new(10,20);
										pre.run_decompress(seg);

										self.lossy_comp(seg);
									},
									Methods::Paa(wsize) => {
										if (*wsize<5000){
											let ws = wsize * 2;
											let cur = PAACompress::new(2,20);
											cur.run_single_compress(seg);
											seg.set_method(Methods::Paa(ws));
										}

									},
									Methods::Fourier(ratio) => {
										let r = ratio/2.0;
										let cur = FourierCompress::new(2,20, r);
										cur.fourier_recode_budget_mut(seg, r);
									},
									Methods::Pla(ratio) => {
										let r = ratio/2.0;
										let cur = PLACompress::new(20, r);
										cur.pla_recode_budget_mut(seg, r);
									},
									Methods::Bufflossy(scale,bits) => {
										let mut r = bits - 8 - bits%8 ;
										let cur = BUFFlossy::new(20, *scale, *bits);
										cur.buff_recode_remove_bits(seg, r);
									},
									Methods::Rrd_sample => {
										// do nothing
									}
									_ => todo!()
								}
								seg.update_comp_times();
							}

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
				Err(BufErr::UnderThresh) => {
					thread::sleep(Duration::from_millis(100));
					continue
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