use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use serde::{Serialize};
use serde::de::DeserializeOwned;
use rocksdb::DBVector;
use crate::file_handler::{FileManager};
use std::collections::vec_deque::Drain;
use std::collections::hash_map::{HashMap, Entry};
use std::{fmt, fs};
use std::ops::Add;
use std::time::{SystemTime, UNIX_EPOCH};
use libc::time;
use num::{FromPrimitive, Num, Signed, zero};
use rl_bandit::bandit::{Bandit, UpdateType};
use rl_bandit::bandits::egreedy::EGreedy;
use rustfft::FFTnum;
use smartcore::api::{Predictor, PredictorVec};
use smartcore::cluster::kmeans::KMeans;
use smartcore::ensemble::random_forest_classifier::RandomForestClassifier;
use smartcore::linalg::{BaseVector, Matrix};
use smartcore::linalg::naive::dense_matrix::DenseMatrix;
use smartcore::math::distance::Distance;
use smartcore::math::distance::euclidian::Euclidian;
use smartcore::math::num::RealNumber;
use smartcore::naive_bayes::gaussian::GaussianNB;
use smartcore::neighbors::knn_classifier::KNNClassifier;
use smartcore::tree::decision_tree_classifier::DecisionTreeClassifier;

use crate::{CompressionMethod, FourierCompress, GorillaCompress, GZipCompress, PAACompress, segment, SnappyCompress, SprintzDoubleCompress};

use segment::{Segment, SegmentKey};
use crate::compress::buff_lossy::BUFFlossy;
use crate::compress::pla_lttb::PLACompress;
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
    fn get(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>, BufErr>;

    /* If the index is valid it will return: Ok(mut ref T)
     * Otherwise, it will return: Err(()) to indicate
     * that the index was invalid
     */
    fn get_mut(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>, BufErr>;

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
    fn drain(&mut self) -> Drain<Segment<T>> {
        unimplemented!()
    }

    /* provide encoding suggestion for the recoding daemon, only implemented by LRU cache */
    fn get_recommend(&self) -> (usize,usize,usize) {
        unimplemented!()
    }

    /* Will copy the buffer and collect it into a vector */
    fn copy(&self) -> Vec<Segment<T>>;

    /* Will lock the buffer and write everything to disk */
    fn persist(&self) -> Result<(), BufErr>;

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


    fn get_buffer_size(&self) -> usize {
        unimplemented!()
    }
    /* Signal done*/
    fn is_done(&self) -> bool;

    fn run_query(&self) {
        unimplemented!();
    }
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
pub struct ClockBuffer<T, U>
    where T: Copy + Send,
          U: FileManager<Vec<u8>, DBVector> + Sync + Send,
{
    hand: usize,
    tail: usize,
    buffer: HashMap<SegmentKey, Segment<T>>,
    clock: Vec<(SegmentKey, bool)>,
    clock_map: HashMap<SegmentKey, usize>,
    file_manager: U,
    buf_size: usize,
    done: bool,
}


impl<T, U> SegmentBuffer<T> for ClockBuffer<T, U>
    where T: Copy + Send + Serialize + DeserializeOwned + Debug,
          U: FileManager<Vec<u8>, DBVector> + Sync + Send,
{
    fn get(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>, BufErr> {
        if self.retrieve(key)? {
            match self.buffer.get(&key) {
                Some(seg) => Ok(Some(seg)),
                None => Err(BufErr::GetFail),
            }
        } else {
            Ok(None)
        }
    }

    fn get_mut(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>, BufErr> {
        if self.retrieve(key)? {
            match self.buffer.get_mut(&key) {
                Some(seg) => Ok(Some(seg)),
                None => Err(BufErr::GetMutFail),
            }
        } else {
            Ok(None)
        }
    }


    fn is_done(&self) -> bool {
        self.done
    }

    #[inline]
    fn put(&mut self, seg: Segment<T>) -> Result<(), BufErr> {
        let seg_key = seg.get_key();
        self.put_with_key(seg_key, seg)
    }




    fn copy(&self) -> Vec<Segment<T>> {
        self.buffer.values().map(|x| x.clone()).collect()
    }

    /* Write to file system */
    fn persist(&self) -> Result<(), BufErr> {
        for (seg_key, seg) in self.buffer.iter() {
            let seg_key_bytes = match seg_key.convert_to_bytes() {
                Ok(bytes) => bytes,
                Err(_) => return Err(BufErr::FailedSegKeySer),
            };
            let seg_bytes = match seg.convert_to_bytes() {
                Ok(bytes) => bytes,
                Err(_) => return Err(BufErr::FailedSegSer),
            };

            match self.file_manager.fm_write(seg_key_bytes, seg_bytes) {
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



    fn remove_segment(&mut self) -> Result<Segment<T>, BufErr> {
        let mut counter = 0;
        loop {
            if let (seg_key, false) = self.clock[self.tail] {
                let seg = match self.buffer.remove(&seg_key) {
                    Some(seg) => seg,
                    None => {
                        //println!("Failed to get segment from buffer.");
                        self.update_tail();
                        return Err(BufErr::EvictFailure);
                    }
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


impl<T, U> ClockBuffer<T, U>
    where T: Copy + Send + Serialize + DeserializeOwned + Debug,
          U: FileManager<Vec<u8>, DBVector> + Sync + Send,
{
    pub fn new(buf_size: usize, file_manager: U) -> ClockBuffer<T, U> {
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
            self.clock[slot] = (key, true);
            slot
        } else {
            let slot = self.hand;
            self.clock.push((key, true));
            self.update_hand();
            slot
        };


        match self.clock_map.insert(key, slot) {
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
    fn retrieve(&mut self, key: SegmentKey) -> Result<bool, BufErr> {
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
                                self.put_with_key(key, seg)?;
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

    fn evict(&mut self) -> Result<usize, BufErr> {
        loop {
            if let (seg_key, false) = self.clock[self.hand] {
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

                match self.file_manager.fm_write(seg_key_bytes, seg_bytes) {
                    Ok(()) => {
                        self.tail = self.hand + 1;
                        return Ok(self.hand);
                    }
                    Err(_) => return Err(BufErr::FileManagerErr),
                }
            } else {
                self.clock[self.hand].1 = false;
            }

            self.update_hand();
        }
    }

    fn evict_no_saving(&mut self) -> Result<usize, BufErr> {
        loop {
            if let (seg_key, false) = self.clock[self.hand] {
                self.buffer.remove(&seg_key);
                self.clock_map.remove(&seg_key);
                return Ok(self.hand);
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
    buffer: HashMap<SegmentKey, Segment<T>>,
    clock: Vec<(SegmentKey, bool)>,
    clock_map: HashMap<SegmentKey, usize>,
    buf_size: usize,
    done: bool,
}


impl<T> SegmentBuffer<T> for NoFmClockBuffer<T>
    where T: Copy + Send + Debug,
{
    fn get(&mut self, _key: SegmentKey) -> Result<Option<&Segment<T>>, BufErr> {
        unimplemented!()
    }

    fn get_mut(&mut self, _key: SegmentKey) -> Result<Option<&Segment<T>>, BufErr> {
        unimplemented!()
    }

    fn is_done(&self) -> bool {
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
    fn persist(&self) -> Result<(), BufErr> {
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

    fn get_buffer_size(&self) -> usize {
        unimplemented!()
    }


    fn remove_segment(&mut self) -> Result<Segment<T>, BufErr> {
        let mut counter = 0;
        loop {
            if let (seg_key, false) = self.clock[self.hand] {
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

    fn put_with_key(&mut self, key: SegmentKey, seg: Segment<T>) -> Result<(), BufErr> {
        let slot = if self.buffer.len() >= self.buf_size {
            let slot = self.evict()?;
            self.clock[slot] = (key, true);
            slot
        } else {
            let slot = self.hand;
            self.clock.push((key, true));
            self.update_hand();
            slot
        };


        match self.clock_map.insert(key, slot) {
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

    fn evict(&mut self) -> Result<usize, BufErr> {
        loop {
            if let (seg_key, false) = self.clock[self.hand] {
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


pub fn Get_AggStats<T: Num + FromPrimitive + FFTnum+ Copy + Send + Into<f64> + PartialOrd + Add<T, Output=T>>(seg: &Segment<T>) -> AggStats<T> {
    let vec = Get_Decomp(seg);
    return Get_AggStatsFromVec(&vec);
}

pub fn Get_AggStatsFromVec<T: Num + FromPrimitive + Copy + Send + Into<f64> + PartialOrd + Add<T, Output=T>>(vec: &Vec<T>) -> AggStats<T> {


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
    return AggStats::new(max, min, sum, count);
}


pub fn Get_Decomp<T: Num + FromPrimitive + Copy + Send + FFTnum + Into<f64> >(seg: &Segment<T>) -> Vec<T> {
    let mut vec = Vec::new();
    let size = seg.get_size();
    match seg.get_method().as_ref().unwrap() {
        Methods::Sprintz(scale) => {
            let pre = SprintzDoubleCompress::new(10, 20, *scale);
            vec = pre.decode_general(seg.get_comp());
        }
        Methods::Buff(scale) => {
            let pre = SplitBDDoubleCompress::new(10, 20, *scale);
            vec = pre.decode_general(seg.get_comp());
        }
        Methods::Gorilla => {
            let pre = GorillaCompress::new(10, 20);
            vec = pre.decode_general(seg.get_comp());
        }
        Methods::Snappy => {
            let pre = SnappyCompress::new(10, 20);
            vec = pre.decode(seg.get_comp());
        }
        Methods::Gzip => {
            let pre = GZipCompress::new(10, 20);
            vec = pre.decode(seg.get_comp());
        }
        Methods::Paa(wsize) => {
            // println!("compress time: {}, wsize:{}, paa segment size: {}", seg.get_comp_times(), wsize, size);
            let cur = PAACompress::new(*wsize, 20);
            vec = cur.decodeVec(seg.get_data());
            vec.truncate(size);
        }
        Methods::Rrd_sample => {
            // println!("compress time: {}, rrd segment size: {}", seg.get_comp_times(), size);
            let cur = RRDsample::new(20);
            vec = cur.decode(seg);
        }
        Methods::Pla(ratio) => {
            // println!("compress time: {}, PLA segment size: {}", seg.get_comp_times(), size);
            let cur = PLACompress::new(20,*ratio);
            vec = cur.decode(seg);
        }
        Methods::Bufflossy(scale,bits) => {
            // println!("compress time: {}, buff lossy bits size: {}", seg.get_comp_times(), bits);
            let cur = BUFFlossy::new(20,*scale,*bits);
            vec = cur.decode_general(seg.get_comp());
        }
        Methods::Fourier(ratio) => {
            // println!("compress times: {}, data len:{}, ratio:{}, fft segment size: {}", seg.get_comp_times(), seg.get_data().len(), ratio, size);
            let cur = FourierCompress::new(2, 20, *ratio);
            vec = cur.decodeVec(seg.get_data(),seg.get_size());
        }
        _ => todo!()
    }
    return vec;
}



pub fn Err_Eval<T:PartialEq>(va: &Vec<T>,vb: &Vec<T>)-> usize{
    let err = va.iter().zip(vb).filter(|&(a, b)| a != b).count();
    return err;
}


impl<'a, T, U> fmt::Debug for LRUBuffer<'a, T, U>
    where T: Copy + Send + RealNumber,
          U: FileManager<Vec<u8>, DBVector> + Sync + Send{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LRU buffer with budget [{}]", self.budget)
    }
}

/***************************************************************
 ************************LRUcomp_Buffer************************
 ***************************************************************/
/* LRU based compression with limited space budget.
Use double linked list to achieve O(1) get and put time complexity*/

pub struct LRUBuffer<'a, T, U>
    where T: Copy + Send + RealNumber,
          U: FileManager<Vec<u8>, DBVector> + Sync + Send,

{
    budget: usize,
    cur_size: usize,
    head: Option<SegmentKey>,
    tail: Option<SegmentKey>,
    buffer: BTreeMap<SegmentKey, Node<T>>,
    agg_stats: BTreeMap<SegmentKey, AggStats<T>>,
    est_agg_stats: BTreeMap<SegmentKey, AggStats<T>>,
    comp_runtime: BTreeMap<SegmentKey, f64>,
    file_manager: U,
    buf_size: usize,
    done: bool,
    rlabel: BTreeMap<SegmentKey, Vec<T>>,
    plabel: BTreeMap<SegmentKey, Vec<T>>,
    predictor: Box<PredictorVec<T>+'a>,
    task: usize,
    // predictor: Option<KMeans<T>,
    mab_250: EGreedy,
    mab_125: EGreedy,
    mab_000: EGreedy
}

#[derive(Clone, Debug)]
struct Node<T>
    where T: Copy + Send,
{
    value: Segment<T>,
    next: Option<SegmentKey>,
    prev: Option<SegmentKey>,
}


#[derive(Debug,Clone)]
pub struct AggStats<T>
    where T: Copy + Send + Clone, {
    max: T,
    min: T,
    sum: T,
    count: usize,
}

impl<T> AggStats<T> where T: Copy + Send + Add<Output=T> + PartialOrd, {
    pub fn new(max: T, min: T, sum: T, count: usize) -> Self {
        Self { max, min, sum, count }
    }

    pub fn merge(&self, y: &AggStats<T>) -> AggStats<T> {
        let mut mi;
        if self.min < y.min {
            mi = self.min;
        } else {
            mi = y.min;
        }

        let mut ma;
        if self.max > y.max {
            ma = self.max;
        } else {
            ma = y.max;
        }

        let su = self.sum + y.sum;
        let count = self.count + y.count;

        return AggStats::new(ma, mi, su, count);
    }
}


impl<'a, T, U> LRUBuffer<'a, T, U>
    where T: Copy + Send + Serialize + DeserializeOwned + Debug + FFTnum + Num + FromPrimitive + PartialOrd + Into<f64> + num::Signed + Display+ RealNumber,
          U: FileManager<Vec<u8>, DBVector> + Sync + Send

{
    pub fn new(budget: usize, file_manager: U, task:&str) -> LRUBuffer<'a, T, U> {
        let mut model_dir = format!("../lossyML/model/cbf_{}.model", task);
        let kmeans_dir = format!("../lossyML/model/cbf_kmeans.model");
        println!("created LRU comp buffer with budget {} bytes with model: {}", budget, model_dir);
        let mut taskid : usize = 0; // 0 means ML task, 1 for max, 2 for sum, 3 for max_ML, 4 for sum_ML.

        let  task_vec = task.split('_').collect::<Vec<&str>>();

        if task_vec.len()==1{
            let model : Box<PredictorVec<T>> = match task_vec[0] {
                "kmeans" => {
                    /* Construct the kmeans model	 */
                    let ml_content = fs::read_to_string(model_dir).expect("Unable to read kmeans file");
                    let deserialized_model: KMeans<T> = serde_json::from_str(&ml_content).unwrap();
                    Box::new(deserialized_model )
                },

                "dtree" => {
                    /* Construct the dtree model	 */
                    let ml_content = fs::read_to_string(model_dir).expect("Unable to read dtree file");
                    let deserialized_model: DecisionTreeClassifier<T> = serde_json::from_str(&ml_content).unwrap();
                    Box::new(deserialized_model )
                },
                "knn" => {
                    /* Construct the knn model	 */
                    let ml_content = fs::read_to_string(model_dir).expect("Unable to read knn file");
                    let deserialized_model: KNNClassifier<T, Euclidian> = serde_json::from_str(&ml_content).unwrap();
                    Box::new(deserialized_model )
                },
                "rforest" => {
                    let ml_content = fs::read_to_string(model_dir).expect("Unable to read knn file");
                    let deserialized_model: RandomForestClassifier<T> = serde_json::from_str(&ml_content).unwrap();
                    Box::new(deserialized_model )
                },
                // we use kmeans model as dummy ml task for aggregation workload
                "max" => {
                    /* Construct the kmeans model	 */
                    let ml_content = fs::read_to_string(kmeans_dir).expect("Unable to read kmeans file for max workload");
                    let deserialized_model: KMeans<T> = serde_json::from_str(&ml_content).unwrap();
                    taskid = 1;
                    Box::new(deserialized_model )
                },
                "sum" => {
                    /* Construct the kmeans model	 */
                    let ml_content = fs::read_to_string(kmeans_dir).expect("Unable to read kmeans file for sum workload");
                    let deserialized_model: KMeans<T> = serde_json::from_str(&ml_content).unwrap();
                    taskid = 2;
                    Box::new(deserialized_model )
                },
                // "nb" => {
                //     let ml_content = fs::read_to_string(model_dir).expect("Unable to read knn file");
                //     let deserialized_model: GaussianNB<T, DenseMatrix<T> > = serde_json::from_str(&ml_content).unwrap();
                //     Box::new(deserialized_model )
                // },
                _ => panic!(format!("{} is not supported yet|", task)),
            };
            println!("task: {}, task id: {}", task, taskid);
            LRUBuffer {
                budget: budget,
                cur_size: 0,
                head: None,
                tail: None,
                buffer: BTreeMap::new(),
                agg_stats: BTreeMap::new(),
                est_agg_stats: BTreeMap::new(),
                comp_runtime: BTreeMap::new(),
                file_manager: file_manager,
                buf_size: 0,
                done: false,
                rlabel: BTreeMap::new(),
                plabel: BTreeMap::new(),
                predictor: model,
                task: taskid,
                mab_250: EGreedy::new(4, 0.1, 0.0, UpdateType::Average),
                mab_125: EGreedy::new(4, 0.1, 0.0, UpdateType::Average),
                mab_000: EGreedy::new(4, 0.1, 0.0, UpdateType::Average)
            }
        }
        else{
            model_dir = format!("../lossyML/model/cbf_{}.model", task_vec[1]);
            let model : Box<PredictorVec<T>> = match task_vec[1] {
                "kmeans" => {
                    /* Construct the kmeans model	 */
                    let ml_content = fs::read_to_string(model_dir).expect("Unable to read kmeans file");
                    let deserialized_model: KMeans<T> = serde_json::from_str(&ml_content).unwrap();
                    Box::new(deserialized_model )
                },

                "dtree" => {
                    /* Construct the dtree model	 */
                    let ml_content = fs::read_to_string(model_dir).expect("Unable to read dtree file");
                    let deserialized_model: DecisionTreeClassifier<T> = serde_json::from_str(&ml_content).unwrap();
                    Box::new(deserialized_model )
                },
                "knn" => {
                    /* Construct the knn model	 */
                    let ml_content = fs::read_to_string(model_dir).expect("Unable to read knn file");
                    let deserialized_model: KNNClassifier<T, Euclidian> = serde_json::from_str(&ml_content).unwrap();
                    Box::new(deserialized_model )
                },
                "rforest" => {
                    let ml_content = fs::read_to_string(model_dir).expect("Unable to read knn file");
                    let deserialized_model: RandomForestClassifier<T> = serde_json::from_str(&ml_content).unwrap();
                    Box::new(deserialized_model )
                },
                _ => panic!(format!("{} is not supported yet|", task)),
            };

            if task_vec[0]=="max"{
                taskid = 3;
            }
            else if task_vec[0]=="sum"{
                taskid = 4;
            }
            else if task_vec[0]=="speed"{
                taskid = 5;
            }
            println!("task: {}, {}-{} task id: {}", task, task_vec[0],task_vec[1], taskid);
            LRUBuffer {
                budget: budget,
                cur_size: 0,
                head: None,
                tail: None,
                buffer: BTreeMap::new(),
                agg_stats: BTreeMap::new(),
                comp_runtime: BTreeMap::new(),
                est_agg_stats: BTreeMap::new(),
                file_manager: file_manager,
                buf_size: 0,
                done: false,
                rlabel: BTreeMap::new(),
                plabel: BTreeMap::new(),
                predictor: model,
                task: taskid,
                mab_250: EGreedy::new(4, 0.005, 40.0, UpdateType::Average),
                mab_125: EGreedy::new(4, 0.005, 40.0, UpdateType::Average),
                mab_000: EGreedy::new(4, 0.001, 40.0, UpdateType::Average)
            }
        }

    }

    fn evaluate_query(&self) {
        let n =10;
        let vector_per_seg = 10;
        let labels_of_N = vector_per_seg *n;
        let mut firstNerr = 0;
        let mut lastNerr = 0;
        let mut totalErr = 0;
        let mut maxErr = 0.0;
        let mut sumErr = 0.0;
        let nSeg = self.buffer.len();
        let mut comp_runtime = 0.0;
        let mut estlatestn: AggStats<T> = AggStats {
            max: (T::one()),
            min: (T::one()),
            sum: (T::one()),
            count: 0,
        };
        let mut estuntilnow = AggStats {
            max: (T::one()),
            min: (T::one()),
            sum: (T::one()),
            count: 0,
        };
        let mut latestn = AggStats {
            max: (T::one()),
            min: (T::one()),
            sum: (T::one()),
            count: 0,
        };
        let mut untilnow = AggStats {
            max: (T::one()),
            min: (T::one()),
            sum: (T::one()),
            count: 0,
        };
        let mut earliestn = AggStats {
            max: (T::one()),
            min: (T::one()),
            sum: (T::one()),
            count: 0,
        };
        let mut estearliestn = AggStats {
            max: (T::one()),
            min: (T::one()),
            sum: (T::one()),
            count: 0,
        };

        let mut cnt: usize = 0;
        for (k, v) in self.agg_stats.iter().rev() {
            if cnt == 0 {
                latestn = AggStats::new(v.max, v.min, v.sum, v.count);
                untilnow = AggStats::new(v.max, v.min, v.sum, v.count);
                // println!("{:?}",k);
            } else if cnt < n {
                latestn = latestn.merge(v);
                untilnow = untilnow.merge(v);
            } else {
                untilnow = untilnow.merge(v);
            }
            cnt += 1;
        }

        cnt = 0;
        let mut speedcnt =0;
        for (k, v) in self.est_agg_stats.iter().rev() {
            let agg = v;

            if cnt<(nSeg-n){
                let real_agg = self.agg_stats.get(k).unwrap();
                let runtime = self.comp_runtime.get(k).unwrap();
                maxErr += num::abs(agg.max.into()-real_agg.max.into())/num::abs(real_agg.max.into());
                sumErr += num::abs(agg.sum.into()-real_agg.sum.into())/num::abs(real_agg.sum.into());
                if *runtime!=0.0{
                    comp_runtime += runtime;
                    speedcnt += 1;
                }

            }
            let plabels = self.plabel.get(k).unwrap();
            let cur_err = Err_Eval(plabels, self.rlabel.get(k).unwrap());
            if (cnt<n){
                lastNerr += cur_err;
            }
            totalErr += cur_err;

            if cnt == 0 {
                // println!("{:?}",k);
                estlatestn = AggStats::new(agg.max, agg.min, agg.sum, agg.count);
                estuntilnow = AggStats::new(agg.max, agg.min, agg.sum, agg.count);
            } else if cnt < n {
                estlatestn = estlatestn.merge(&agg);
                estuntilnow = estuntilnow.merge(&agg);
            }
            else {
                estuntilnow = estuntilnow.merge(&agg);
            }

            cnt += 1;
        }
        maxErr = maxErr/(nSeg-n) as f64;
        sumErr = sumErr/(nSeg-n) as f64;

        if speedcnt==0{
            comp_runtime = 0.0;
        }
        else {
            comp_runtime = comp_runtime/speedcnt as f64;
        }

        // calculate the earliest N
        cnt = 0;
        for (k, v) in self.agg_stats.iter() {
            if cnt == 0 {
                // println!("{:?}",k);
                earliestn = AggStats::new(v.max, v.min, v.sum, v.count);

            } else if cnt < n {
                earliestn = earliestn.merge(v);
            }else {
                break;
            }
            cnt += 1;
        }

        cnt = 0;
        for (k, v) in self.est_agg_stats.iter() {
            let agg = v;
            let plabels = self.plabel.get(k).unwrap();
            let cur_err = Err_Eval(plabels, self.rlabel.get(k).unwrap());
            if (cnt<n){
                firstNerr += cur_err;
            }

            if cnt == 0 {
                // println!("segment size: {}, compress timm:{}, first segment: {:?}", v.value.get_data().len(),v.value.get_comp_times(), &vec[..10]);
                estearliestn = AggStats::new(agg.max, agg.min, agg.sum, agg.count);
            } else if cnt < n {
                estearliestn = estearliestn.merge(&agg);
            }
            else {
                break;
            }
            cnt += 1;
        }
        let total_label = vector_per_seg*nSeg;



        // println!("true earliest max: {}, est max: {}",earliestn.max , estearliestn.max);
        // println!("true max: {}, est max: {}",untilnow.max , estuntilnow.max);
        // println!("true nmax: {}, est nmax: {}",latestn.max , estlatestn.max);
        // println!("Aggregation stats (earlies-latest-untilnow): {},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as f64/1000000.0,
        println!("Aggregation stats (earlies-latest-untilnow): {},{},{},{},{},{},{},{},{},{},{},{},{},{:.4},{:.4},{:.4}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as f64/1000000.0,
                 num::abs((earliestn.max - estearliestn.max) / earliestn.max), num::abs((earliestn.min - estearliestn.min) / earliestn.min),
                 num::abs((earliestn.sum - estearliestn.sum) / earliestn.sum),
                 num::abs((latestn.max - estlatestn.max) / latestn.max), num::abs((latestn.min - estlatestn.min) / latestn.min),
                 num::abs((latestn.sum - estlatestn.sum) / latestn.sum), num::abs((untilnow.max - estuntilnow.max) / untilnow.max),
                 num::abs((untilnow.min - estuntilnow.min) / untilnow.min), num::abs((untilnow.sum - estuntilnow.sum) / untilnow.sum),maxErr,sumErr,comp_runtime,
                 firstNerr as f64 /labels_of_N as f64, lastNerr as f64 /labels_of_N as f64, totalErr as f64 / total_label as f64
        )
    }

    fn update_ml_mab(&mut self, m: &Methods, acc: f64){
        let mut ratio = 0.0;
        let c = 1.0;
        match m {
            Methods::Bufflossy(_,bits) => {
                // only for bits >= 8, fall back to rrd otherwise
                ratio = (*bits) as f64/64.0;
                if ratio>=0.25 {
                    self.mab_250.update(0, acc/(c+ratio));
                }
                else if ratio >=0.125{
                    self.mab_125.update(0, acc/(c+ratio));
                }
            }
            Methods::Rrd_sample => {
                // only for 1 sample, so no value for mab125 and mab 250.
                ratio = 1.0/10000.0;
                self.mab_000.update(0, acc/(c+ratio));
            }
            Methods::Paa(wsize) => {
                ratio = 1.0/(*wsize) as f64;
                if ratio>=0.25 {
                    self.mab_250.update(1, acc/(c+ratio));
                }
                else if ratio >=0.125{
                    self.mab_125.update(1, acc/(c+ratio));
                }
                else{
                    self.mab_000.update(1, acc/(c+ratio));
                }
            }
            Methods::Fourier(ratio) => {
                if *ratio>=0.25 {
                    self.mab_250.update(2, acc/(c+ratio));
                }
                else if *ratio >=0.125{
                    self.mab_125.update(2, acc/(c+ratio));
                }
                else{
                    self.mab_000.update(2, acc/(c+ratio));
                }

            }
            Methods::Pla(ratio) => {
                if *ratio>=0.25 {
                    self.mab_250.update(3, acc/(c+ratio));
                }
                else if *ratio >=0.125{
                    self.mab_125.update(3, acc/(c+ratio));
                }
                else{
                    self.mab_000.update(3, acc/(c+ratio));
                }
            }
            _ => {panic!("lossy compression is not supported by LRU ML query");}
        }
        println!("agg_ML reward: {}", acc/(c+ratio));
    }

    fn update_agg_ml_mab(&mut self, m: &Methods, key: &SegmentKey, aggacc: f64, mlacc: f64){
        let r = 0.3;
        let mut agg = 0.0;
        if aggacc==0.0{
            agg = 20.0
        }
        else{
            agg = -aggacc.log10();
        }

        let mut ratio = 0.0;
        let c = 1.0;

        self.comp_runtime.insert(key.clone(), 10.0*mlacc + r*agg);

        match m {
            Methods::Bufflossy(_,bits) => {
                // only for bits >= 8, fall back to rrd otherwise
                ratio = (*bits) as f64/64.0;
                if ratio>=0.25 {
                    self.mab_250.update(0, 10.0*mlacc + r*agg);
                }
                else if ratio >=0.125{
                    self.mab_125.update(0, 10.0*mlacc + r*agg);
                }
            }
            Methods::Rrd_sample => {
                // only for 1 sample, so no value for mab125 and mab 250.
                ratio = 1.0/10000.0;
                self.mab_000.update(0, 10.0*mlacc + r*agg);
            }
            Methods::Paa(wsize) => {
                ratio = 1.0/(*wsize) as f64;
                if ratio>=0.25 {
                    self.mab_250.update(1, 10.0*mlacc + r*agg);
                }
                else if ratio >=0.125{
                    self.mab_125.update(1, 10.0*mlacc + r*agg);
                }
                else{
                    self.mab_000.update(1, 10.0*mlacc + r*agg);
                }
            }
            Methods::Fourier(ratio) => {
                if *ratio>=0.25 {
                    self.mab_250.update(2, 10.0*mlacc + r*agg);
                }
                else if *ratio >=0.125{
                    self.mab_125.update(2, 10.0*mlacc + r*agg);
                }
                else{
                    self.mab_000.update(2, 10.0*mlacc + r*agg);
                }

            }
            Methods::Pla(ratio) => {
                if *ratio>=0.25 {
                    self.mab_250.update(3, 10.0*mlacc + r*agg);
                }
                else if *ratio >=0.125{
                    self.mab_125.update(3, 10.0*mlacc + r*agg);
                }
                else{
                    self.mab_000.update(3, 10.0*mlacc + r*agg);
                }
            }
            _ => {panic!("lossy compression is not supported by LRU ML query");}
        }
        println!("agg_ML reward: {}", 10.0*mlacc + r*agg);

    }

    fn update_speed_ml_mab(&mut self, m: &Methods, speed: f64, mlacc: f64){
        let r = 1.1;
        let mut spd = speed/200000.0;


        let mut ratio = 0.0;
        match m {
            Methods::Bufflossy(_,bits) => {
                // only for bits >= 8, fall back to rrd otherwise
                ratio = (*bits) as f64/64.0;
                if ratio>=0.25 {
                    self.mab_250.update(0, 1.0*mlacc + r*spd);
                }
                else if ratio >=0.125{
                    self.mab_125.update(0, 1.0*mlacc + r*spd);
                }
            }
            Methods::Rrd_sample => {
                // only for 1 sample, so no value for mab125 and mab 250.
                ratio = 1.0/10000.0;
                self.mab_000.update(0, 1.0*mlacc + r*spd);
            }
            Methods::Paa(wsize) => {
                ratio = 1.0/(*wsize) as f64;
                if ratio>=0.25 {
                    self.mab_250.update(1, 1.0*mlacc + r*spd);
                }
                else if ratio >=0.125{
                    self.mab_125.update(1, 1.0*mlacc + r*spd);
                }
                else{
                    self.mab_000.update(1, 1.0*mlacc + r*spd);
                }
            }
            Methods::Fourier(ratio) => {
                if *ratio>=0.25 {
                    self.mab_250.update(2, 1.0*mlacc + r*spd);
                }
                else if *ratio >=0.125{
                    self.mab_125.update(2, 1.0*mlacc + r*spd);
                }
                else{
                    self.mab_000.update(2, 1.0*mlacc + r*spd);
                }

            }
            Methods::Pla(ratio) => {
                if *ratio>=0.25 {
                    self.mab_250.update(3, 1.0*mlacc + r*spd);
                }
                else if *ratio >=0.125{
                    self.mab_125.update(3, 1.0*mlacc + r*spd);
                }
                else{
                    self.mab_000.update(3, 1.0*mlacc + r*spd);
                }
            }
            _ => {panic!("lossy compression is not supported by LRU ML query");}
        }
        println!("speedd_ML reward: {}", 1.0*mlacc + r*spd);

    }

    fn update_agg_mab(&mut self, m: &Methods, val: f64){
        let mut reward = 0.0;
        if val==0.0{
            reward = 20.0
        }
        else{
            reward = -val.log10();
        }
        println!("agg reward: {}", reward);

        match m {
            Methods::Bufflossy(_,bits) => {
                // only for bits >= 8, fall back to rrd otherwise
                let ratio = (*bits) as f64/64.0;
                if ratio>=0.25 {
                    self.mab_250.update(0, reward);
                }
                else if ratio >=0.125{
                    self.mab_125.update(0, reward);
                }
            }
            Methods::Rrd_sample => {
                // only for 1 sample, so no value for mab125 and mab 250.
                self.mab_000.update(0, reward);
            }
            Methods::Paa(wsize) => {
                let ratio = 1.0/(*wsize) as f64;
                if ratio>=0.25 {
                    self.mab_250.update(1, reward);
                }
                else if ratio >=0.125{
                    self.mab_125.update(1, reward);
                }
                else{
                    self.mab_000.update(1, reward);
                    // println!("update 000");
                }
            }
            Methods::Fourier(ratio) => {
                if *ratio>=0.25 {
                    self.mab_250.update(2, reward);
                }
                else if *ratio >=0.125{
                    self.mab_125.update(2, reward);
                }
                else{
                    self.mab_000.update(2, reward);
                }
            }
            Methods::Pla(ratio) => {
                if *ratio>=0.25 {
                    self.mab_250.update(3, reward);
                }
                else if *ratio >=0.125{
                    self.mab_125.update(3, reward);
                }
                else{
                    self.mab_000.update(3, reward);
                }
            }
            _ => {panic!("lossy compression is not supported by LRU agg query");}
        }

    }

    fn get_recommend(&self) -> (usize,usize,usize){
        return (self.mab_250.choose(),self.mab_125.choose(),self.mab_000.choose());
    }

    fn push_back_node(&mut self, key: SegmentKey, value: Segment<T>) {
        let size = value.get_byte_size().unwrap();
        let entry_size = value.get_size();

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
            None => {}
        }
        self.push_back_node(key, value);
    }


    fn put_with_key(&mut self, key: SegmentKey, seg: Segment<T>) -> Result<(), BufErr> {
        // update aggstats and ML labels for query accuracy profiling
        let entry_size = seg.get_size();
        let size = seg.get_byte_size().unwrap();
        let comp_runtime = seg.get_comp_runtime();
        self.comp_runtime.insert(key, comp_runtime);
        // println!("recode method: {:?}",seg.get_method().as_ref().unwrap());
        if IsLossless(seg.get_method().as_ref().unwrap())==true {
            // println!("buffer size: {}, agg stats size: {}",self.buffer.len(), self.agg_stats.len() );
            let vec = Get_Decomp(&seg);
            if !self.rlabel.contains_key(&key){
                // println!("vec size {}", size);
                // let label = self.predictor.as_ref().unwrap().predictVec(&vec,entry_size/1000);
                let label = self.predictor.predictVec(&vec,entry_size/1000);
                self.rlabel.insert(key, label.clone());
                self.plabel.insert(key, label);
            }
            let stats =  Get_AggStatsFromVec(&vec);
            self.agg_stats.insert(key,stats.clone());
            self.est_agg_stats.insert(key, stats);
        }
        else {
            let vec = Get_Decomp(&seg);
            // println!("vec size {}", size);
            // let label = self.predictor.as_ref().unwrap().predictVec(&vec,entry_size/1000);
            let label = self.predictor.predictVec(&vec,entry_size/1000);
            // println!("plabel {:?}", &label);
            // println!("rlabel {:?}", self.rlabel.get(&key).unwrap());
            let acc =  1.0 - Err_Eval(&label, self.rlabel.get(&key).unwrap()) as f64/ label.len() as f64;
            self.plabel.insert(key, label);
            let estaggstats = Get_AggStatsFromVec(&vec);
            let estmax = estaggstats.max.into();
            let estsum = estaggstats.sum.into();
            self.est_agg_stats.insert(key, estaggstats);
            if self.task==0{
                self.update_ml_mab(seg.get_method().as_ref().unwrap(),acc);
            }
            else if self.task==1 {
                let agg = self.agg_stats.get(&key).unwrap();
                let max= agg.max.into();
                self.update_agg_mab(seg.get_method().as_ref().unwrap(),num::abs((max - estmax) / max));
            }
            else if self.task==2 {
                let agg = self.agg_stats.get(&key).unwrap();
                let sum = agg.sum.into();
                self.update_agg_mab(seg.get_method().as_ref().unwrap(),num::abs((sum - estsum) / sum));
            }
            else if self.task==3 {
                let agg = self.agg_stats.get(&key).unwrap();
                let max= agg.max.into();
                self.update_agg_ml_mab(seg.get_method().as_ref().unwrap(), &key,num::abs((max - estmax) / max),acc);
            }
            else if self.task==4 {
                let agg = self.agg_stats.get(&key).unwrap();
                let sum = agg.sum.into();
                self.update_agg_ml_mab(seg.get_method().as_ref().unwrap(), &key,num::abs((sum - estsum) / sum),acc);
            }
            else if self.task==5 {
                let &comp_runtime = &seg.get_comp_runtime();
                self.update_speed_ml_mab(seg.get_method().as_ref().unwrap(),comp_runtime,acc);
                self.comp_runtime.insert(key, 1.1*comp_runtime/200000.0 + acc);
            }
        }

        match self.get(key) {
            Some(v) => {
                self.cur_size -= v.get_byte_size().unwrap();
                let nsize = &seg.get_byte_size().unwrap();
                self.buffer.get_mut(&key).unwrap().value = seg;
                self.cur_size += nsize;
                return Ok(());
            }
            None => {}
        }
        self.push_back_node(key, seg);
        Ok(())
    }

    /* Gets the segment from the filemanager and places it in */
    fn retrieve(&mut self, key: SegmentKey) -> Result<bool, BufErr> {
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
                                self.put_with_key(key, seg)?;
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



impl<'a, T, U> SegmentBuffer<T> for LRUBuffer<'a, T, U>
    where T: Copy + Send + Serialize + DeserializeOwned + Debug + Num +FFTnum+ FromPrimitive + PartialOrd + Into<f64> + Signed + Display+ RealNumber,
          U: FileManager<Vec<u8>, DBVector> + Sync + Send
{
    fn get(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>, BufErr> {
        if self.retrieve(key)? {
            match self.buffer.get(&key) {
                Some(seg) => Ok(Some(&seg.value)),
                None => Err(BufErr::GetFail),
            }
        } else {
            Ok(None)
        }
    }

    fn get_mut(&mut self, key: SegmentKey) -> Result<Option<&Segment<T>>, BufErr> {
        if self.retrieve(key)? {
            match self.buffer.get_mut(&key) {
                Some(seg) => Ok(Some(&seg.value)),
                None => Err(BufErr::GetMutFail),
            }
        } else {
            Ok(None)
        }
    }


    fn is_done(&self) -> bool {
        self.done
    }

    fn run_query(&self) {
        self.evaluate_query();
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
    fn persist(&self) -> Result<(), BufErr> {
        for (seg_key, seg) in self.buffer.iter() {
            let seg_key_bytes = match seg_key.convert_to_bytes() {
                Ok(bytes) => bytes,
                Err(_) => return Err(BufErr::FailedSegKeySer),
            };
            let seg_bytes = match seg.value.convert_to_bytes() {
                Ok(bytes) => bytes,
                Err(_) => return Err(BufErr::FailedSegSer),
            };

            match self.file_manager.fm_write(seg_key_bytes, seg_bytes) {
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

    fn get_buffer_size(&self) -> usize {
        self.cur_size
    }


    fn remove_segment(&mut self) -> Result<Segment<T>, BufErr> {
        match self.head {
            None => {
                return Err(BufErr::BufEmpty);
            }
            Some(head) => {
                match self.get(head) {
                    Some(v) => {
                        return Ok(v);
                    }
                    None => {
                        return Err(BufErr::GetFail);
                    }
                }
            }
        }
    }

    fn idle_threshold(&self, threshold: f32) -> bool {
        //println!("{}full, threshold:{}",(self.buffer.len() as f32 / self.buf_size as f32), threshold);
        return (self.cur_size as f32 / self.budget as f32) < threshold;
    }

    fn exceed_batch(&self, batchsize: usize) -> bool {
        return self.buffer.len() >= batchsize;
    }

    fn get_recommend(&self) -> (usize,usize,usize){
        return self.get_recommend();
    }
}


