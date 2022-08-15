use crate::segment::Segment;
use serde::{Serialize, Deserialize};
use crate::methods::bit_packing::{sprintz_double_encoder, BitPack, unzigzag};
use std::mem;
use croaring::Bitmap;
use std::time::Instant;
use crate::methods::compress::CompressionMethod;
use std::slice::Iter;
use my_bit_vec::BitVec;
use num::FromPrimitive;
use rand::Rng;
use crate::methods::Methods;

#[derive(Clone)]
pub struct RRDsample {
    batchsize: usize,
}

impl RRDsample {
    pub fn new(batchsize: usize) -> Self {
        RRDsample { batchsize }
    }

    // RDD sample a index from current vector and keep it as compressed version
    pub fn encodeVec<'a,T>(&self, seg: &mut Segment<T>)
        where T: Serialize + Clone+ Copy + Deserialize<'a>{
        let vec = seg.get_data();
        let size = seg.get_size();
        let mut rng = rand::thread_rng();
        // let rand_idx = rng.gen_range(0, size);
        let rand_idx = 0;
        // println!("rand rdd index: {}", rand_idx);
        let mut new_vec =  Vec::new();
        new_vec.push(vec[rand_idx]);
        seg.set_data(new_vec);
    }


    pub fn decode<T>(&self, seg: &Segment<T>) -> Vec<T>
        where T:  Clone+ Copy {
        let size = seg.get_size();
        let vec = seg.get_data();
        let v = vec[0];
        let mut uncom = Vec::new();
        let mut paa_data = vec.iter();

        let mut val:f64 = 0.0;

        for j in 0..size{
            uncom.push(v);
        }


        return uncom;
    }





}

impl<'a, T> CompressionMethod<T> for RRDsample
    where T: Serialize + Clone+ Copy+Into<f64>+ Deserialize<'a>+ FromPrimitive{
    fn get_segments(&self) {
        unimplemented!()
    }

    fn get_batch(&self) -> usize {
        self.batchsize
    }

    fn run_compress<'b>(&self, segs: &mut Vec<Segment<T>>) {
        let start = Instant::now();
        for seg in segs {
            self.encodeVec(seg);
            seg.set_method(Methods::Rrd_sample);
        }

        let duration = start.elapsed();
//        println!("Time elapsed in sprintz function() is: {:?}", duration);
    }

    fn run_single_compress(&self, seg: &mut Segment<T>) {
        self.encodeVec(seg);
        seg.set_method(Methods::Rrd_sample);
    }

    fn run_decompress(&self, seg: &mut Segment<T>) {
        let vec =  self.decode(seg);
        seg.set_comp(None);
        seg.set_data(vec);
        seg.set_method(Methods::Rrd_sample);
    }
}
