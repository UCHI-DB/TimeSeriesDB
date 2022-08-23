use std::convert::TryFrom;
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
use lttb::{DataPoint,lttb};
use piecewise_linear::PiecewiseLinearFunction;

#[derive(Clone)]
pub struct PLACompress {
    batchsize: usize,
    ratio: f64
}


pub fn pla_ratio(data: &[f64], ratio: f64) -> Vec<f64>{
    let size = data.len();
    let mut budget= (ratio * size as f64) as usize;
    if budget <2{
        budget = 2;
    }

    // println!("pla budget: {}",budget);

    let mut raw = vec!();
    for (pos, &e) in data.iter().enumerate() {
        raw.push(DataPoint::new(pos as f64, e));
    }
    // Downsample the raw data to use just three datapoints.
    let downsampled = lttb(raw, budget);

    // println!("{},{}",size,  downsampled.len());
    let mut sample = vec!();
    for p in downsampled{
        sample.push((p.x,p.y));
    }
    let f = PiecewiseLinearFunction::try_from(sample).unwrap();
    let mut res:Vec<f64>  = Vec::new();
    for i in 0..size {
        res.push(f.y_at_x(i as f64).unwrap());
    }
    // println!("{:?}\n{:?}",data, res);
    res
}

impl PLACompress {
    pub fn new(batchsize: usize, ratio: f64) -> Self {
        PLACompress { batchsize, ratio}
    }

    // PLA a index from current vector and keep it as compressed version
    pub fn encodeVec<'a,T>(&self, seg: &mut Segment<T>)
        where T: Serialize + FromPrimitive + Clone+ Copy + Into<f64> + Deserialize<'a>{
        let vec = seg.get_data();
        let size = seg.get_size();

        let mut budget= (self.ratio / 2.0 * size as f64) as usize;
        if budget <4{
            budget = 4;
        }
        let mut raw = vec!();
        for (pos, &e) in vec.iter().enumerate() {
            raw.push(DataPoint::new(pos as f64, e.into()));
        }
        // Downsample the raw data to use just three datapoints.
        let downsampled = lttb(raw, budget);

        // println!("{},{}",size,  downsampled.len());
        let mut sample = Vec::new();
        for p in downsampled{
            sample.push(FromPrimitive::from_f64(p.x).unwrap());
            sample.push(FromPrimitive::from_f64(p.y).unwrap());
        }
        seg.set_data(sample);
    }


    pub fn decode<T>(&self, seg: &Segment<T>) -> Vec<T>
        where T:  Clone+ Copy + FromPrimitive + Into<f64> {
        let size = seg.get_size();
        let csize = seg.get_data().len();
        let vec = seg.get_data();
        let mut i = 0;


        let mut sample = vec!();
        while i<csize{
            sample.push((vec[i].into(),vec[i+1].into()));
            i+=2;
        }

        let f = PiecewiseLinearFunction::try_from(sample).unwrap();
        let mut res  = Vec::new();
        for i in 0..size {
            res.push(FromPrimitive::from_f64(f.y_at_x(i as f64).unwrap()).unwrap());
        }
        // println!("{:?}\n{:?}",data, res);
        res
    }


    pub fn pla_recode_budget_mut<T: FromPrimitive +Clone+ Copy +Into<f64>>(&self, seg:&mut Segment<T>, nratio : f64) {
        let size = seg.get_size();
        let csize =  seg.get_data().len();
        let vec_len =  csize/2;
        let budget = (size as f64 * nratio/2.0) as usize;
        let vec = seg.get_data();

        if budget<4 || budget>=vec_len {

        }else{

            let mut i = 0;


            let mut raw = vec!();
            while i<csize{
                raw.push(DataPoint::new(vec[i].into(), vec[i + 1].into()));
                i+=2;
            }

            // Downsample the raw data to use just three datapoints.
            let downsampled = lttb(raw, budget);

            // println!("{},{}",budget,  downsampled.len());
            let mut sample = Vec::new();
            for p in downsampled{
                sample.push(FromPrimitive::from_f64(p.x).unwrap());
                sample.push(FromPrimitive::from_f64(p.y).unwrap());
            }

            seg.set_data(sample);

            seg.set_comp(None);
            seg.set_method(Methods::Pla(nratio));
        }



        // skip normalizing by sqr(size), will handle this in decompression step

    }


}

impl<'a, T> CompressionMethod<T> for PLACompress
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
            seg.set_method(Methods::Pla(self.ratio));
        }

        let duration = start.elapsed();
//        println!("Time elapsed in sprintz function() is: {:?}", duration);
    }

    fn run_single_compress(&self, seg: &mut Segment<T>) {
        self.encodeVec(seg);
        seg.set_method(Methods::Pla(self.ratio));
    }

    fn run_decompress(&self, seg: &mut Segment<T>) {
        let vec =  self.decode(seg);
        seg.set_comp(None);
        seg.set_data(vec);
        seg.set_method(Methods::Pla(self.ratio));
    }
}
