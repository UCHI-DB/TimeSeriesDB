use crate::segment::Segment;
use serde::{Serialize, Deserialize};
use crate::methods::bit_packing::{sprintz_double_encoder, BitPack, unzigzag};
use std::mem;
use croaring::Bitmap;
use std::time::Instant;
use crate::methods::compress::CompressionMethod;
use std::slice::Iter;
use itertools::Itertools;
use my_bit_vec::BitVec;
use num::FromPrimitive;
use rand::Rng;
use crate::compress::PRECISION_MAP;
use crate::methods::Methods;
use crate::methods::prec_double::{get_precision_bound, PrecisionBound};

#[derive(Clone)]
pub struct BUFFlossy {
    batchsize: usize,
    pub(crate) scale: usize,
    bits: usize
}

impl BUFFlossy {
    pub fn new(batchsize: usize, scale: usize, bits: usize) -> Self {
        BUFFlossy { batchsize, scale, bits }
    }

    pub fn byte_fixed_encode<'a,T>(&self, seg: &mut Segment<T>) -> Vec<u8>
        where T: Serialize + Clone+ Copy+Into<f64> + Deserialize<'a>{

        let mut fixed_vec = Vec::new();

        let mut t:u32 = seg.get_data().len() as u32;
        let mut prec = 0;
        if self.scale == 0{
            prec = 0;
        }
        else{
            prec = (self.scale as f32).log10() as i32;
        }
        let prec_delta = get_precision_bound(prec);
        // println!("precision {}, precision delta:{}", prec, prec_delta);

        let mut bound = PrecisionBound::new(prec_delta);
        // let start1 = Instant::now();
        let dec_len = *(PRECISION_MAP.get(&prec).unwrap()) as u64;
        bound.set_length(0,dec_len);
        let mut min = i64::max_value();
        let mut max = i64::min_value();

        for bd in seg.get_data(){
            let fixed = bound.fetch_fixed_aligned((*bd).into());
            if fixed<min {
                min = fixed;
            }
            else if fixed>max {
                max = fixed;
            }
            fixed_vec.push(fixed);
        }
        let delta = max-min;
        let base_fixed = min;
        // println!("base integer: {}, max:{}",base_fixed,max);
        let ubase_fixed = unsafe { mem::transmute::<i64, u64>(base_fixed) };
        let base_fixed64:i64 = base_fixed;
        let mut single_val = false;
        let mut cal_int_length = 0.0;
        if delta == 0 {
            single_val = true;
        }else {
            cal_int_length = (delta as f64).log2().ceil();
        }

        let fixed_len = cal_int_length as usize;
        bound.set_length((cal_int_length as u64-dec_len), dec_len);
        let ilen = fixed_len -dec_len as usize;
        let dlen = dec_len as usize;
        // println!("int_len:{},dec_len:{}",ilen as u64,dec_len);
        let mut bitpack_vec = BitPack::<Vec<u8>>::with_capacity(8);
        bitpack_vec.write(ubase_fixed as u32,32);
        bitpack_vec.write((ubase_fixed>>32) as u32,32);
        bitpack_vec.write(t, 32);
        bitpack_vec.write(ilen as u32, 32);
        bitpack_vec.write(dlen as u32, 32);

        // set the compression parameters
        seg.set_method(Methods::Bufflossy(self.scale, fixed_len));
        // let duration1 = start1.elapsed();
        // println!("Time elapsed in dividing double function() is: {:?}", duration1);

        // let start1 = Instant::now();
        let mut remain = fixed_len;
        let mut bytec = 0;

        if remain<8{
            for i in fixed_vec{
                bitpack_vec.write_bits((i-base_fixed64) as u32, remain).unwrap();
            }
            remain = 0;
        }
        else {
            bytec+=1;
            remain -= 8;
            let mut fixed_u64 = Vec::new();
            let mut cur_u64 = 0u64;
            if remain>0{
                // let mut k = 0;
                fixed_u64 = fixed_vec.iter().map(|x|{
                    cur_u64 = (*x-base_fixed64) as u64;
                    bitpack_vec.write_byte((cur_u64>>remain) as u8);
                    cur_u64
                }).collect_vec();
            }
            else {
                fixed_u64 = fixed_vec.iter().map(|x|{
                    cur_u64 = (*x-base_fixed64) as u64;
                    bitpack_vec.write_byte((cur_u64) as u8);
                    cur_u64
                }).collect_vec();
            }
            // println!("write the {}th byte of dec",bytec);

            while (remain>=8){
                bytec+=1;
                remain -= 8;
                if remain>0{
                    for d in &fixed_u64 {
                        bitpack_vec.write_byte((*d >>remain) as u8).unwrap();
                    }
                }
                else {
                    for d in &fixed_u64 {
                        bitpack_vec.write_byte(*d as u8).unwrap();
                    }
                }


                // println!("write the {}th byte of dec",bytec);
            }
            if (remain>0){
                bitpack_vec.finish_write_byte();
                for d in fixed_u64 {
                    bitpack_vec.write_bits(d as u32, remain as usize).unwrap();
                }
                // println!("write remaining {} bits of dec",remain);
            }
        }


        let vec = bitpack_vec.into_vec();

        let origin = t * mem::size_of::<T>() as u32;
        let ratio = vec.len() as f64 /origin as f64;
        // print!("{}",ratio);
        vec
    }




    pub fn decode_general<T>(&self, bytes: &Vec<u8>) -> Vec<T>
        where T: FromPrimitive{
        let prec = (self.scale as f32).log10() as i32;
        let prec_delta = get_precision_bound(prec);

        let mut bitpack = BitPack::<&[u8]>::new(bytes.as_slice());
        let mut bound = PrecisionBound::new(prec_delta);
        let lower = bitpack.read(32).unwrap();
        let higher = bitpack.read(32).unwrap();
        let ubase_int= (lower as u64)|((higher as u64)<<32);
        let base_int = unsafe { mem::transmute::<u64, i64>(ubase_int) };
        // println!("base integer: {}",base_int);
        let len = bitpack.read(32).unwrap();
        // println!("total vector size:{}",len);
        let ilen = bitpack.read(32).unwrap();
        // println!("bit packing length:{}",ilen);
        let dlen = bitpack.read(32).unwrap();
        bound.set_length(ilen as u64, dlen as u64);
        // check integer part and update bitmap;
        let mut cur;

        let mut expected_datapoints = Vec::new();
        let mut fixed_vec:Vec<u64> = Vec::new();

        let mut dec_scl:f64 = 2.0f64.powi(dlen as i32);
        // println!("Scale for decimal:{}", dec_scl);

        let mut remain = self.bits;
        let deltabits = (dlen+ilen) as usize -self.bits;
        // println!("remain :{}, delta bits:{}", remain, deltabits);
        let mut bytec = 0;
        let mut chunk;
        let mut f_cur = 0f64;

        if remain<8{
            for i in 0..len {
                cur = bitpack.read_bits(remain as usize).unwrap();
                expected_datapoints.push(FromPrimitive::from_f64((base_int + ((cur as u64)<<deltabits) as i64 ) as f64 / dec_scl).unwrap());
            }
            remain=0
        }
        else {
            bytec+=1;
            remain -= 8;
            chunk = bitpack.read_n_byte(len as usize).unwrap();

            if remain == 0 {
                for &x in chunk {
                    expected_datapoints.push(FromPrimitive::from_f64((base_int + ((x as u64)<<deltabits) as i64) as f64 / dec_scl).unwrap());
                }
                let x1  = chunk[0];
                let x2  = chunk[1];
                // println!("decoded {},{}",x1,x2);
            }
            else{
                // dec_vec.push((bitpack.read_byte().unwrap() as u32) << remain);
                // let mut k = 0;
                for x in chunk{
                    // if k<10{
                    //     println!("write {}th value with first byte {}",k,(*x))
                    // }
                    // k+=1;
                    fixed_vec.push(((*x) as u64)<<remain)
                }
            }
            // println!("read the {}th byte of dec",bytec);

            while (remain>=8){
                bytec+=1;
                remain -= 8;
                chunk = bitpack.read_n_byte(len as usize).unwrap();
                if remain == 0 {
                    for (cur_fixed,cur_chunk) in fixed_vec.iter().zip(chunk.iter()){
                        expected_datapoints.push( FromPrimitive::from_f64((base_int + (((*cur_fixed)|((*cur_chunk) as u64))<<deltabits) as i64 ) as f64 / dec_scl).unwrap());
                    }
                }
                else{
                    let mut it = chunk.into_iter();
                    fixed_vec=fixed_vec.into_iter().map(|x| x|((*(it.next().unwrap()) as u64)<<remain)).collect();
                }


                // println!("read the {}th byte of dec",bytec);
            }

            if (remain>0){
                bitpack.finish_read_byte();
                // println!("read remaining {} bits of dec",remain);
                // println!("length for fixed:{}", fixed_vec.len());
                for cur_fixed in fixed_vec.into_iter(){
                    f_cur = (base_int + (((cur_fixed)|(bitpack.read_bits( remain as usize).unwrap() as u64))<<deltabits) as i64) as f64 / dec_scl;
                    // todo: this is for value reconstruction
                    expected_datapoints.push( FromPrimitive::from_f64(f_cur).unwrap());
                }
            }
        }

        // println!("Number of scan items:{}", expected_datapoints.len());
        expected_datapoints
    }



    pub fn buff_recode_remove_bits<T: FromPrimitive +Clone+ Copy +Into<f64>>(&self, seg:&mut Segment<T>, bits:usize) {
        let size = seg.get_size();
        if (bits<8 ){
            // less than one byte, then apply rrd
            let lossy_vec = self.decode_general(seg.get_comp());
            let mut n_vec =  Vec::new();
            let sample: T = lossy_vec[0];
            n_vec.push( sample);
            // println!("first value: {}", sample.into());
            seg.set_comp(None);
            seg.set_data(n_vec);
            seg.set_method(Methods::Rrd_sample);

        }
        else if bits>=self.bits{

        }else{
            let delta = self.bits-bits;
            let deltabits = delta*size/8;
            let mut vec = seg.get_comp().clone();
            let ori_size = vec.len();
            vec.truncate(ori_size-deltabits);

            seg.set_comp(Some(vec));
            seg.set_method(Methods::Bufflossy(self.scale,bits));
        }


    }


}

impl<'a, T> CompressionMethod<T> for BUFFlossy
    where T: Serialize + Clone+ Copy+Into<f64>+ Deserialize<'a>+ FromPrimitive{
    fn get_segments(&self) {
        unimplemented!()
    }

    fn get_batch(&self) -> usize {
        self.batchsize
    }

    fn run_compress<'b>(&self, segs: &mut Vec<Segment<T>>) {
        // let start = Instant::now();
        for seg in segs {
            let binary =  self.byte_fixed_encode(seg);
            seg.set_comp(Some(binary));
            seg.set_data(Vec::new());
            // method parameter set in encoding function
        }
        // let duration = start.elapsed();
        // info!("Time elapsed in splitBD function() is: {:?}", duration);
    }

    fn run_single_compress(&self, seg: &mut Segment<T>) {
        let binary = self.byte_fixed_encode(seg);
        seg.set_comp(Some(binary));
        seg.set_data(Vec::new());
        // method parameter set in encoding function
    }

    fn run_decompress(&self, seg: &mut Segment<T>) {
        let vec =  self.decode_general(seg.get_comp());
        seg.set_comp(None);
        seg.set_data(vec);
        seg.set_method(Methods::Bufflossy(self.scale, self.bits));

    }
}