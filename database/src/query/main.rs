use std::env;
use std::time::{Instant, SystemTime};
use time_series_start::outlier::gen_vector_indices;
use time_series_start::client::construct_file_iterator_skip_newline;
use time_series_start::compress::split_double::SplitBDDoubleCompress;
use time_series_start::compress::gorilla::{GorillaCompress, GorillaBDCompress};
use time_series_start::methods::compress::{SnappyCompress, GZipCompress};
use time_series_start::compress::sprintz::SprintzDoubleCompress;
use time_series_start::segment::Segment;
use log::{error, info, warn};

fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();


    let args: Vec<String> = env::args().collect();
    info!("input args{:?}", args);
    let input_file = &args[1];
    let compression = &args[2];
    let scl = args[3].parse::<usize>().unwrap();
    let size = args[4].parse::<usize>().unwrap();
    let o_ratio = args[5].parse::<f64>().unwrap();

    let file_iter = construct_file_iterator_skip_newline::<f64>(input_file, 0, ',');
    let file_vec: Vec<f64> = file_iter.unwrap().collect();
    let mut seg = Segment::new(None,SystemTime::now(),0,file_vec.clone(),None,None);
    let org_size = seg.get_byte_size().unwrap();


    let indices = gen_vector_indices(o_ratio,size).unwrap();

    let start = Instant::now();

    println!("candidates size: {}", indices.len());
    let mut iter = indices.iter();
    let mut f = Vec::new();
    let mut duration6;

    match compression.as_str(){
        "buff" => {
            let comp = SplitBDDoubleCompress::new(10,10,scl);
            let compressed = comp.byte_fixed_encode(&mut seg);
            let start6 = Instant::now();
            f = comp.buff_decode_condition(compressed,iter);
            duration6 = start6.elapsed();

            println!("Time elapsed in buff single function() is: {:?}", duration6);
        },
        "buff-major" => {
            let comp = SplitBDDoubleCompress::new(10,10,scl);
            let compressed = comp.buff_encode_majority(&mut seg);
            let start6 = Instant::now();
            f = comp.buff_major_decode_condition(compressed,iter);
            duration6 = start6.elapsed();

            println!("Time elapsed in buff-major single function() is: {:?}", duration6);

        },
        "gorilla" => {
            let comp = GorillaCompress::new(10,10);
            let compressed = comp.encode(&mut seg);
            let start6 = Instant::now();
            f = comp.decode_condition(compressed,iter);
            duration6 = start6.elapsed();

            println!("Time elapsed in gorilla single function() is: {:?}", duration6);
        },
        "gorillabd" => {
            let comp = GorillaBDCompress::new(10,10,scl);
            let compressed = comp.encode(&mut seg);
            let start6 = Instant::now();
            f = comp.decode_condition(compressed,iter);
            duration6 = start6.elapsed();

            println!("Time elapsed in gorillabd single function() is: {:?}", duration6);
        },

        "snappy" => {
            let comp = SnappyCompress::new(10,10);
            let compressed = comp.encode(&mut seg);
            let start6 = Instant::now();
            f = comp.decode_condition(compressed,iter);
            duration6 = start6.elapsed();

            println!("Time elapsed in snappy single function() is: {:?}", duration6);
        },

        "gzip" => {
            let comp = GZipCompress::new(10,10);
            let compressed = comp.encode(&mut seg);
            let start6 = Instant::now();
            f = comp.decode_condition(compressed,iter);
            duration6 = start6.elapsed();

            println!("Time elapsed in gzip single function() is: {:?}", duration6);

        },

        "fixed" => {
            let comp = SplitBDDoubleCompress::new(10,10,scl);
            let compressed = comp.fixed_encode(&mut seg);
            let start6 = Instant::now();
            f = comp.fixed_decode_condition(compressed,iter);
            duration6 = start6.elapsed();

            println!("Time elapsed in fixed single function() is: {:?}", duration6);

        },
        "sprintz" => {
            let comp = SprintzDoubleCompress::new(10,10,scl);
            let compressed = comp.encode(&mut seg);
            let start6 = Instant::now();
            f = comp.decode_condition(compressed,iter);
            duration6 = start6.elapsed();
            println!("Time elapsed in sprintz single function() is: {:?}", duration6);

        },
        _ => {panic!("Compression not supported yet.")}
    }

    let duration = start.elapsed();
    let proj= duration6.as_micros() as f64/1000.0664;
    let total = duration.as_micros() as f64/1000.0f64;
    let other = total-proj;
    println!("extracted cur_load values: {}",f.len());
    println!("Time elapsed in projection function() is: {:?}", proj);
    println!("res:{},{},{},{},{},{}",input_file, compression, size, o_ratio,f.len(),proj);


}
