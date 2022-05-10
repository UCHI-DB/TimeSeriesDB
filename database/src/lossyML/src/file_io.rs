use std::path::Path;
use std::io::{BufReader, BufRead};
use std::fs::File;
use std::str::FromStr;
use smartcore::dataset::Dataset;
use time_series_start::methods::prec_double::{PrecisionBound, get_precision_bound};
use time_series_start::knn::fft_ifft_ratio;
use time_series_start::client::read_dict;
use core::mem;
use csv::Reader;
use time_series_start::kernel::Kernel;
use ndarray::Array2;
use isolation_forest::isolation_forest::{Feature, FeatureList, Forest, Sample};
use rand::distributions::Uniform;
use rand::prelude::*;
use lttb::{DataPoint,lttb};
use piecewise_linear::{PiecewiseLinearFunction};
use std::convert::TryFrom;

pub fn read_csvfile(file: &Path, prec:i32) -> Dataset<f64, f64> {
    let prec_delta = get_precision_bound(prec);
    let mut bound = PrecisionBound::new(prec_delta);
    let mut labels = Vec::new();
    let mut vals  = Vec::new();
    for line in BufReader::new(File::open(file).unwrap()).lines().skip(1){
        let line = line.unwrap();
        let mut iter = line.trim()
            .split(',')
            .map(|x| f64::from_str(x).unwrap());

        let label =  iter.next().unwrap();
        let pixels = {if prec<0 {
            iter.collect::<Vec<_>>()
        }else{
            iter.map(|x|{
                let bd = bound.precision_bound(x);
                // println!("before: {}, after: {}",x,bd);
                bd
            }).collect::<Vec<_>>()
        }
        };
        labels.push(label);
        vals.extend(pixels);
    }
    let ns = labels.len();
    let nf = vals.len()/ns;
    // println!("ns: {}, nf: {}", ns, nf);
    let dataset = Dataset {
        data: vals,
        target: labels,
        num_samples: ns,
        num_features: nf,
        feature_names: vec![],
        target_names: vec![],
        description: "".to_string(),
    };
    return dataset;
}

pub fn read_paafile(file: &Path, wsize:i32) -> Dataset<f64, f64> {
    let mut labels = Vec::new();
    let mut vals  = Vec::new();
    for line in BufReader::new(File::open(file).unwrap()).lines().skip(1){
        let line = line.unwrap();
        let mut iter = line.trim()
            .split(',')
            .map(|x| f64::from_str(x).unwrap());

        let label =  iter.next().unwrap();
        let vec = iter.collect::<Vec<_>>();
        let pixels = {if wsize<=1 {
            vec
        }else{
            let vsize = vec.len();
            let mut paa_data= vec.chunks(wsize as usize)
                .map(|x| {
                    x.iter().fold(0f64, |sum, &i| sum + i
                    ) / (wsize as f64)
                });
            let mut unpaa = Vec::new();

            let mut val:f64 = 0.0;
            for i in 0..vsize as i32 {
                if i%wsize == 0{
                    val=paa_data.next().unwrap();
                }
                unpaa.push(val);
            }
            // println!("{:?}",unpaa.as_slice());
            unpaa
        }
        };
        labels.push(label);
        vals.extend(pixels);
    }
    let ns = labels.len();
    let nf = vals.len()/ns;
    // println!("ns: {}, nf: {}", ns, nf);
    let dataset = Dataset {
        data: vals,
        target: labels,
        num_samples: ns,
        num_features: nf,
        feature_names: vec![],
        target_names: vec![],
        description: "".to_string(),
    };
    return dataset;
}



pub fn read_fftfile(file: &Path, ratio:f64) -> Dataset<f64, f64> {
    let mut labels = Vec::new();
    let mut vals  = Vec::new();
    for line in BufReader::new(File::open(file).unwrap()).lines().skip(1){
        let line = line.unwrap();
        let mut iter = line.trim()
            .split(',')
            .map(|x| f64::from_str(x).unwrap());

        let label =  iter.next().unwrap();
        let vec = iter.collect::<Vec<_>>();
        let pixels = {if ratio>=1.0 {
            vec
        }else{
            fft_ifft_ratio(&vec,ratio)
        }
        };
        // println!("{:?}",vec.as_slice());
        labels.push(label);
        vals.extend(pixels);
    }
    let ns = labels.len();
    let nf = vals.len()/ns;
    // println!("ns: {}, nf: {}", ns, nf);
    let dataset = Dataset {
        data: vals,
        target: labels,
        num_samples: ns,
        num_features: nf,
        feature_names: vec![],
        target_names: vec![],
        description: "".to_string(),
    };
    return dataset;
}

// implementation for largest triangle three buckets (lttb) algorithm for time series downsampling
pub fn read_plafile(file: &Path, ratio:f64) -> Dataset<f64, f64> {
    let mut labels = Vec::new();
    let mut vals  = Vec::new();
    for line in BufReader::new(File::open(file).unwrap()).lines().skip(1){
        let line = line.unwrap();
        let mut iter = line.trim()
            .split(',')
            .map(|x| f64::from_str(x).unwrap());

        let label =  iter.next().unwrap();
        let vec = iter.collect::<Vec<_>>();
        let pixels = {if ratio>=1.0 {
            vec
        }else{
            pla_ratio(&vec,ratio/2.0)
        }
        };
        // println!("{:?}",vec.as_slice());
        labels.push(label);
        vals.extend(pixels);
    }
    let ns = labels.len();
    let nf = vals.len()/ns;
    // println!("ns: {}, nf: {}", ns, nf);
    let dataset = Dataset {
        data: vals,
        target: labels,
        num_samples: ns,
        num_features: nf,
        feature_names: vec![],
        target_names: vec![],
        description: "".to_string(),
    };
    return dataset;
}


pub fn read_grailfile(file: &Path, dict: &Path, gamma:usize, coeffs:usize) -> Dataset<f64, f64> {
    // read dictionary first;
    let dic = read_dict::<f64>(dict.to_str().unwrap(),',');
    // println!("dictionary shape: {} * {}", dic.rows(), dic.cols());

    let mut grail = Kernel::new(dic,gamma,coeffs,1);
    grail.dict_pre_process_v0();

    let mut labels = Vec::new();
    let mut vals  = Vec::new();
    for line in BufReader::new(File::open(file).unwrap()).lines().skip(1){
        let line = line.unwrap();
        let mut iter = line.trim()
            .split(',')
            .map(|x| f64::from_str(x).unwrap());

        let label =  iter.next().unwrap();
        let pixels = {
            let mut batch_vec:Vec<f64> = iter.collect::<Vec<_>>();
            let belesize = batch_vec.len();
            // println!("vec for matrix length: {}", belesize);
            let mut x = Array2::from_shape_vec((1,belesize),mem::replace(&mut batch_vec, Vec::with_capacity(belesize))).unwrap();

            grail.run_v0(x)
        };
        // println!("{:?}",vec.as_slice());
        labels.push(label);
        vals.extend(pixels);
    }
    let ns = labels.len();
    let nf = vals.len()/ns;
    // println!("ns: {}, nf: {}", ns, nf);
    let dataset = Dataset {
        data: vals,
        target: labels,
        num_samples: ns,
        num_features: nf,
        feature_names: vec![],
        target_names: vec![],
        description: "".to_string(),
    };
    return dataset;
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

pub(crate) fn build_iforest(vec: &Vec<f64>, labels: &Vec<f64>, nc: usize, min:f64) -> Forest {

    let mut forest = Forest::new(10, 10);

    let mut idx = 0;
    let target = 1.0;
    let mut ct = 0;

    for row in vec.chunks(nc){
        if labels[idx] != target{
            let mut features = FeatureList::new();
            for (i, &x) in row.iter().enumerate(){
                features.push(Feature::new(i.to_string().as_str(), ((x-min) * 100000.0) as u64));
            }
            let mut sample = Sample::new(labels[idx].to_string().as_str());
            sample.add_features(&mut features);
            forest.add_sample(sample.clone());
            ct+=1;
        }
        idx+=1;
    }
    // Create the forest.
    forest.create();
    println!("finished training with {} sample from {} rows", ct,idx);
    return forest;
}


/// designed for two label sets with 1 difference
pub fn compareGroundTruthUCR(grd: &Vec<f64>, pred: &Vec<f64>, k:usize) -> f64{
    println!("ground truth label {:?}, predicted label: {}",grd.len(), pred.len());
    let mut map = vec![0; k];
    let mut correct = 0;
    for cluster in 0..k{
        let mut counter = vec![0; k];
        for (i, &x) in pred.iter().enumerate() {
            // println!("i:{},x:{}, x as usize: {}, cluster :{}",i,x,x as usize,cluster+1);
            if (x as usize ) ==cluster{
                // println!("true label: {}",grd[i]);
                counter[(grd[i] as usize-1)] += 1
            }
        }
        let mut max = 0;
        let mut translate = 0;
        // println!("--label translation counter: {:?}",counter);
        for (i, &x) in counter.iter().enumerate() {
            if x>max{
                max = x;
                translate = i;
            }
        }
        map[cluster] = translate;
    }
    // println!("--label translation: {:?}",map);


    for (i, &x) in pred.iter().enumerate() {
        if (map[x as usize] + 1) == grd[i] as usize{
            correct+=1;
        }
    }

    let acc = correct as f64 /grd.len() as f64;
    return acc;
}


pub fn compareGroundTruthUCI(grd: &Vec<f64>, pred: &Vec<f64>, k:usize) -> f64{
    println!("ground truth label {:?}, predicted label: {}",grd.len(), pred.len());
    let mut map = vec![0; k];
    let mut correct = 0;
    for cluster in 0..k{
        let mut counter = vec![0; k];
        for (i, &x) in pred.iter().enumerate() {
            // println!("i:{},x:{}, x as usize: {}, cluster :{}",i,x,x as usize,cluster+1);
            if (x as usize ) ==cluster{
                // println!("true label: {}",grd[i]);
                counter[(grd[i] as usize)] += 1
            }
        }
        let mut max = 0;
        let mut translate = 0;
        // println!("--label translation counter: {:?}",counter);
        for (i, &x) in counter.iter().enumerate() {
            if x>max{
                max = x;
                translate = i;
            }
        }
        map[cluster] = translate;
    }
    // println!("--label translation: {:?}",map);


    for (i, &x) in pred.iter().enumerate() {
        if (map[x as usize] ) == grd[i] as usize{
            correct+=1;
        }
    }

    let acc = correct as f64 /grd.len() as f64;
    return acc;
}

/// Designed for two label vectors with the same label set.
pub fn compare2lablesDirect(grd: &Vec<f64>, pred: &Vec<f64>) -> f64{
    let mut correct  = 0;
    for (i, &x) in grd.iter().enumerate() {
        // println!("{},{}",x,origin_labels[i]);
        if x as usize == pred[i] as usize{
            correct+=1;
        }
    }
    println!("correct count: {}",correct);
    let acc = correct as f64 /grd.len() as f64;
    return acc;
}

/// compare two labels: find frequent labels and match
pub fn compare2lables(grd: &Vec<f64>, pred: &Vec<f64>, k:usize) -> f64{
    let mut map = vec![0; k+1];
    let mut correct = 0;
    for cluster in 0..k+1{
        let mut counter = vec![0; k+1];
        for (i, &x) in pred.iter().enumerate() {
            // println!("i:{},x:{}, x as usize: {}, cluster :{}",i,x,x as usize,cluster+1);
            if (x as usize ) ==cluster{
                // println!("true label: {}",grd[i]);
                counter[(grd[i] as usize)] += 1
            }
        }
        let mut max = 0;
        let mut translate = 0;
        // println!("--label translation counter: {:?}",counter);
        for (i, &x) in counter.iter().enumerate() {
            if x>=max{
                max = x;
                translate = i;
            }
        }
        map[cluster] = translate;
    }
    // println!("--label translation: {:?}",map);


    for (i, &x) in pred.iter().enumerate() {
        // println!("{},{},{}",x,grd[i],map[x as usize]);
        if (map[x as usize] ) == grd[i] as usize{
            correct+=1;
        }
    }
    println!("correct count: {}",correct);
    let acc = correct as f64 /grd.len() as f64;
    return acc;
}



pub(crate) fn predict_iforest(forest:Forest, vec:&Vec<f64>, labels:&Vec<f64>, nc: usize,  min:f64)  {
    let mut avg_control_set_score = 0.0;
    let mut avg_outlier_set_score = 0.0;
    let mut avg_control_set_normalized_score = 0.0;
    let mut avg_outlier_set_normalized_score = 0.0;
    let mut num_control_tests = 0;
    let mut num_outlier_tests = 0;
    let mut test_samples = Vec::new();
    let target = 1.0;
    let training_class_name=target.to_string();
    let mut idx = 0;
    for row in vec.chunks(nc){
        if idx==10{
            println!("{:?}",row.clone());
        }
        let mut features = FeatureList::new();
        for (i, &x) in row.iter().enumerate(){
            features.push(Feature::new(i.to_string().as_str(), ((x-min) * 100000.0) as u64));
        }
        let mut sample = Sample::new(labels[idx].to_string().as_str());
        sample.add_features(&mut features);
        test_samples.push(sample);
        idx+=1;
    }

    for test_sample in test_samples {
        let score = forest.score(&test_sample);
        let normalized_score = forest.normalized_score(&test_sample);
        println!("score:{}, n_score:{}, sample name:{:?}", score, normalized_score, test_sample.name.as_str());

        if training_class_name.as_str() != test_sample.name.as_str() {
            avg_control_set_score = avg_control_set_score + score;
            avg_control_set_normalized_score = avg_control_set_normalized_score + normalized_score;
            num_control_tests = num_control_tests + 1;
        }
        else {
            avg_outlier_set_score = avg_outlier_set_score + score;
            avg_outlier_set_normalized_score = avg_outlier_set_normalized_score + normalized_score;
            num_outlier_tests = num_outlier_tests + 1;
        }
    }

// Compute statistics.
    if num_control_tests > 0 {
        avg_control_set_score = avg_control_set_score / num_control_tests as f64;
        avg_control_set_normalized_score = avg_control_set_normalized_score / num_control_tests as f64;
    }
    if num_outlier_tests > 0 {
        avg_outlier_set_score = avg_outlier_set_score / num_outlier_tests as f64;
        avg_outlier_set_normalized_score = avg_outlier_set_normalized_score / num_outlier_tests as f64;
    }

    println!("Avg Control Score: {}", avg_control_set_score);
    println!("Avg Control Normalized Score: {}", avg_control_set_normalized_score);
    println!("Avg Outlier Score: {}", avg_outlier_set_score);
    println!("Avg Outlier Normalized Score: {}", avg_outlier_set_normalized_score);
}

#[test]
fn test_iforest(){
    let file_path = "./data/iris.data.txt";
    let file = match std::fs::File::open(&file_path) {
        Err(why) => panic!("Couldn't open {} {}", file_path, why),
        Ok(file) => file,
    };

    let mut reader = Reader::from_reader(file);
    let mut forest = Forest::new(10, 10);
    let training_class_name = "Iris-setosa";
    let mut training_samples = Vec::new();
    let mut test_samples = Vec::new();
    let mut avg_control_set_score = 0.0;
    let mut avg_outlier_set_score = 0.0;
    let mut avg_control_set_normalized_score = 0.0;
    let mut avg_outlier_set_normalized_score = 0.0;
    let mut num_control_tests = 0;
    let mut num_outlier_tests = 0;
    let mut rng = rand::thread_rng();
    let range = Uniform::from(0..10);

    for record in reader.records() {
        let record = record.unwrap();

        let sepal_length_cm: f64 = record[0].parse().unwrap();
        let sepal_width_cm: f64 = record[1].parse().unwrap();
        let petal_length_cm: f64 = record[2].parse().unwrap();
        let petal_width_cm: f64 = record[3].parse().unwrap();
        let name: String = record[4].parse().unwrap();

        let mut features = FeatureList::new();
        features.push(Feature::new("sepal length in cm", (sepal_length_cm * 10.0) as u64));
        features.push(Feature::new("sepal width in cm", (sepal_width_cm * 10.0) as u64));
        features.push(Feature::new("petal length in cm", (petal_length_cm * 10.0) as u64));
        features.push(Feature::new("petal width in cm", (petal_width_cm * 10.0) as u64));

        let mut sample = Sample::new(&name);
        sample.add_features(&mut features);

        // Randomly split the samples into training and test samples.
        let x = range.sample(&mut rng) as u64;
        if x > 5 && name == training_class_name {
            forest.add_sample(sample.clone());
            training_samples.push(sample);
        }
        else {
            test_samples.push(sample);
        }
    }

// Create the forest.
    forest.create();

// Use each test sample.
    for test_sample in test_samples {
        let score = forest.score(&test_sample);
        let normalized_score = forest.normalized_score(&test_sample);
        println!("score:{}, n_score:{}, sample name:{:?}", score, normalized_score, test_sample.name.as_str());

        if training_class_name == test_sample.name {
            avg_control_set_score = avg_control_set_score + score;
            avg_control_set_normalized_score = avg_control_set_normalized_score + normalized_score;
            num_control_tests = num_control_tests + 1;
        }
        else {
            avg_outlier_set_score = avg_outlier_set_score + score;
            avg_outlier_set_normalized_score = avg_outlier_set_normalized_score + normalized_score;
            num_outlier_tests = num_outlier_tests + 1;
        }
    }

// Compute statistics.
    if num_control_tests > 0 {
        avg_control_set_score = avg_control_set_score / num_control_tests as f64;
        avg_control_set_normalized_score = avg_control_set_normalized_score / num_control_tests as f64;
    }
    if num_outlier_tests > 0 {
        avg_outlier_set_score = avg_outlier_set_score / num_outlier_tests as f64;
        avg_outlier_set_normalized_score = avg_outlier_set_normalized_score / num_outlier_tests as f64;
    }

    println!("Avg Control Score: {}", avg_control_set_score);
    println!("Avg Control Normalized Score: {}", avg_control_set_normalized_score);
    println!("Avg Outlier Score: {}", avg_outlier_set_score);
    println!("Avg Outlier Normalized Score: {}", avg_outlier_set_normalized_score);
}