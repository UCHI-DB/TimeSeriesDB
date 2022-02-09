mod file_io;

use smartcore::dataset::*;
// DenseMatrix wrapper around Vec
use smartcore::linalg::naive::dense_matrix::DenseMatrix;
// K-Means
use smartcore::cluster::kmeans::{KMeans, KMeansParameters};
// Performance metrics
use smartcore::metrics::{homogeneity_score, completeness_score, v_measure_score};
use std::env;
use crate::file_io::{read_csvfile, read_paafile, read_fftfile, read_grailfile, build_iforest, predict_iforest};
use std::path::Path;
use std::collections::HashSet;
use std::iter::FromIterator;
use smartcore::cluster::dbscan::DBSCAN;
use smartcore::cluster::dbscan::*;
use smartcore::model_selection::train_test_split;
use smartcore::tree::decision_tree_classifier::DecisionTreeClassifier;
use time_series_start::knn::get_gamma;

fn main() {

    let args: Vec<String> = env::args().collect();
    let method = &args[1];
    let train_set = &args[2];
    let lossy = &args[3];
    let precision = args[4].parse::<f64>().unwrap();
    // let window = 1;
    println!("{}, {}, {}, {},",precision, train_set, method, lossy);

    let mut need_test_file = false;
    let mut test_file = "".to_string();
    if method.as_str()=="dtree" || method.as_str()=="iforest"{
        need_test_file = true;
        test_file = train_set.clone().replace("_TRAIN", "_TEST");
    }



    let mut digits_data = Dataset {
        data: vec![],
        target: vec![],
        num_samples: 0,
        num_features: 0,
        feature_names: vec![],
        target_names: vec![],
        description: "".to_string()
    };

    let mut test_data = Dataset {
        data: vec![],
        target: vec![],
        num_samples: 0,
        num_features: 0,
        feature_names: vec![],
        target_names: vec![],
        description: "".to_string()
    };
    // let digits_data = read_csvfile(&Path::new(train_set),precision as i32);
    // let digits_data = read_paafile(&Path::new(train_set),precision as i32);
    // let digits_data = read_fftfile(&Path::new(train_set), precision);


    match lossy.as_str() {
        "buff" => {
            digits_data = read_csvfile(&Path::new(train_set),precision as i32);
            if need_test_file{
                test_data = read_csvfile(&Path::new(&test_file),precision as i32);
            }
        }
        "fft" => {
            digits_data = read_fftfile(&Path::new(train_set),precision);
            if need_test_file{
                test_data = read_fftfile(&Path::new(&test_file),precision);
            }
        }
        "paa" => {
            digits_data = read_paafile(&Path::new(train_set),precision as i32);
            if need_test_file{
                test_data = read_paafile(&Path::new(&test_file),precision as i32);
            }
        }
        "grail" => {
            let train_name = train_set.split('/').last().unwrap();
            let root: &str = "../../../ucr_dict/";
            let gm=get_gamma(&Path::new("../../../database/script/data/gamma_ucr_new.csv"));
            if !(gm.contains_key(train_name)){
                return;
            }
            let gamma= *gm.get(train_name).unwrap() as usize;
            println!("dataset: {} with gamma: {}",train_name,gamma);

            let dict_file = format!("{}{}", root, train_name);
            println!("dict file: {} ",dict_file);

            digits_data = read_grailfile(&Path::new(train_set), &Path::new(&dict_file), gamma, usize::MAX);
            if need_test_file{
                test_data = read_grailfile(&Path::new(&test_file), &Path::new(&dict_file), gamma, usize::MAX);

            }
        }
        _ => {panic!("clustering method not supported yet.")}
    }
    //




// Transform dataset into a NxM matrix
    let x = DenseMatrix::from_array(
        digits_data.num_samples,
        digits_data.num_features,
        &digits_data.data,
    );

    let totest = DenseMatrix::from_array(
        test_data.num_samples,
        test_data.num_features,
        &test_data.data,
    );

// These are our target class labels
    let mut true_labels = &digits_data.target;
    let k = HashSet::<isize>::from_iter(true_labels.iter().map(|&x|x as isize).collect::<Vec<isize>>()).len();
    println!("K: {}", k);

    let mut labels= Vec::new();

    match method.as_str() {
        "dbscan" => {
            // Fit & predict with dbscan
            labels = DBSCAN::fit(&x, DBSCANParameters::default().with_eps(0.5)).
                and_then(|dbscan| dbscan.predict(&x)).unwrap();

        }
        "kmeans" => {
            // Fit & predict with kmeans
            let model = KMeans::fit(&x, KMeansParameters::default().with_k(k));
            labels = model.and_then(|kmeans| kmeans.predict(&x))
                .unwrap();
        }
        "iforest" => {
            // Fit & predict with isolated forest
            let train_min = digits_data.data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let test_min = test_data.data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let mut min = train_min;
            if train_min>test_min{
                min = test_min
            }
            println!("min value: {},{}", train_min,test_min);
            let iforest = build_iforest(&digits_data.data, &digits_data.target,digits_data.num_features,min);
            predict_iforest(iforest, &test_data.data, &test_data.target, test_data.num_features, min);

        }
        "dtree" => {
            // Fit & predict with decision tree
            let (x_train, x_test, y_train, y_test) = train_test_split(&x, &true_labels, 0.2, true);
            let model = DecisionTreeClassifier::fit(&x, &true_labels, Default::default());
            labels = model.and_then(|tree| tree.predict(&totest)).unwrap();
            true_labels = &test_data.target;
        }
        _ => {panic!("clustering method not supported yet.")}
    }

// Measure performance
    let mut correct = 0;
    if method.as_str()=="dtree"{
        for (i, &x) in labels.iter().enumerate() {
            if x as usize == true_labels[i] as usize{
                correct+=1;
            }
        }
    }
    else {
        let mut map = vec![0; k];
        for cluster in 0..k{
            let mut counter = vec![0; k];
            for (i, &x) in labels.iter().enumerate() {
                // println!("i:{},x:{}, x as usize: {}, cluster :{}",i,x,x as usize,cluster+1);
                if (x as usize ) ==cluster{
                    // println!("true label: {}",true_labels[i]);
                    counter[(true_labels[i] as usize-1)] += 1
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


        for (i, &x) in labels.iter().enumerate() {
            if (map[x as usize] + 1) == true_labels[i] as usize{
                correct+=1;
            }
        }

    }

    println!("Acc: {}",correct as f64/true_labels.len() as f64);
    println!("Homogeneity: {}", homogeneity_score(true_labels, &labels));
    println!("Completeness: {}", completeness_score(true_labels, &labels));
    println!("V Measure: {}", v_measure_score(true_labels, &labels));
    // println!("true label: {:?}\n test label: {:?}",true_labels, labels);
    // println!("kmeans distortion: {}", model.unwrap_err().to_string())
}
