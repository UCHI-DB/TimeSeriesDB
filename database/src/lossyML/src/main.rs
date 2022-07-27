mod file_io;

use smartcore::dataset::*;
// DenseMatrix wrapper around Vec
use smartcore::linalg::naive::dense_matrix::DenseMatrix;
// K-Means
use smartcore::cluster::kmeans::{KMeans, KMeansParameters};
// Performance metrics
use smartcore::metrics::{homogeneity_score, completeness_score, v_measure_score, roc_auc_score, accuracy};
use std::{env, fs};
use crate::file_io::{read_csvfile, read_paafile, read_fftfile, read_grailfile, build_iforest, predict_iforest, compare2lables, compare2lablesDirect, compareGroundTruthUCI, compareGroundTruthUCR, read_plafile};
use std::path::Path;
use std::collections::HashSet;
use std::iter::FromIterator;
use smartcore::cluster::dbscan::DBSCAN;
use smartcore::cluster::dbscan::*;
use smartcore::ensemble::random_forest_classifier::RandomForestClassifier;
use smartcore::model_selection::train_test_split;
use smartcore::naive_bayes::gaussian::GaussianNB;
use smartcore::neighbors::knn_classifier::KNNClassifier;
use smartcore::svm::svc::{SVC, SVCParameters};
use smartcore::tree::decision_tree_classifier::DecisionTreeClassifier;
use time_series_start::knn::get_gamma;
use serde_json::{Result, Value};

fn main() {

    let args: Vec<String> = env::args().collect();
    let method = &args[1];
    let train_set = &args[2];
    let lossy = &args[3];
    let precision = args[4].parse::<f64>().unwrap();
    let mut save = false;
    if args.len()>5{
        save = args[5].parse::<bool>().unwrap();
    }

    // let window = 1;
    println!("{}, {}, {}, {},",precision, train_set, method, lossy);


    let mut need_test_file = false;
    let mut test_file =  train_set.clone().replace("_TRAIN", "_TEST");

    let vec_file: Vec<&str> = train_set.split('/').collect();
    let temp_name = vec_file[vec_file.len()-1];
    let name_vec: Vec<&str> = temp_name.split('_').collect();
    let name = name_vec[0];
    println!("extracteddd name: {}, ",name);

    if method.as_str()=="dtree" || method.as_str()=="iforest" || method.as_str()=="knn" || method.as_str()=="rforest"{
        need_test_file = true;
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



    let mut origin_data = Dataset {
        data: vec![],
        target: vec![],
        num_samples: 0,
        num_features: 0,
        feature_names: vec![],
        target_names: vec![],
        description: "".to_string()
    };

    let mut origin_test_data = Dataset {
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

    origin_data = read_paafile(&Path::new(train_set),1);
    origin_test_data = read_paafile(&Path::new(&test_file),1);

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
        "pla" => {
            digits_data = read_plafile(&Path::new(train_set),precision);
            if need_test_file{
                test_data = read_plafile(&Path::new(&test_file),precision);
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

    let origin_x = DenseMatrix::from_array(
        origin_data.num_samples,
        origin_data.num_features,
        &origin_data.data,
    );

    let origin_totest = DenseMatrix::from_array(
        origin_test_data.num_samples,
        origin_test_data.num_features,
        &origin_test_data.data,
    );

// These are our target class labels
    let mut true_labels = &digits_data.target;
    let k = HashSet::<isize>::from_iter(true_labels.iter().map(|&x|x as isize).collect::<Vec<isize>>()).len();
    println!("K: {}", k);
    println!("#sample: {}, #features: {}", origin_data.num_samples, origin_data.num_features);
    let mut labels= Vec::new();
    let mut origin_labels = Vec::new();
    let model_file = format!("./model/{}_{}.model",&name, &method);
    println!("model directory: {}",model_file);

    match method.as_str() {
        "dbscan" => {
            // Fit & predict with dbscan
            let model = DBSCAN::fit(&origin_x, DBSCANParameters::default().with_eps(0.2).with_min_samples(k*10)).unwrap();
            if save{
                fs::write(model_file,&serde_json::to_string(&model).unwrap());
            }
            labels = model.predict(&x).unwrap();
            origin_labels = model.predict(&origin_x).unwrap();
        }
        "kmeans" => {
            // Fit & predict with kmeans
            let model = KMeans::fit(&origin_x, KMeansParameters::default().with_k(k)).unwrap();
            if save{
                fs::write(model_file,&serde_json::to_string(&model).unwrap());
            }
            labels = model.predict(&x).unwrap();
            origin_labels =  model.predict(&origin_x).unwrap();
        }
        "iforest" => {
            // Fit & predict with isolated forest
            let train_min = origin_data.data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let test_min = origin_test_data.data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let mut min = train_min;
            if train_min>test_min{
                min = test_min
            }
            println!("min value: {},{}", train_min,test_min);
            let iforest = build_iforest(&origin_data.data, &origin_data.target,origin_data.num_features,min);
            if save{
                fs::write(model_file,&serde_json::to_string(&iforest).unwrap());
            }
            predict_iforest(iforest, &test_data.data, &test_data.target, test_data.num_features, min);
            // todo: need to implemente for match labels
        }
        "dtree" => {
            // Fit & predict with decision tree
            let model = DecisionTreeClassifier::fit(&origin_x, &true_labels, Default::default()).unwrap();
            if save{
                fs::write(model_file,&serde_json::to_string(&model).unwrap());
            }
            labels = model.predict(&totest).unwrap();
            origin_labels = model.predict(&origin_totest).unwrap();
            true_labels = &test_data.target;
        }
        "knn" => {
            // Fit & predict with knn
            let model =  KNNClassifier::fit(&origin_x, &true_labels, Default::default()).unwrap();
            if save{
                fs::write(model_file,&serde_json::to_string(&model).unwrap());
            }
            labels = model.predict(&totest).unwrap();
            origin_labels =  model.predict(&origin_totest).unwrap();
            true_labels = &test_data.target;
            // AUC is only work for binary clustering
            // println!("AUC: {}", roc_auc_score(true_labels, &labels));
        }
        "rforest" => {
            // Fit & predict with decision tree
            let model =  RandomForestClassifier::fit(&origin_x, &true_labels, Default::default()).unwrap();
            if save{
                fs::write(model_file,&serde_json::to_string(&model).unwrap());
            }
            labels = model.predict(&totest).unwrap();
            origin_labels = model.predict(&origin_totest).unwrap();
            true_labels = &test_data.target;
            // AUC is only work for binary clustering
            // println!("AUC: {}", roc_auc_score(true_labels, &labels));
        }
        "nb" => {
            // Fit & predict with decision tree
            let gnb = GaussianNB::fit(&origin_x, &true_labels, Default::default()).unwrap();
            if save{
                fs::write(model_file,&serde_json::to_string(&gnb).unwrap());
            }
            labels = gnb.predict(&x).unwrap(); // Predict class labels
            origin_labels = gnb.predict(&origin_x).unwrap();
            // println!("accuracy: {}", accuracy(true_labels, &labels)); // Prints 0.96
        }
        // "svm" => {
        //     // Fit & predict with decision tree
        //     let (x_train, x_test, y_train, y_test) = train_test_split(&x, &true_labels, 0.2, true);
        //     let model =  SVC::fit(&x, &true_labels,  SVCParameters::default().with_c());
        //     labels = model.and_then(|tree| tree.predict(&totest)).unwrap();
        //     true_labels = &test_data.target;
        //     // AUC is only work for binary clustering
        //     // println!("AUC: {}", roc_auc_score(true_labels, &labels));
        // }
        _ => {panic!("clustering method not supported yet.")}
    }

// Measure performance
    let mut correct = 0;
    let mut acc = 0.0;
    let mut acc1 = 0.0;
    if method.as_str()=="dtree" || method.as_str()=="knn" || method.as_str()=="nb" || method.as_str()=="rforest"{
        acc = compare2lablesDirect(&true_labels,&labels);
        acc1 = compare2lablesDirect(&origin_labels,&labels);
    }
    else {
        if train_set.contains("UCRArchive2018"){
            acc = compareGroundTruthUCR(true_labels,&labels, k);
        }
        else if train_set.contains("UCI121"){
            acc = compareGroundTruthUCI(true_labels,&labels, k);
        }

        acc1 = compare2lables(&origin_labels,&labels, k);
    }


    println!("Acc: {},{}",acc,acc1);
    println!("Homogeneity: {}", homogeneity_score(true_labels, &labels));
    println!("Completeness: {}", completeness_score(true_labels, &labels));
    println!("V Measure: {}", v_measure_score(true_labels, &labels));
    // println!("true label: {:?}\n test label: {:?}",true_labels, labels);
    // println!("kmeans distortion: {}", model.unwrap_err().to_string())
}
