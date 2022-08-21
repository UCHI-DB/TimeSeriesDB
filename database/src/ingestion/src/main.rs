use ingestion::{run_mab_test, run_single_test};
use std::env;


fn main() {
	let args: Vec<String> = env::args().collect();

	let config_file = &args[1];
	let comp = &args[2];
	let recode = &args[3];
	let num_comp = args[4].parse::<i32>().ok().expect("I wasn't given an integer for encoding number!");
	let num_recode = args[5].parse::<i32>().ok().expect("I wasn't given an integer recoding number!");

	run_mab_test(config_file,comp, recode,num_comp, num_recode)

    
}
