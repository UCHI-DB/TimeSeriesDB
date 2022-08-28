use ingestion::{run_mab_test, run_single_test};
use std::env;


fn main() {
	let args: Vec<String> = env::args().collect();

	let config_file = &args[1];
	let task = &args[2];
	let comp = &args[3];
	let recode = &args[4];
	let num_comp = args[5].parse::<i32>().ok().expect("I wasn't given an integer for encoding number!");
	let num_recode = args[6].parse::<i32>().ok().expect("I wasn't given an integer recoding number!");
	let mab = args[7].parse::<bool>().ok().expect("I wasn't given an boolean!");

	if mab{
		run_mab_test(config_file,task, comp, recode,num_comp, num_recode);
	}
	else{
		run_single_test(config_file, task,comp, recode,num_comp, num_recode)
	}

    
}
