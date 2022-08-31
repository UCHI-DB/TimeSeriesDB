use ingestion::{run_mab_test, run_online_mab_test, run_online_test, run_single_test};
use std::env;


fn main() {
	let args: Vec<String> = env::args().collect();

	let config_file = &args[1];
	let task = &args[2];
	let comp = &args[3];
	let recode = &args[4];
	let num_comp = args[5].parse::<i32>().ok().expect("I wasn't given an integer for encoding number!");
	let num_recode = args[6].parse::<i32>().ok().expect("I wasn't given an integer recoding number!");
	let mode = &args[7];
	let tcr = args[8].parse::<f64>().ok().expect("I wasn't given an f64!");

	match mode.as_str(){
		"maboffline" => {
			run_mab_test(config_file,task, comp, recode,num_comp, num_recode);
		}
		"offline" => {
			run_single_test(config_file, task,comp, recode,num_comp, num_recode)
		}
		"mabonline" => {
			run_online_mab_test(config_file, task,comp, recode,num_comp, num_recode, tcr);
		}
		"online" => {
			run_online_test(config_file, task,comp, recode,num_comp, num_recode, tcr);
		}

		_ => {panic!("mode not supported yet!")}
	}


    
}
