use ZMQClient::run_client;
use std::env;

// A simple ZMQ Client for testing purposes.
fn main() {
    
	let args: Vec<String> = env::args().collect();

	let config_file = &args[1];
	run_client(config_file.as_str())

	/* match read_as.as_str() {
		"f32" => run_client::<f32>(config_file.as_str()),
		"f64" => run_client::<f64>(config_file.as_str()),
		_ => panic!("Data type not supported yet")
	} */
	
}
