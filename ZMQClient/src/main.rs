use ZMQClient::run_client;
use std::env;

// A simple ZMQ Client for testing purposes.
fn main() {
    
	let args: Vec<String> = env::args().collect();

	let config_file = &args[1];
    run_client(config_file.as_str())
}
