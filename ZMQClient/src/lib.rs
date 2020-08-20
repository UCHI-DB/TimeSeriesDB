#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate futures;

// (De)serialization
use std::str::FromStr;
use std::fmt::Debug;
use serde::Serialize;
use serde::de::DeserializeOwned;
use bincode::{deserialize, serialize};

// tokio
use tokio::runtime::{Runtime, Builder};
use tokio::stream::{Stream, StreamExt};
use futures::prelude::Future;
use futures::future::join_all;
use core::pin::Pin;
use tokio::time;

// Tokio TCP Connection
use zmq::{Message, Socket};
use std::io;
use std::net::Shutdown;

// Hash Map
use std::collections::HashMap;
use fnv::{FnvHashMap, FnvBuildHasher};

// Other useful libraries
use std::time::Duration;
use std::thread::sleep;
use rand::random;
use std::sync::Arc;

// Client imports
use toml::Value;
use std::fs::read_to_string;
use tokio::time::{Instant, interval_at};
use rand::distributions::Uniform;
mod client;
use client::{Amount, Frequency, construct_file_client, construct_file_client_skip_newline};
use client::{construct_gen_client, construct_normal_gen_client};

const DEFAULT_DELIM: char = '\n';

/* Dummy Client to receive mapping and send to corresponding ports
 * Argument 1: config file to use (config file syntax is similar to that of client config for the main DB)
 * Argument 2: number of data points to send per message (i.e. size of vector that is being sent each time)
 */

/* TODO
 * 1) Initial mapping receive buffer size
 * 
 * Check other TODOs in the code
 */ 
pub fn run_client(config_file: &str, send_size: usize)
{
    
	let config = load_config(config_file);
	let mut rt = Builder::new()
		.threaded_scheduler()
		.core_threads(8)
		.build().expect("tokio runtime failure");
	
	// Receive initial mapping
	let context = zmq::Context::new();
	let (format_type, mapping) = recv_mapping(&context, &config);
	println!("Connected: type {}, mapping {:?}", format_type, mapping);
	
	// Run with appropriate data type
    match format_type.as_str() {
		"f32" => {
			begin_async::<f32>(context, &config, &mapping, send_size, rt);
		}
		"f64" => {
			begin_async::<f64>(context, &config, &mapping, send_size, rt);
		}
		x => {
			println!("Received unsupported format: {}", x);
		}
	}
}


/*
 * Given address and port, receives the data type and the initial ID-to-port mapping and return it
 * 
 * TODO arbitrary buffer size
 */
fn recv_mapping(context: &zmq::Context, config: &Config) -> (String, HashMap<u64, u16, FnvBuildHasher>) {
    let requester = context.socket(zmq::DEALER).unwrap();
	requester.set_identity(&config.id).unwrap();
	let five_sec = Duration::from_secs(5);
    loop {
        match requester.connect(format!("{}://{}:{}", config.protocol, config.address, config.port).as_str()) {
            Ok(_) => { break; }
            Err(_) => { sleep(five_sec);}
        }
	}
	
	let mut buffer = [0; 1024];
	loop {
		requester.send(Message::from(""), 0).unwrap();
		match requester.recv_into(&mut buffer, 0) {
			Ok(recv_size) => {
				match bincode::deserialize::<(&str, HashMap<u64, u16, FnvBuildHasher>)>(&buffer[0..recv_size]) {
					Ok((s, m)) => {
						return (String::from(s), m);
					}
					Err(e) => {
						// Deserialization error... do something robust here?
						panic!("{:?}", e)
					}
				}
			}
			Err(_e) => {
				println!("Error: dispatcher could not process received data");
			}
		}
	}
}

/*
 * Begins the async clients using the Tokio runtime
 * 
 */
fn begin_async<T: 'static>(context: zmq::Context, config: &Config, mapping: &HashMap<u64, u16, FnvBuildHasher>, send_size: usize, mut rt: Runtime)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug + Unpin
{
	// This has to be read here as reading client depends on type received from server
	let clients = load_clients(&*config.config);
	let context_rc = Arc::new(context);
	let mut joins = Vec::new();
	for (id, client) in clients {
		match mapping.get(&id) {
			Some(port) => {
				joins.push(rt.spawn(client_async::<T>(context_rc.clone(), client, config.protocol.clone(), config.address.clone(), config.id, *port, send_size)));
			}
			None => {
				println!("No port found!");
			}
		}
	}

	// TODO join with joins (JoinHandle)
	rt.block_on(join_all(joins));
}

/*
 * Represents the process of each client; connects to appropriate port, receives data from stream, then serializes and sends
 * 
 */
async fn client_async<T>(context: Arc<zmq::Context>, mut client: Box<dyn Stream<Item=T> + Sync + Send + Unpin>,
	protocol: String, address: String, id: [u8; 5], port: u16, send_size: usize)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug + Unpin
{
	// Establish connection
	let requester = context.socket(zmq::DEALER).unwrap();
	requester.set_identity(&id).unwrap();

	let five_sec = Duration::from_secs(5);
    loop {
        match requester.connect(format!("{}://{}:{}", protocol, address, port).as_str()) {
            Ok(_) => { break; }
            Err(_) => { sleep(five_sec);}
        }
	}

	loop {
		let mut data: Vec<T> = Vec::new();
		for _i in 0..send_size {
			match client.next().await {
				Some(value) => { 
					data.push(value);
				}
				None => { 
					// Send whatever is left and disconnect
					let serialized = serialize(&data).unwrap();
					requester.send(&serialized, 0).unwrap();
					requester.send(Message::from("F"), 0).unwrap(); // Indicates finished
					println!("Finished");
					return;
				}
			}
		}
		let serialized = serialize(&data).unwrap();
		//println!("Sending {:?}", data);
		requester.send(&serialized, 0).unwrap();
	}
}

/* Config Loading */

pub struct Config
{
	protocol: String,
    address: String,
	port: u16,
	id: [u8; 5],
    config :Box<Value>
}

pub fn load_config(config_file: &str) -> Config
{
    let raw_string = match read_to_string(config_file) {
        Ok(value) => value,
        Err(e) => {
            panic!("{:?}", e)
        }
    };
	let config = raw_string.parse::<Value>().unwrap();
	
	let protocol = match config.get("protocol") {
        Some(value) => {
            match value.as_str().expect("Protocol must be provided as a string") {
                "tcp" => "tcp",
                _ => panic!("Unsupported protocol")
            }
        }
        None => "tcp"
    };

    let addr = config.get("address")
                        .expect("Address must be provided")
                        .as_str()
                        .expect("Address must be provided as a string");

    let port = match config.get("port") {
        Some(value) => {
            let tmp = value.as_integer().expect("Port number must be specified as an integer") as u16;
            if tmp == 0 {
                panic!("Port number must not be 0");
            }
            tmp
        }
        None => panic!("Port number must be specified"),
	};
	
	// This is the identity for ZMQ
    let id = match config.get("id") {
        Some(value) => {
            let arr = value.as_array().expect("The ID must be specified as an array of 5 integers");
            if arr.len() != 5 { panic!("The ID must be specified as an array of length 5") }
            let mut idarr: [u8; 5] = [0; 5];
            for i in 0..5 {
                idarr[i] = arr[i].as_integer().expect("Each element in the ID array must be an integer") as u8;
            }
            idarr
        }
        None => {
            // Random ID
            let mut idarr: [u8; 5] = [0; 5];
            for i in 0..5 {
                idarr[i] = random::<u8>();
            }
            idarr
        },
    };

    Config {
		protocol: String::from(protocol),
        address: String::from(addr),
		port: port,
		id: id.clone(),
		config: Box::new(config)
    }
    
}


struct ClientCommonConfig {
	signal_id: u64,
	amount: Amount,
	frequency: Frequency
}

/* Loads client configuration common for all client types */
fn load_common_client_configs(client_config: &Value) -> ClientCommonConfig {
	// This is the signal ID that the DB will store the data in
	let signal_id = client_config.get("id")
		.expect("The signal ID must be provided")
		.as_integer()
		.expect("The signal ID must be provided as an integer") as u64;

	let amount = match client_config.get("amount") {
		Some(value) => Amount::Limited (value.as_integer().expect("The client amount argument must be specified as an integer") as u64),
		None => Amount::Unlimited,
	};
	
	// Variables to pass to ZMQ Dispatcher/Client creations (as tokio::Interval doesn't implement Clone)
	/* let mut freq_start: Option<Instant> = None;
	let mut freq_interval: Option<Duration> = None; */
	let frequency = match client_config.get("interval") {
		Some(table) => {
			let secs = match table.get("sec") {
				Some(sec_value) => sec_value.as_integer().expect("The sec argument in run period must be provided as an integer") as u64,
				None => 0,
			};
			let nano_secs = match table.get("nano_sec") {
				Some(nano_sec_value) => nano_sec_value.as_integer().expect("The nano_sec argument in run period must be provided as an integer") as u32,
				None => 0,
			};

			if secs == 0 && nano_secs == 0 {
				panic!("The interval period was provided with a value of 0 for both secs and nano_secs. This is not allowed as the signal will have no delay");
			}

			let interval = Duration::new(secs,nano_secs);

			let start_secs = match table.get("start_sec") {
				Some(sec_value) => sec_value.as_integer().expect("The start sec argument in run period must be provided as an integer") as u64,
				None => 0,
			};
			let start_nano_secs = match table.get("start_nano_sec") {
				Some(nano_sec_value) => nano_sec_value.as_integer().expect("The start nano_sec argument in run period must be provided as an integer") as u32,
				None => 0,
			};

			let start = Instant::now() + Duration::new(start_secs,start_nano_secs);

			/* let freq_start = Some(start);
			let freq_interval = Some(interval); */

			Frequency::Delayed(interval_at(start,interval))
		}
		None => Frequency::Immediate,
	};

	ClientCommonConfig {
		signal_id: signal_id,
		amount: amount,
		frequency: frequency
	}
}


/*
 * Loads clients from the given config toml::Value
 * Returns the testdict; the signals and the dispatchers have their reference editted
 */
fn load_clients<T: 'static>(config: &Value) -> Vec<(u64, Box<dyn Stream<Item=T> + Sync + Send + Unpin>)>
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + Debug + FromStr + From<f32> + Unpin,
{	
	let mut clients: Vec<(u64, Box<dyn Stream<Item=T> + Sync + Send + Unpin>)> = Vec::new();
	/* Construct the clients */
	for client_config in config.get("clients")
		.expect("At least one client must be provided")
		.as_table()
		.expect("The clients must be provided as a TOML table")
		.values()
	{
		// Common configuration
		let cc = load_common_client_configs(&client_config);
		let (signal_id, amount, frequency) = (cc.signal_id, cc.amount, cc.frequency);

		// Per client type configuration
		let client_type = client_config.get("type").expect("The client type must be provided");
		match client_type.as_str().expect("The client type must be provided as a string") {
			"file" => {
				let params = client_config.get("params").expect("The file client must provide a params table");
				let reader_type =  params.get("reader_type")
					.expect("A file client must provide a reader types in the params table")
					.as_str()
					.expect("The reader type must be provided as a string");

				let path = params
					.get("path")
					.expect("The file client parameters must provide a file path argument")
					.as_str()
					.expect("The file path for the client must be provided as a string");

				let delim = match params.get("delim") {
					Some(value) => value.as_str()
						.expect("The file delimiter must be privded as a string")
						.chars().next().expect("The provided delimiter must have some value"),
					None => DEFAULT_DELIM,
				};

				let this_client: Box<(dyn Stream<Item=T> + Sync + Send + Unpin)> = match reader_type {
					"NewlineAndSkip" => {

						let skip_val = match params.get("skip") {
							Some(skip_val) => skip_val.as_integer().expect("The skip value must be provided as an integer") as usize,
							None => 0,
						};
						Box::new(construct_file_client_skip_newline::<T>(path, skip_val, delim, amount, frequency).expect("Client could not be properly produced"))
					}
					"DeserializeDelim" => Box::new(construct_file_client::<T>(path, delim as u8, amount, frequency).expect("Client could not be properly produced")),
					x => panic!("The specified file reader, {:?}, is not supported yet", x),
				};
				clients.push((signal_id, this_client));
			}
			"gen" => {
				if amount == Amount::Unlimited {
					if !client_config.get("never_die").map_or(false,|v| v.as_bool().expect("The never_die field must be provided as a boolean")) {
						panic!("Provided a generator client that does have an amount or time bound\n
							This client would run indefintely and the program would not terminate\n
							If this is what you want, then create the never_die field under this client and set the value to true");
					}
				}
				let params = client_config.get("params").expect("The generator client type requires a params table");
				let this_client: Box<(dyn Stream<Item=T> + Sync + Send + Unpin)> = match client_config.get("gen_type")
					.expect("The gen client must be provided a gen type field")
					.as_str()
					.expect("The gen type must be provided as a string")
				{
					"normal" => {
						let std = params.get("std")
							.expect("The normal distribution requires an std field")
							.as_float()
							.expect("The standard deviation must be provided as a float");

						let mean = params.get("std")
							.expect("The normal distribution requires a mean field")
							.as_float()
							.expect("The mean must be provided as a float");

						Box::new(construct_normal_gen_client(mean, std, amount, frequency))
					}
					"uniform" => {
						let low = params.get("low")
							.expect("The uniform distribution requires a low field")
							.as_float()
							.expect("The lower end value of the uniform dist must be provided as a float") as f32;

						let high = params.get("high")
							.expect("The uniform distribution requires a high field")
							.as_float()
							.expect("The higher end value of the uniform dist must be provided as a float") as f32;

						let dist = Uniform::new(low,high);

						Box::new(construct_gen_client::<f32,Uniform<f32>,T>(dist, amount, frequency))
					}
					x => panic!("The provided generator type, {:?}, is not currently supported", x),
				};
				clients.push((signal_id, this_client));
			}
			x => panic!("The provided type, {:?}, is not currently supported", x),
		}
	}
	clients
}
