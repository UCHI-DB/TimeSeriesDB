#[macro_use]
extern crate serde_derive;
extern crate bincode;

// (De)serialization
use std::str::FromStr;
use std::fmt::Debug;
use serde::Serialize;
use serde::de::DeserializeOwned;
use bincode::{deserialize, serialize};

// tokio
use tokio::runtime::Runtime;
//use tokio::stream::{Stream, StreamExt};
use core::pin::Pin;
use tokio::prelude::*;

// Tokio TCP Connection
use tokio::net::TcpStream;
use std::net::SocketAddr;
//use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io;
use std::net::Shutdown;

// Hash Map
use std::collections::HashMap;
use fnv::{FnvHashMap, FnvBuildHasher};

// Other useful libraries
use std::time::{Duration,Instant};
use tokio::timer::Interval;

// Client imports
use toml::Value;
use std::fs::read_to_string;
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
	let mut rt = Runtime::new().expect("tokio runtime failure");
	
	// Receive initial mapping
	/*
	let mapping = Mapping{ addr: config.address.clone(), port: config.port };
	let (format_type, mapping) = match rt.block_on(mapping) {
		Ok((f, m)) => (f, m),
		Err(e) => {
			panic!("Error in parsing mapping");
		}
	};
	*/
	let (format_type, mapping) = get_mapping(&config.address, config.port);
	println!("Connected: type {}, mapping {:?}", format_type, mapping);
	
	// Run with appropriate data type
    match format_type.as_str() {
		"f32" => {
			begin_async::<f32>(&config.config, &config.address, &mapping, send_size, rt);
		}
		"f64" => {
			begin_async::<f64>(&config.config, &config.address, &mapping, send_size, rt);
		}
		x => {
			println!("Received unsupported format: {}", x);
		}
	}
}


fn get_mapping(addr: &String, port: u16) -> (String, HashMap<u64, u16, FnvBuildHasher>) {
	//let full_addr = format!("{}:{}", addr, port).as_str();
	let mut stream;
	loop {
		match std::net::TcpStream::connect(format!("{}:{}", addr, port).as_str()) {
			Ok(s) => {
				stream = s;
				break;
			}
			Err(e) => (),
		}
	}
	stream.write_all(b"I").unwrap();
	let mut buffer = [0; 1024];
	let recv_size = stream.read(&mut buffer).unwrap();
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

/*
 * Given address and port, receives the data type and the initial ID-to-port mapping and return it
 * 
 * TODO arbitrary buffer size
 */
pub struct Mapping {
	addr: String,
	port: u16
}

impl Future for Mapping {
	type Item = (String, HashMap<u64, u16, FnvBuildHasher>);
	type Error = ();

	fn poll(&mut self) -> Result<Async<(String, HashMap<u64, u16, FnvBuildHasher>)>, ()> {
		let full_addr = format!("{}:{}", self.addr, self.port).as_str().parse::<SocketAddr>().unwrap();
		println!("{:?}", full_addr);
		let mut stream;
		loop {
			match TcpStream::connect(&full_addr).poll() {
				Ok(Async::Ready(s)) => {
					stream = s;
					break;
				}
				Ok(Async::NotReady) => {
					//println!("Here?");
					//return Ok(Async::NotReady);
				}
				Err(e) => {
					// Connection error, sleep for 1 sec and try again
					println!("{:?}", e);
				}
			}
		}
		stream.write_all(b"I").unwrap();
		let mut buffer = [0; 1024];
		let recv_size = stream.read(&mut buffer).unwrap();
		match bincode::deserialize::<(&str, HashMap<u64, u16, FnvBuildHasher>)>(&buffer[0..recv_size]) {
			Ok((s, m)) => {
				return Ok(Async::Ready((String::from(s), m)));
			}
			Err(e) => {
				// Deserialization error... do something robust here?
				panic!("{:?}", e)
			}
		}
	}
}

/*
 * Begins the async clients using the Tokio runtime
 * 
 */
fn begin_async<T: 'static>(config: &Value, addr: &String, mapping: &HashMap<u64, u16, FnvBuildHasher>, send_size: usize, mut rt: Runtime)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug + Unpin
{
	// This has to be read here as reading client depends on type received from server
	let clients = load_clients(config);
	for (id, client) in clients {
		match mapping.get(&id) {
			Some(port) => {
				let tc = TcpClient::<T> {
					client: client,
					addr: format!("{}:{}", addr, port).parse::<SocketAddr>().unwrap(),
					send_size: send_size
				};
				rt.spawn(tc);
			}
			None => {
				println!("No port found!");
			}
		}
	}

	// TODO join with joins (JoinHandle)
	rt.shutdown_on_idle().wait().unwrap();
}

/*
 * Represents the process of each client; connects to appropriate port, receives data from stream, then serializes and sends
 * 
 */

pub struct TcpClient<T>
{
    client: Box<dyn Stream<Item=T, Error=()> + Sync + Send + Unpin>,
	addr: SocketAddr,
	send_size: usize
}

impl<T> Future for TcpClient<T>
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug + Unpin
{
	type Item = ();
	type Error = ();

	fn poll(&mut self) -> Poll<(),()> {
		// Establish connection
		let mut stream;

		// Data sending
		loop {
			// Establish connection
			/*
			loop {
				match TcpStream::connect(&self.addr).poll() {
					Ok(Async::Ready(s)) => {
						stream = s;
						break;
					}
					Ok(Async::NotReady) => (),
					Err(e) => {
						// Connection error, sleep for 1 sec and try again
						println!("{:?}", e);
					}
				}
			}
			*/
			println!("{:?}", self.addr);
			stream = TcpStream::connect(&self.addr).wait().unwrap();
			let mut data: Vec<T> = Vec::new();
			// Fill batch vector
			for _i in 0..self.send_size {
				match self.client.poll() {
					Ok(Async::Ready(Some(value))) => { 
						data.push(value);
					}
					Ok(Async::Ready(None)) => { 
						// Send whatever is left and disconnect
						let serialized = serialize(&data).unwrap();
						match stream.write_all(&serialized[..]) {
							Ok(_) => (),
							Err(e) => {
								println!("{:?}", e);
							}
						}
						
						loop {
							match TcpStream::connect(&self.addr).poll() {
								Ok(Async::Ready(ref mut s)) => {
									s.write_all(b"F").unwrap();
									break;
								}
								Ok(Async::NotReady) => (),
								Err(e) => {
									// Connection error, sleep for 1 sec and try again (only 5 times)
									println!("{:?}", e);
								}
							}
						}
						println!("Dropping...");
						return Ok(Async::Ready(()));
					}
					Ok(Async::NotReady) => (),
					Err(e) => {
						println!("{:?}", e);
					}
				}
				
			}
			
			// Serialize and send full vector
			let serialized = serialize(&data).unwrap();
			match stream.write_all(&serialized[..]) {
				Ok(_) => (),
				Err(e) => {
					println!("{:?}", e);
				}
			}
		}
	}
}


/* Config Loading */

pub struct Config
{
    address: String,
    port: u16,
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

    Config {
        address: String::from(addr),
        port: port,
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

			Frequency::Delayed(Interval::new(start,interval))
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
fn load_clients<T: 'static>(config: &Value) -> Vec<(u64, Box<dyn Stream<Item=T, Error=()> + Sync + Send + Unpin>)>
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + Debug + FromStr + From<f32> + Unpin,
{	
	let mut clients: Vec<(u64, Box<dyn Stream<Item=T, Error=()> + Sync + Send + Unpin>)> = Vec::new();
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

				let this_client: Box<(dyn Stream<Item=T, Error=()> + Sync + Send + Unpin)> = match reader_type {
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
				let this_client: Box<(dyn Stream<Item=T, Error=()> + Sync + Send + Unpin)> = match client_config.get("gen_type")
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
