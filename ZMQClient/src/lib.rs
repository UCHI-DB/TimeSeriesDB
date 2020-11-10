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
use bytes::Bytes; // Needed for Framed

// tokio
use tokio::runtime::{Runtime, Builder};
use tokio::task::JoinHandle;
use tokio::stream::{Stream, StreamExt};
use futures::future::join_all;
use core::pin::Pin;
use tokio::time;

// Network
use zmq::{Message, Socket};
use tokio::net::{TcpStream,UdpSocket};
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncWrite};
use futures::sink::SinkExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use std::net::SocketAddr;

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
const DEFAULT_THREAD_NUM: usize = 8;

/* Dummy Client to receive mapping and send to corresponding ports
 * Argument 1: config file to use (config file syntax is similar to that of client config for the main DB)
 * Argument 2: number of data points to send per message (i.e. size of vector that is being sent each time)
 */

/* TODO
 * 1) Implement Framed for UDP
 * 2) Initial mapping receive buffer size
 * 
 * Check other TODOs in the code
 */ 
pub fn run_client(config_file: &str, send_size: usize)
{
    // First load settings
	let config = load_config(config_file);

	// Try building Tokio runtime
	let rt = Builder::new_multi_thread()
		.worker_threads(config.thread_num)
		.enable_all()
		.build().expect("tokio runtime failure");
	
	// Receive initial mapping
	let (format_type, mapping) = recv_mapping(&config);
	println!("Connected: type {}, mapping {:?}", format_type, mapping);
	
	// Run with appropriate data type
    match format_type.as_str() {
		"f32" => {
			begin_async::<f32>(&config, &mapping, send_size, rt);
		}
		"f64" => {
			begin_async::<f64>(&config, &mapping, send_size, rt);
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
fn recv_mapping(config: &Config) -> (String, HashMap<u64, u16, FnvBuildHasher>) {
	let context = zmq::Context::new();
	let requester = context.socket(zmq::DEALER).unwrap();
	
	let mut id: [u8; 5] = [0; 5];
	for i in 0..5 {
		id[i] = random::<u8>();
	}
	requester.set_identity(&id).unwrap();

	let five_sec = Duration::from_secs(5);
    loop {
        match requester.connect(format!("{}://{}:{}", "tcp", config.address, config.port).as_str()) {
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
 * Begins the async signals using the Tokio runtime
 * 
 */
fn begin_async<T: 'static>(config: &Config, mapping: &HashMap<u64, u16, FnvBuildHasher>, send_size: usize, rt: Runtime)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug + Unpin
{
	// This has to be read here as reading signal depends on type received from server
	let signals = load_signals(&*config.config);
	let addr = config.address.clone();
	match config.protocol.as_str() {
		"tcp" => {
			rt.block_on(async move {
				let mut signal_joins: Vec<JoinHandle<()>> = Vec::new();
				// Create asynchronous task for each signal
				for (id, signal) in signals {
					match mapping.get(&id) {
						Some(port) => {
							signal_joins.push(tokio::spawn(send_signal_tcp_async::<T>(signal, addr.clone(), *port, send_size)));
						}
						None => {
							println!("No port found!");
						}
					}
				}
				
				join_all(signal_joins).await;
			});
		}
		"udp" => {
			rt.block_on(async move {
				// Create UdpSocket in async setting; port 0 makes OS automatically allocate available port
				let sock = UdpSocket::bind("0.0.0.0:0".parse::<SocketAddr>().unwrap()).await.unwrap();
				let arc_sock = Arc::new(sock);
				let mut signal_joins: Vec<JoinHandle<()>> = Vec::new();
				// Create asynchronous task for each signal
				for (id, signal) in signals {
					match mapping.get(&id) {
						Some(port) => {
							signal_joins.push(tokio::spawn(send_signal_udp_async::<T>(arc_sock.clone(), signal, addr.clone(), *port, send_size)));
						}
						None => {
							println!("No port found!");
						}
					}
				}
				
				join_all(signal_joins).await;
			});
		}
		_ => panic!("Unsupported protocol")
	}
	
}


async fn send_signal_udp_async<T>(socket: Arc<UdpSocket>, mut signal: Box<dyn Stream<Item=T> + Sync + Send + Unpin>,
	address: String, port: u16, send_size: usize)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug + Unpin
{
	let signal_addr = format!("{}:{}", address, port).as_str().parse::<SocketAddr>().unwrap();
	let finish_msg = b"F";
	loop {
		let mut data: Vec<T> = Vec::new();
		// Aggregate data from signal into batches of send_size
		for _i in 0..send_size {
			match signal.next().await {
				Some(value) => { 
					data.push(value);
				}
				None => { 
					// Signal stream over; send whatever is left in buffer
					let serialized = serialize(&data).unwrap();
					socket.send_to(&serialized, &signal_addr).await.unwrap();
					socket.send_to(&finish_msg[..], &signal_addr).await.unwrap(); // Indicates finished
					println!("Finished");
					return;
				}
			}
		}
		let serialized = serialize(&data).unwrap();
		socket.send_to(&serialized, &signal_addr).await.unwrap();
	}
}

async fn send_signal_tcp_async<T>(mut signal: Box<dyn Stream<Item=T> + Sync + Send + Unpin>, address: String, port: u16, send_size: usize)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug + Unpin
{
	let signal_addr = format!("{}:{}", address, port).as_str().parse::<SocketAddr>().unwrap();
	let mut stream;
	match TcpStream::connect(signal_addr).await {
		Ok(s) => {
			stream = s;
			println!("Connected at {}!", port);
		}
		Err(e) => {
			println!("Failed to connect - {:?}", e);
			return;
		}
	};

	// Instead of using TcpStream, write to Framed
	let mut frame = Framed::new(stream, LengthDelimitedCodec::new());

	let finish_msg = b"F";
	loop {
		let mut data: Vec<T> = Vec::new();
		// Aggregate data from signal into batches of send_size
		for _i in 0..send_size {
			match signal.next().await {
				Some(value) => { 
					data.push(value);
				}
				None => { 
					// Signal stream over; send whatever is left in buffer
					let serialized = serialize(&data).unwrap();
					frame.send(Bytes::from(serialized)).await;
					frame.send(Bytes::from(&finish_msg[..])).await;
					//stream.write(&serialized).await.unwrap();
					//stream.write(&finish_msg[..]).await.unwrap(); // Indicates finished
					println!("Finished");
					return;
				}
			}
		}
		let serialized = serialize(&data).unwrap();
		//stream.write(&serialized).await.unwrap();
		frame.send(Bytes::from(serialized)).await;
	}
}

/* Config Loading */

// Struct for encapsulating config data
pub struct Config
{
	thread_num: usize,
	protocol: String,
    address: String,
	port: u16, // Port to receive mapping info from
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
				"udp" => "udp",
                _ => panic!("Unsupported protocol")
            }
        }
        None => "tcp"
	};	

	let thread_num: usize = match config.get("thread") {
        Some(value) => {
            let tmp = value.as_integer().expect("Thread number must be specified as an integer") as usize;
            if tmp <= 0 {
                panic!("Port number must be greater than 0");
            }
            tmp
        }
        None => DEFAULT_THREAD_NUM
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

    Config {
		thread_num: thread_num,
		protocol: protocol.to_owned(),
        address: String::from(addr),
		port: port,
		config: Box::new(config)
    }
    
}


struct SignalCommonConfig {
	signal_id: u64,
	amount: Amount,
	frequency: Frequency
}

/* Loads signal configuration common for all signal types */
fn load_common_signal_configs(signal_config: &Value) -> SignalCommonConfig {
	// This is the signal ID that the DB will store the data in
	let signal_id = signal_config.get("id")
		.expect("The signal ID must be provided")
		.as_integer()
		.expect("The signal ID must be provided as an integer") as u64;

	let amount = match signal_config.get("amount") {
		Some(value) => Amount::Limited (value.as_integer().expect("The signal amount argument must be specified as an integer") as u64),
		None => Amount::Unlimited,
	};
	
	// Variables to pass to ZMQ Dispatcher/signal creations (as tokio::Interval doesn't implement Clone)
	/* let mut freq_start: Option<Instant> = None;
	let mut freq_interval: Option<Duration> = None; */
	let frequency = match signal_config.get("interval") {
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

	SignalCommonConfig {
		signal_id: signal_id,
		amount: amount,
		frequency: frequency
	}
}


/*
 * Loads signals from the given config toml::Value
 * Returns the testdict; the signals and the dispatchers have their reference editted
 */
fn load_signals<T: 'static>(config: &Value) -> Vec<(u64, Box<dyn Stream<Item=T> + Sync + Send + Unpin>)>
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + Debug + FromStr + From<f32> + Unpin,
{	
	let mut signals: Vec<(u64, Box<dyn Stream<Item=T> + Sync + Send + Unpin>)> = Vec::new();
	/* Construct the signals */
	for signal_config in config.get("signals")
		.expect("At least one signal must be provided")
		.as_table()
		.expect("The signals must be provided as a TOML table")
		.values()
	{
		// Common configuration
		let cc = load_common_signal_configs(&signal_config);
		let (signal_id, amount, frequency) = (cc.signal_id, cc.amount, cc.frequency);

		// Per signal type configuration
		let signal_type = signal_config.get("type").expect("The signal type must be provided");
		match signal_type.as_str().expect("The signal type must be provided as a string") {
			"file" => {
				let params = signal_config.get("params").expect("The file signal must provide a params table");
				let reader_type =  params.get("reader_type")
					.expect("A file signal must provide a reader types in the params table")
					.as_str()
					.expect("The reader type must be provided as a string");

				let path = params
					.get("path")
					.expect("The file signal parameters must provide a file path argument")
					.as_str()
					.expect("The file path for the signal must be provided as a string");

				let delim = match params.get("delim") {
					Some(value) => value.as_str()
						.expect("The file delimiter must be privded as a string")
						.chars().next().expect("The provided delimiter must have some value"),
					None => DEFAULT_DELIM,
				};

				let this_signal: Box<(dyn Stream<Item=T> + Sync + Send + Unpin)> = match reader_type {
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
				signals.push((signal_id, this_signal));
			}
			"gen" => {
				if amount == Amount::Unlimited {
					if !signal_config.get("never_die").map_or(false,|v| v.as_bool().expect("The never_die field must be provided as a boolean")) {
						panic!("Provided a generator signal that does have an amount or time bound\n
							This signal would run indefintely and the program would not terminate\n
							If this is what you want, then create the never_die field under this signal and set the value to true");
					}
				}
				let params = signal_config.get("params").expect("The generator signal type requires a params table");
				let this_signal: Box<(dyn Stream<Item=T> + Sync + Send + Unpin)> = match signal_config.get("gen_type")
					.expect("The gen signal must be provided a gen type field")
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
				signals.push((signal_id, this_signal));
			}
			x => panic!("The provided type, {:?}, is not currently supported", x),
		}
	}
	signals
}
