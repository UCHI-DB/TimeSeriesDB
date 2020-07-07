#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate futures;
extern crate toml_loader;
#[macro_use]
extern crate queues;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate ndarray;
extern crate ndarray_linalg;

use rand::prelude::*;
use rand::distributions::Uniform;
use crate::client::{construct_normal_gen_client, read_dict};
use crate::client::construct_gen_client;
use std::time::SystemTime;
use crate::client::construct_file_client;
use crate::segment::{ FourierCompress, PAACompress};
use rocksdb::{DB};
use std::str::FromStr;
use serde::Serialize;
use std::fmt::Debug;
use serde::de::DeserializeOwned;
use crate::buffer_pool::{SegmentBuffer,ClockBuffer,NoFmClockBuffer};
use crate::future_signal::{BufferedSignal};
use std::path::Path;
use toml_loader::{Loader};
use toml::Value;
use std::time::{Duration,Instant};
use tokio::timer::Interval;
use tokio::prelude::*;
use tokio::runtime::{Builder};
use futures::sync::oneshot;
use std::sync::{Arc,Mutex};
use std::convert::TryFrom;

mod buffer_pool;
mod dictionary;
mod file_handler;
mod segment;
pub mod methods;
mod future_signal;
mod client;
mod query;
mod tree;
mod stats;
mod btree;
mod lcce;
mod kernel;
mod compression_demon;
mod remote_stream;
mod dispatcher;

use client::{construct_file_client_skip_newline,Amount,RunPeriod,Frequency};
use remote_stream::remote_stream_from_receiver;
use dispatcher::ZMQDispatcher;
use std::collections::{HashMap,HashSet};
use std::collections::hash_map::RandomState;
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::sync::atomic::{AtomicBool, Ordering};
use ndarray::Array2;
use rustfft::FFTnum;
use num::Float;
use ndarray_linalg::Lapack;
use crate::compression_demon::CompressionDemon;
use std::thread;
use std::fs::File;
use crate::kernel::Kernel;
use crate::methods::compress::{GZipCompress, ZlibCompress, DeflateCompress, SnappyCompress, GorillaCompress, CompressionMethod};
use crate::methods::Methods::Fourier;
use crate::methods::gorilla_encoder::GorillaEncoder;

const DEFAULT_BUF_SIZE: usize = 150;
const DEFAULT_MPSC_BUF_SIZE: usize = 25;
const DEFAULT_DELIM: char = '\n';

pub fn run_test<T: 'static>(config_file: &str)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + Debug + FFTnum + Float + Lapack + FromStr + From<f32>,
//		  f64: std::convert::From<T>,
//		  f32: std::convert::From<T>

{

	let config = match Loader::from_file(Path::new(config_file)) {
		Ok(config) => config,
		Err(e) => panic!("{:?}", e),
	};

	/* Get segment size */
	let seg_size = config
					.lookup("segment_size")
					.expect("A segment size must be provided")
					.as_integer()
					.expect("The segment size argument must be provided as an integer") as usize;


	/* Construct the file manager to be used */
	let fm = match config.lookup("file_handler") {
		Some (config) => {
			let fm_type = config.lookup("file_manager").expect("A file manager must be provided");
			match fm_type.as_str().expect("A file manager must be provided as a string") {
				"Rocks" => {
					let params = config.lookup("params").expect("A RocksDB file manager requires parameters");
					let path = params.lookup("path").expect("RocksDB requires a path be provided").as_str().expect("Rocks file path must be provided as string");
					let mut db_opts = rocksdb::Options::default();
					db_opts.create_if_missing(true);
					match rocksdb::DB::open(&db_opts, path) {
						Ok(x) => Some(Box::new(x)),
						Err(e) => panic!("Failed to create RocksFM object: {:?}", e),
					}
				}
				x => panic!("File manager type, {:?}, not supported yet", x),
			}
		}
		None => None,
	};


	/* Construct the file manager for compression to be used */
	let fm_comp = match config.lookup("file_handler") {
		Some (config) => {
			let fm_type = config.lookup("file_manager").expect("A file manager must be provided");
			match fm_type.as_str().expect("A file manager must be provided as a string") {
				"Rocks" => {
					let params = config.lookup("params").expect("A RocksDB file manager requires parameters");
					let path = params.lookup("path").expect("RocksDB requires a path be provided").as_str().expect("Rocks file path must be provided as string");
					let mut comp_path = String::from(path);
					comp_path.push_str("comp");
					let new_path = comp_path.as_str();
					let mut db_opts = rocksdb::Options::default();
					db_opts.create_if_missing(true);
					match rocksdb::DB::open(&db_opts, new_path) {
						Ok(x) => Some(Box::new(x)),
						Err(e) => panic!("Failed to create RocksFM object: {:?}", e),
					}
				}
				x => panic!("File manager type, {:?}, not supported yet", x),
			}
		}
		None => None,
	};

	/* Construct the buffer to be used */
	let buffer_size = match config.lookup("buffer") {
		Some(value) => value.lookup("buffer_size").map_or(DEFAULT_BUF_SIZE, |v| v.as_integer().expect("The buffer size should be provided as an integer") as usize),
		None => DEFAULT_BUF_SIZE,
	};

	let buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<T> + Send + Sync)>>>> = match fm {
		Some(fm) => {
			match config.lookup("buffer") {
					Some(config) => {
						let buf_type = config.lookup("type").expect("A buffer type must be provided");
						match buf_type.as_str().expect("Buffer type must be provided as string") {
							"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<T,rocksdb::DB>::new(buffer_size,*fm))))),
							x => panic!("The buffer type, {:?}, is not currently supported to run with a file manager", x),
						}
					}
					None => None,
			}
		}
		None => {
			match config.lookup("buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as a string") {
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<T>::new(buffer_size))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run without a file manager", x),
					}
				}
				None => None,
			}
		}
	};

    /* Create buffer for compression segments*/
    let compre_buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<T> + Send + Sync)>>>> = match fm_comp {
        Some(fm) => {
            match config.lookup("buffer") {
                Some(config) => {
                    let buf_type = config.lookup("type").expect("A buffer type must be provided");
                    match buf_type.as_str().expect("Buffer type must be provided as string") {
                        "Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<T,rocksdb::DB>::new(buffer_size,*fm))))),
                        x => panic!("The buffer type, {:?}, is not currently supported to run with a file manager", x),
                    }
                }
                None => None,
            }
        }
        None => {
            match config.lookup("buffer") {
                Some(config) => {
                    let buf_type = config.lookup("type").expect("A buffer type must be provided");
                    match buf_type.as_str().expect("Buffer type must be provided as a string") {
                        "NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<T>::new(buffer_size))))),
                        x => panic!("The buffer type, {:?}, is not currently supported to run without a file manager", x),
                    }
                }
                None => None,
            }
        }
    };

	/* Construct the clients */
	let mut signals: Vec<Box<(Future<Item=Option<SystemTime>,Error=()> + Send + Sync)>> = Vec::new();
	let mut rng = thread_rng();
	let mut signal_id = rng.gen();

	let mut testdict = None;

	for client_config in config.lookup("clients")
							  .expect("At least one client must be provided")
							  .as_table()
							  .expect("The clients must be provided as a TOML table")
							  .values()
	{
		if let Some(x) = client_config.lookup("id") {
			signal_id = x.as_integer().expect("If an ID for a client is provided it must be supplied as an integer") as u64;
		}

		let client_type = client_config.lookup("type").expect("The client type must be provided");

		let amount = match client_config.lookup("amount") {
			Some(value) => Amount::Limited (value.as_integer().expect("The client amount argument must be specified as an integer") as u64),
			None => Amount::Unlimited,
		};

		let run_period = match client_config.lookup("run_period") {
			Some(table) => {
				let secs = match table.lookup("sec") {
					Some(sec_value) => sec_value.as_integer().expect("The sec argument in run period must be provided as an integer") as u64,
					None => 0,
				};
				let nano_secs = match table.lookup("nano_sec") {
					Some(nano_sec_value) => nano_sec_value.as_integer().expect("The nano_sec argument in run period must be provided as an integer") as u32,
					None => 0,
				};

				if secs == 0 && nano_secs == 0 {
					panic!("The run period was provided a value of 0 for both secs and nano_secs. This is not allowed as the signal will start and immediately exit");
				}

				RunPeriod::Finite(Duration::new(secs,nano_secs))
			}
			None => RunPeriod::Indefinite,
		};

		let frequency = match client_config.lookup("interval") {
			Some(table) => {
				let secs = match table.lookup("sec") {
					Some(sec_value) => sec_value.as_integer().expect("The sec argument in run period must be provided as an integer") as u64,
					None => 0,
				};
				let nano_secs = match table.lookup("nano_sec") {
					Some(nano_sec_value) => nano_sec_value.as_integer().expect("The nano_sec argument in run period must be provided as an integer") as u32,
					None => 0,
				};

				if secs == 0 && nano_secs == 0 {
					panic!("The interval period was provided with a value of 0 for both secs and nano_secs. This is not allowed as the signal will have no delay");
				}

				let interval = Duration::new(secs,nano_secs);

				let start_secs = match table.lookup("start_sec") {
					Some(sec_value) => sec_value.as_integer().expect("The start sec argument in run period must be provided as an integer") as u64,
					None => 0,
				};
				let start_nano_secs = match table.lookup("start_nano_sec") {
					Some(nano_sec_value) => nano_sec_value.as_integer().expect("The start nano_sec argument in run period must be provided as an integer") as u32,
					None => 0,
				};

				let start = Instant::now() + Duration::new(start_secs,start_nano_secs);
				Frequency::Delayed(Interval::new(start,interval))
			}
			None => Frequency::Immediate,
		};

		match client_type.as_str().expect("The client type must be provided as a string") {
			"file" => {
				let params = client_config.lookup("params").expect("The file client must provide a params table");
				let reader_type =  params.lookup("reader_type")
										 .expect("A file client must provide a reader types in the params table")
										 .as_str()
										 .expect("The reader type must be provided as a string");

				let path = params
							.lookup("path")
							.expect("The file client parameters must provide a file path argument")
							.as_str()
							.expect("The file path for the client must be provided as a string");

				let delim = match params.lookup("delim") {
					Some(value) => value.as_str()
										.expect("The file delimiter must be privded as a string")
										.chars().next().expect("The provided delimiter must have some value"),
					None => DEFAULT_DELIM,
				};

				let dict = match params.lookup("dict") {
					Some(value) => {
						let dict_str = value.as_str().expect("The file dictionary file must be privded as a string");
						let mut dic = read_dict::<T>(dict_str,delim);
						println!("dictionary shape: {} * {}", dic.rows(), dic.cols());
						Some(dic)
					},
					None => None,
				};

				testdict = dict.clone();

				let client: Box<(Stream<Item=T,Error=()> + Sync + Send)> = match reader_type {
					"NewlineAndSkip" => {

						let skip_val = match params.lookup("skip") {
							Some(skip_val) => skip_val.as_integer().expect("The skip value must be provided as an integer") as usize,
							None => 0,
						};
					;
						Box::new(construct_file_client_skip_newline::<T>(path, skip_val, delim, amount, run_period, frequency).expect("Client could not be properly produced"))
					}
					"DeserializeDelim" => Box::new(construct_file_client::<T>(path, delim as u8, amount, run_period, frequency).expect("Client could not be properly produced")),
					x => panic!("The specified file reader, {:?}, is not supported yet", x),
				};



				match &buf_option {
					Some(buf) => signals.push(Box::new(BufferedSignal::new(signal_id, client, seg_size, *buf.clone(), |i,j| i >= j, |_| (), false,dict))),
					None => panic!("Buffer and File manager provided not supported yet"),
				}
			}
			"gen" => {
				if amount == Amount::Unlimited && run_period == RunPeriod::Indefinite {
					if !client_config.lookup("never_die").map_or(false,|v| v.as_bool().expect("The never_die field must be provided as a boolean")) {
						panic!("Provided a generator client that does have an amount or time bound\n
							    This client would run indefintely and the program would not terminate\n
							    If this is what you want, then create the never_die field under this client and set the value to true");
					}
				}
				let params = client_config.lookup("params").expect("The generator client type requires a params table");
				let client: Box<(Stream<Item=T,Error=()> + Sync + Send)> = match client_config.lookup("gen_type")
								   .expect("The gen client must be provided a gen type field")
								   .as_str()
								   .expect("The gen type must be provided as a string")
				{
					"normal" => {
						let std = params.lookup("std")
										.expect("The normal distribution requires an std field")
										.as_float()
										.expect("The standard deviation must be provided as a float");

						let mean = params.lookup("std")
										 .expect("The normal distribution requires a mean field")
										 .as_float()
										 .expect("The mean must be provided as a float");

						Box::new(construct_normal_gen_client(mean, std, amount, run_period, frequency))
					}
					"uniform" => {
						let low = params.lookup("low")
										.expect("The uniform distribution requires a low field")
										.as_float()
										.expect("The lower end value of the uniform dist must be provided as a float") as f32;

						let high = params.lookup("high")
									   .expect("The uniform distribution requires a high field")
									   .as_float()
									   .expect("The higher end value of the uniform dist must be provided as a float") as f32;

						let dist = Uniform::new(low,high);

						Box::new(construct_gen_client::<f32,Uniform<f32>,T>(dist, amount, run_period, frequency))
					}
					x => panic!("The provided generator type, {:?}, is not currently supported", x),
				};
				match &buf_option {
					Some(buf) => signals.push(Box::new(BufferedSignal::new(signal_id, client, seg_size, *buf.clone(), |i,j| i >= j, |_| (), false, None))),
					None => panic!("Buffer and File manager provided not supported yet"),
				}
			}
			x => panic!("The provided type, {:?}, is not currently supported", x),
		}
		signal_id = rng.gen();
	}

	let buf = buf_option.clone();
	let comp_buf = compre_buf_option.clone();
//	let buf1 = buf_option.clone();
//	let comp_buf1 = compre_buf_option.clone();
//	let buf2 = buf_option.clone();
//	let comp_buf2 = compre_buf_option.clone();

	//let mut kernel = Kernel::new(testdict.clone().unwrap(),1,4,30);
	//kernel.rbfdict_pre_process();

//    let mut compress_demon:CompressionDemon<_,DB,_> = CompressionDemon::new(*buf_option.unwrap().clone(),*compre_buf_option.unwrap().clone(),None,0.1,0.1,|x|(paa_compress(x,50)));
//	let mut compress_demon:CompressionDemon<_,DB,_> = CompressionDemon::new(*buf.unwrap(),*comp_buf.unwrap(),None,0.1,0.1,kernel);
	let mut compress_demon:CompressionDemon<_,DB,_> = CompressionDemon::new(*buf.unwrap(),*comp_buf.unwrap(),None,0.1,0.1,PAACompress::new(10,10));
//	let mut compress_demon1:CompressionDemon<_,DB,_> = CompressionDemon::new(*buf1.unwrap(),*comp_buf1.unwrap(),None,0.1,0.1,FourierCompress::new(10,1));
//	let mut compress_demon2:CompressionDemon<_,DB,_> = CompressionDemon::new(*buf2.unwrap(),*comp_buf2.unwrap(),None,0.1,0.1,FourierCompress::new(10,1));

	/* Construct the runtime */
	let rt = match config.lookup("runtime") {
		None => Builder::new()
					.after_start(|| println!("Threads have been constructed"))
					.build()
					.expect("Failed to produce a default runtime"),

		Some(value) => {
			let core_threads = value.lookup("core_threads")
									.expect("Core threads field required by custom runtime")
									.as_integer()
									.expect("Core threads should be provided as an integer") as usize;

			let blocking_threads = value.lookup("blocking_threads")
										.expect("Blocking threads field required by custom runtime")
										.as_integer()
										.expect("Blocking threads should be provided as an integer") as usize;

			Builder::new()
					.core_threads(core_threads)
					.blocking_threads(blocking_threads)
					.after_start(|| println!("Threads have been constructed"))
					.build()
					.expect("Failed to produce the custom runtime")
		}
	};

	let handle = thread::spawn(move || {
		println!("Run compression demon" );
		compress_demon.run();
		println!("segment commpressed: {}", compress_demon.get_processed() );
	});

	let executor = rt.executor();

	let mut spawn_handles: Vec<oneshot::SpawnHandle<Option<SystemTime>,()>> = Vec::new();

	for sig in signals {
		spawn_handles.push(oneshot::spawn(sig, &executor))
	}



//	let handle1 = thread::spawn(move || {
//		println!("Run compression demon 1" );
//		compress_demon1.run();
//		println!("segment commpressed: {}", compress_demon1.get_processed() );
//	});
//
//	let handle2 = thread::spawn(move || {
//		println!("Run compression demon 2" );
//		compress_demon2.run();
//		println!("segment commpressed: {}", compress_demon2.get_processed() );
//	});

	for sh in spawn_handles {
		match sh.wait() {
			Ok(Some(x)) => println!("Produced a timestamp: {:?}", x),
			_ => println!("Failed to produce a timestamp"),
		}
	}

	handle.join().unwrap();
	//handle1.join().unwrap();
	//handle2.join().unwrap();

	match rt.shutdown_on_idle().wait() {
		Ok(_) => (),
		Err(_) => panic!("Failed to shutdown properly"),
	}

}



pub fn run_single_test<T: 'static>(config_file: &str, comp:&str, num_comp:i32)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + Debug + FFTnum + Float + Lapack + FromStr + From<f32>,
{

	/* let config = match Loader::from_file(Path::new(config_file)) {
		Ok(config) => config,
		Err(e) => panic!("{:?}", e),
	}; */

	/* Loading the config file as toml::Value
	 * toml-loader was causing issues with different versions of toml
	 * as it only supported previous versions
	 */
	let mut file = match File::open(&config_file) {
        Ok(file) => file,
        Err(e)  => {
            panic!("{:?}", e)
        }
    };
	let mut raw_string = String::new();
    file.read_to_string(&mut raw_string).unwrap();
    let config = raw_string.parse::<Value>().unwrap();

	/* File manager loading */
	let fm = load_file_manager(&config);
	let fm_comp = load_file_manager_comp(&config);

	/* Buffer loading */
	let (buf_option, compre_buf_option) = load_buffer(&config, fm, fm_comp);

	/* Client loading */
	let mut signals: Vec<Box<(dyn Future<Item=Option<SystemTime>,Error=()> + Send + Sync)>> = Vec::new();
	let mut remote_clients: HashMap<u64, Sender<T>, RandomState> = HashMap::new();
	match load_clients(&config, &mut signals, &mut remote_clients, &buf_option) {
		// testdict
		None => (),
		d => {
			let mut kernel = Kernel::new(d.clone().unwrap(),1,4,30);
			kernel.rbfdict_pre_process();
		}
	};

	/* Dispatcher loading*/
	remote_clients.shrink_to_fit();
	let mut dispatchers: Vec<ZMQDispatcher<T>> = load_dispatchers(&config, remote_clients);

	//let mut comps:Vec<CompressionDemon<_,DB,_>> = Vec::new();
	let batch = 20;

	/* Construct compression threads */
	let mut comp_handlers = Vec::new();
	for x in 0..num_comp {
		match comp{
			"paa" => {
				let mut compress_demon:CompressionDemon<_,DB,_> = CompressionDemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,PAACompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run compression demon" );
					compress_demon.run();
					println!("segment commpressed: {}", compress_demon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"fourier" => {
				let mut compress_demon: CompressionDemon<_, DB, _> = CompressionDemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, FourierCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run compression demon" );
					compress_demon.run();
					println!("segment commpressed: {}", compress_demon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"snappy" => {
				let mut compress_demon: CompressionDemon<_, DB, _> = CompressionDemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SnappyCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run compression demon" );
					compress_demon.run();
					println!("segment commpressed: {}", compress_demon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gzip" => {
				let mut compress_demon:CompressionDemon<_,DB,_> = CompressionDemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,GZipCompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run compression demon" );
					compress_demon.run();
					println!("segment commpressed: {}", compress_demon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			_ => {panic!("Compression not supported yet.")}
		}
	}

	/* Construct the runtime */
	let rt = match config.get("runtime") {
		None => Builder::new()
			.after_start(|| println!("Threads have been constructed"))
			.build()
			.expect("Failed to produce a default runtime"),

		Some(value) => {
			let core_threads = value.get("core_threads")
				.expect("Core threads field required by custom runtime")
				.as_integer()
				.expect("Core threads should be provided as an integer") as usize;

			let blocking_threads = value.get("blocking_threads")
				.expect("Blocking threads field required by custom runtime")
				.as_integer()
				.expect("Blocking threads should be provided as an integer") as usize;
			
			Builder::new()
				.core_threads(core_threads)
				.blocking_threads(blocking_threads)
				.after_start(|| println!("Threads have been constructed"))
				.build()
				.expect("Failed to produce the custom runtime")
		}
	};

	/* Use constructed runtime to create signals */
	let executor = rt.executor();
	let mut spawn_handles: Vec<oneshot::SpawnHandle<Option<SystemTime>,()>> = Vec::new();
	for sig in signals {
		spawn_handles.push(oneshot::spawn(sig, &executor))
	}

	/* Construct dispatcher threads */
	let mut dispatcher_handles: Vec<thread::JoinHandle<_>> = Vec::with_capacity(dispatchers.len());
	let mut dispatcher_continue: Vec<Arc<AtomicBool>> = Vec::with_capacity(dispatchers.len()); // TODO mutable needed?
	while dispatchers.len() > 0 {
		let mut dispatcher= dispatchers.pop().unwrap();
		let dispatcher_cont_t = Arc::new(AtomicBool::new(true));
		dispatcher_continue.push(Arc::clone(&dispatcher_cont_t));
		dispatcher_handles.push(thread::spawn(move || {
			&dispatcher.run(dispatcher_cont_t);
		}))
	}

	/* Wait for the future signals to finish. */
	for sh in spawn_handles {
		match sh.wait() {
			Ok(Some(x)) => println!("Produced a timestamp: {:?}", x),
			_ => println!("Failed to produce a timestamp"),
		}
	}
	/* Signals have all joined, so tell dispatcher to stop running */
	for end_flag in dispatcher_continue {
		end_flag.store(false, Ordering::Release);
	}
	/* Join dispatcher threads */
	for dispatcher_handle in dispatcher_handles {
		dispatcher_handle.join().unwrap();
	}
	
	// Wait until the runtime becomes idle and shut it down.
	match rt.shutdown_on_idle().wait() {
		Ok(_) => (),
		Err(_) => panic!("Failed to shutdown properly"),
	}

}




/*
 *
 * Helper functions for loading configuration files 
 * 
 */

/* Load the configurations for the file manager to be used */
fn load_file_manager(config: &Value) -> Option<Box<rocksdb::DB>>{
	/* Construct the file manager to be used */
	match config.get("file_handler") {
		Some (config) => {
			let fm_type = config.get("file_manager").expect("A file manager must be provided");
			match fm_type.as_str().expect("A file manager must be provided as a string") {
				"Rocks" => {
					let params = config.get("params").expect("A RocksDB file manager requires parameters");
					let path = params.get("path").expect("RocksDB requires a path be provided").as_str().expect("Rocks file path must be provided as string");
					let mut db_opts = rocksdb::Options::default();
					db_opts.create_if_missing(true);
					match rocksdb::DB::open(&db_opts, path) {
						Ok(x) => Some(Box::new(x)),
						Err(e) => panic!("Failed to create RocksFM object: {:?}", e),
					}
				}
				x => panic!("File manager type, {:?}, not supported yet", x),
			}
		}
		None => None,
	}
}

/* Load the configurations for the file manager for compression to be used */
fn load_file_manager_comp(config: &Value) -> Option<Box<rocksdb::DB>> {
	match config.get("file_handler") {
		Some (config) => {
			let fm_type = config.get("file_manager").expect("A file manager must be provided");
			match fm_type.as_str().expect("A file manager must be provided as a string") {
				"Rocks" => {
					let params = config.get("params").expect("A RocksDB file manager requires parameters");
					let path = params.get("path").expect("RocksDB requires a path be provided").as_str().expect("Rocks file path must be provided as string");
					let mut comp_path = String::from(path);
					comp_path.push_str("comp");
					let new_path = comp_path.as_str();
					let mut db_opts = rocksdb::Options::default();
					db_opts.create_if_missing(true);
					match rocksdb::DB::open(&db_opts, new_path) {
						Ok(x) => Some(Box::new(x)),
						Err(e) => panic!("Failed to create RocksFM object: {:?}", e),
					}
				}
				x => panic!("File manager type, {:?}, not supported yet", x),
			}
		}
		None => None,
	}
}


/* Load the configurations for the buffer to be used */
fn load_buffer<T: 'static>(config: &Value, fm: Option<Box<rocksdb::DB>>, fm_comp: Option<Box<rocksdb::DB>>)
	-> (Option<Box<Arc<Mutex<(dyn SegmentBuffer<T> + Send + Sync)>>>>, Option<Box<Arc<Mutex<(dyn SegmentBuffer<T> + Send + Sync)>>>>)
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + Debug + FFTnum + Float + Lapack + FromStr + From<f32>,
{
	let buffer_size = match config.get("buffer") {
		Some(value) => value.get("buffer_size").map_or(DEFAULT_BUF_SIZE, |v| v.as_integer().expect("The buffer size should be provided as an integer") as usize),
		None => DEFAULT_BUF_SIZE,
	};
	let buf_option: Option<Box<Arc<Mutex<(dyn SegmentBuffer<T> + Send + Sync)>>>> = match fm {
		Some(fm) => {
			match config.get("buffer") {
				Some(config) => {
					let buf_type = config.get("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<T,rocksdb::DB>::new(buffer_size,*fm))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run with a file manager", x),
					}
				}
				None => None,
			}
		}
		None => {
			match config.get("buffer") {
				Some(config) => {
					let buf_type = config.get("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as a string") {
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<T>::new(buffer_size))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run without a file manager", x),
					}
				}
				None => None,
			}
		}
	};

	/* Create buffer for compression segments*/
	let compre_buf_option: Option<Box<Arc<Mutex<(dyn SegmentBuffer<T> + Send + Sync)>>>> = match fm_comp {
		Some(fm) => {
			match config.get("buffer") {
				Some(config) => {
					let buf_type = config.get("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<T,rocksdb::DB>::new(buffer_size,*fm))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run with a file manager", x),
					}
				}
				None => None,
			}
		}
		None => {
			match config.get("buffer") {
				Some(config) => {
					let buf_type = config.get("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as a string") {
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<T>::new(buffer_size))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run without a file manager", x),
					}
				}
				None => None,
			}
		}
	};

	(buf_option, compre_buf_option)
}

struct ClientCommonConfig {
	signal_id: u64,
	amount: Amount,
	run_period: RunPeriod,
	frequency: Frequency
}

/* Loads client configuration common for all client types */
fn load_common_client_configs(client_config: &Value, rng: &mut ThreadRng) -> ClientCommonConfig {
	let signal_id = match client_config.get("id") {
		Some(x) => x.as_integer().expect("If an ID for a client is provided it must be supplied as an integer") as u64,
		None => rng.gen()
	};

	let amount = match client_config.get("amount") {
		Some(value) => Amount::Limited (value.as_integer().expect("The client amount argument must be specified as an integer") as u64),
		None => Amount::Unlimited,
	};

	let run_period = match client_config.get("run_period") {
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
				panic!("The run period was provided a value of 0 for both secs and nano_secs. This is not allowed as the signal will start and immediately exit");
			}

			RunPeriod::Finite(Duration::new(secs,nano_secs))
		}
		None => RunPeriod::Indefinite,
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
		run_period: run_period,
		frequency: frequency
	}
}


/*
 * Loads clients from the given config toml::Value
 * Returns the testdict; the signals and the dispatchers have their reference editted
 */
fn load_clients<T>(config: &Value,
	signals: &mut Vec<Box<(dyn Future<Item=Option<SystemTime>,Error=()> + Send + Sync)>>,
	remote_clients: &mut HashMap<u64, Sender<T>, RandomState>,
	buf_option: &Option<Box<Arc<Mutex<(dyn SegmentBuffer<T> + Send + Sync)>>>>,)
	-> Option<Array2<T>>
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + Debug + FFTnum + Float + Lapack + FromStr + From<f32>,
{
	/* Get segment size */
	let seg_size = config
		.get("segment_size")
		.expect("A segment size must be provided")
		.as_integer()
		.expect("The segment size argument must be provided as an integer") as usize;
	
	/* Construct the clients */
	let mut rng = thread_rng();
	let mut testdict = None;
	for client_config in config.get("clients")
		.expect("At least one client must be provided")
		.as_table()
		.expect("The clients must be provided as a TOML table")
		.values()
	{
		// Common configuration
		let cc = load_common_client_configs(&client_config, &mut rng);
		let (signal_id, amount, run_period, frequency) = (cc.signal_id, cc.amount, cc.run_period, cc.frequency);

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

				let dict = match params.get("dict") {
					Some(value) => {
						let dict_str = value.as_str().expect("The file dictionary file must be privded as a string");
						let dic = read_dict::<T>(dict_str,delim);
						println!("dictionary shape: {} * {}", dic.rows(), dic.cols());
						Some(dic)
					},
					None => None,
				};

				testdict = dict.clone();

				let client: Box<(dyn Stream<Item=T,Error=()> + Sync + Send)> = match reader_type {
					"NewlineAndSkip" => {

						let skip_val = match params.get("skip") {
							Some(skip_val) => skip_val.as_integer().expect("The skip value must be provided as an integer") as usize,
							None => 0,
						};
						Box::new(construct_file_client_skip_newline::<T>(path, skip_val, delim, amount, run_period, frequency).expect("Client could not be properly produced"))
					}
					"DeserializeDelim" => Box::new(construct_file_client::<T>(path, delim as u8, amount, run_period, frequency).expect("Client could not be properly produced")),
					x => panic!("The specified file reader, {:?}, is not supported yet", x),
				};

				match &buf_option {
					Some(buf) => signals.push(Box::new(BufferedSignal::new(signal_id, client, seg_size, *buf.clone(), |i,j| i >= j, |_| (), false,dict))),
					None => panic!("Buffer and File manager provided not supported yet"),
				}
			}
			"gen" => {
				if amount == Amount::Unlimited && run_period == RunPeriod::Indefinite {
					if !client_config.get("never_die").map_or(false,|v| v.as_bool().expect("The never_die field must be provided as a boolean")) {
						panic!("Provided a generator client that does have an amount or time bound\n
							This client would run indefintely and the program would not terminate\n
							If this is what you want, then create the never_die field under this client and set the value to true");
					}
				}
				let params = client_config.get("params").expect("The generator client type requires a params table");
				let client: Box<(dyn Stream<Item=T,Error=()> + Sync + Send)> = match client_config.get("gen_type")
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

							Box::new(construct_normal_gen_client(mean, std, amount, run_period, frequency))
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

							Box::new(construct_gen_client::<f32,Uniform<f32>,T>(dist, amount, run_period, frequency))
						}
						x => panic!("The provided generator type, {:?}, is not currently supported", x),
					};
				match &buf_option {
					Some(buf) => signals.push(Box::new(BufferedSignal::new(signal_id, client, seg_size, *buf.clone(), |i,j| i >= j, |_| (), false, None))),
					None => panic!("Buffer and File manager provided not supported yet"),
				}
			}
			"remote" => {
				// Overriding previous reading of signal ID as we cannot use randomly generated signal ID
				let remote_signal_id = client_config.get("id")
					.expect("Signal ID required for remote clients")
					.as_integer()
					.expect("If an ID for a client is provided it must be supplied as an integer") as u64;
				let buffer = match client_config.get("params") {
					Some(p) => {
						match p.get("buffer_size") {
							Some(size) => {
								// TODO make unwrap display something meaningful (like saying "buffer size too large")
								usize::try_from(size.as_integer().expect("Buffer size must be provided as an integer")).unwrap()
							}
							None => DEFAULT_MPSC_BUF_SIZE, // Default buffer size
						}
					}
					None => DEFAULT_MPSC_BUF_SIZE, // Default buffer size
				};

				let (mut sender, mut receiver) = channel::<T>(buffer);
				//let remote_stream = Box::new(remote_stream_from_receiver::<T>(receiver, amount, run_period));
				remote_clients.insert(signal_id, sender);
				
				match &buf_option {
					Some(buf) => signals.push(Box::new(BufferedSignal::new(remote_signal_id, receiver, seg_size, *buf.clone(), |i,j| i >= j, |_| (), false, None))),
					None => panic!("Buffer and File manager provided not supported yet"),
				}
			}
			x => panic!("The provided type, {:?}, is not currently supported", x),
		}
	}
	testdict
}

fn load_dispatchers<T>(config: &Value, remote_clients: HashMap<u64, Sender<T>, RandomState>) -> Vec<ZMQDispatcher<T>>
	where T: Copy + Send + Sync + Serialize + DeserializeOwned + Debug + FromStr + From<f32>,
{
	let mut rng = thread_rng();
	let mut dispatchers: Vec<ZMQDispatcher<T>> = Vec::new();
	if remote_clients.len() > 0 {
		let ports: HashSet<u16> = HashSet::with_capacity(remote_clients.len());
		for dispatcher_config in config.get("dispatchers")
			.expect("Dispatcher must be provided if there is a remote client")
			.as_table()
			.expect("The clients must be provided as a TOML table").values()
		{
			let dis_id = match dispatcher_config.get("id") {
				Some(id) => id.as_integer().expect("If an ID for a client is provided it must be supplied as an integer") as u64,
				None => rng.gen(),
			};
			let port = dispatcher_config.get("port")
							.expect("The port to receive data from must be specified")
							.as_integer()
							.expect("The port must be provided as an integer") as u16;
			
			if ports.contains(&port) {
				println!("Duplicate port {:?}, dispatcher {:?} will be skipped", port, dis_id);
			} else {
				dispatchers.push(ZMQDispatcher::new(dis_id, remote_clients.clone(), port));
			}
		}
		if dispatchers.is_empty() {
			panic!("At least one dispatcher must be provided if there is a remote client");
		}
	}
	dispatchers
}