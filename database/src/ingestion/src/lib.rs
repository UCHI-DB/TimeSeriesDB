#[macro_use] extern crate futures;
#[macro_use] extern crate queues;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate itertools;
#[macro_use] extern crate ndarray;
extern crate toml_loader;
#[macro_use] extern crate serde_derive;
extern crate bincode;
extern crate ndarray_linalg;
#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

use rand::prelude::*;
use rand::distributions::Uniform;
use time_series_start::client::{construct_normal_gen_client, read_dict};
use time_series_start::client::construct_gen_client;
use std::time::SystemTime;
use time_series_start::client::construct_file_client;
use time_series_start::segment::{ FourierCompress, PAACompress};
use std::str::FromStr;
use std::fmt::Debug;
use ndarray::Array2;
use rustfft::FFTnum;
use num::Float;
use time_series_start::compression_daemon::CompressionDaemon;
use std::{fs, thread};
use time_series_start::kernel::Kernel;
use time_series_start::methods::compress::{GZipCompress, ZlibCompress, DeflateCompress, SnappyCompress, CompressionMethod};
use time_series_start::methods::Methods::Fourier;
use time_series_start::methods::gorilla_encoder::GorillaEncoder;
use time_series_start::compress::gorilla::GorillaCompress;
use time_series_start::compress::sprintz::SprintzDoubleCompress;

use time_series_start::methods::prec_double::{PrecisionBound, get_precision_bound};
use time_series_start::knn::fft_ifft_ratio;
use time_series_start::client::{construct_file_client_skip_newline,Amount,RunPeriod,Frequency};
use time_series_start::buffer_pool::{SegmentBuffer,ClockBuffer,LRUBuffer, NoFmClockBuffer};
use time_series_start::future_signal::{BufferedSignal};
use toml_loader::{Loader};
use rocksdb::{DB};
use ndarray_linalg::Lapack;
use std::path::Path;
use std::time::{Duration,Instant};
use tokio::timer::Interval;
use tokio::prelude::*;
use tokio::runtime::{Builder};
use futures::sync::oneshot;
use std::sync::{Arc,Mutex};
use serde::Serialize;
use time_series_start::compress::split_double::SplitBDDoubleCompress;
use time_series_start::MABcompression_daemon::MABCompressionDaemon;
use time_series_start::MABrecoding_daemon::MABRecodingDaemon;
use time_series_start::methods::Methods;
use time_series_start::recoding_daemon::RecodingDaemon;


const DEFAULT_BUF_SIZE: usize = 150;
const DEFAULT_DELIM: char = '\n';

pub fn run_single_test(config_file: &str, task:&str, comp:&str, recode:&str, num_comp:i32, num_recode: i32){

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

	let buffer_budget = match config.lookup("comp_buffer") {
		Some(value) => value.lookup("buffer_budget").map_or(DEFAULT_BUF_SIZE, |v| v.as_integer().expect("The buffer size should be provided as an integer") as usize),
		None => DEFAULT_BUF_SIZE,
	};

	let buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<f64> + Send + Sync)>>>> = match fm {
		Some(fm) => {
			match config.lookup("buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<f64,rocksdb::DB>::new(buffer_size,*fm))))),
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
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<f64>::new(buffer_size))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run without a file manager", x),
					}
				}
				None => None,
			}
		}
	};

	/* Create buffer for compression segments*/
	let compre_buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<f64> + Send + Sync)>>>> = match fm_comp {
		Some(fm) => {
			match config.lookup("comp_buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<f64,rocksdb::DB>::new(buffer_size,*fm))))),
						"LRU" => Some(Box::new(Arc::new(Mutex::new(LRUBuffer::<f64,rocksdb::DB>::new(buffer_budget,*fm, task))))),
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
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<f64>::new(buffer_size))))),
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
							let dic = read_dict::<f64>(dict_str,delim);
							println!("dictionary shape: {} * {}", dic.rows(), dic.cols());
							Some(dic)
						},
						None => None,
					};

					testdict = dict.clone();

					let client: Box<(Stream<Item=f64,Error=()> + Sync + Send)> = match reader_type {
						"NewlineAndSkip" => {

							let skip_val = match params.lookup("skip") {
								Some(skip_val) => skip_val.as_integer().expect("The skip value must be provided as an integer") as usize,
								None => 0,
							};
							Box::new(construct_file_client_skip_newline::<f64>(path, skip_val, delim, amount, run_period, frequency).expect("Client could not be properly produced"))
						}
						"DeserializeDelim" => Box::new(construct_file_client::<f64>(path, delim as u8, amount, run_period, frequency).expect("Client could not be properly produced")),
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
					let client: Box<(dyn Stream<Item=f64,Error=()> + Sync + Send)> = match client_config.lookup("gen_type")
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

								Box::new(construct_gen_client::<f32,_,f64>(dist, amount, run_period, frequency))
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


	let batch = 5;



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

	let mut comp_handlers = Vec::new();
	let recoding = match recode{
		"paa" => {Methods::Paa(1)},
		"fft" => {Methods::Fourier(1.0)},
		"pla" => {Methods::Pla(1.0)},
		"rrd" => {Methods::Rrd_sample},
		"bufflossy" => {Methods::Bufflossy(10000, 32)},
		_ => {Methods::Uncompr}
	};

	for _x in 0..num_comp {
		match comp{
			"paa" => {
				let mut compress_daemon:CompressionDaemon<_,DB,_> = CompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,PAACompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run paa compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"buff" => {
				let mut compress_daemon:CompressionDaemon<_,DB,_> = CompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,SplitBDDoubleCompress::new(10,batch, 10000));
				let handle = thread::spawn(move || {
					println!("Run buff compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"fourier" => {
				let mut compress_daemon: CompressionDaemon<_, DB, _> = CompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, FourierCompress::new(10, batch,1.0));
				let handle = thread::spawn(move || {
					println!("Run fourier compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"snappy" => {
				let mut compress_daemon: CompressionDaemon<_, DB, _> = CompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SnappyCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run snappy compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gorilla" => {
				let mut compress_daemon: CompressionDaemon<_, DB, _> = CompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, GorillaCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run gotilla compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"sprintz" => {
				let mut compress_daemon: CompressionDaemon<_, DB, _> = CompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SprintzDoubleCompress::new(10, batch,10000));
				let handle = thread::spawn(move || {
					println!("Run sprintz compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gzip" => {
				let mut compress_daemon:CompressionDaemon<_,DB,_> = CompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,GZipCompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run gzip compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}

			"kernel" => {
				let mut knl = Kernel::new(array![[1.0, 1.0],[1.0, 1.0]],1,4,30);
				if testdict != None{
					knl = Kernel::new(testdict.clone().unwrap(),1,4,30);
					knl.rbfdict_pre_process();
				}
				let mut compress_daemon:CompressionDaemon<_,DB,_> = CompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,knl);
				let handle = thread::spawn(move || {
					println!("Run kernel compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			_ => {panic!("Compression not supported yet.")}
		}

	}

	for _x in 0..num_recode {
		let rec_name = recoding.clone();
		let mut rec:RecodingDaemon<_,DB> = RecodingDaemon::new(*(compre_buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.8,0.8,batch, recoding.clone());
		let handle = thread::spawn(move || {
			println!("Run recoding demon with lossy {}", rec_name );
			rec.run();
			println!("segment recoded: {}", rec.get_processed() );
		});
		comp_handlers.push(handle);
	}


	let executor = rt.executor();

	let mut spawn_handles: Vec<oneshot::SpawnHandle<Option<SystemTime>,()>> = Vec::new();

	for sig in signals {
		spawn_handles.push(oneshot::spawn(sig, &executor))
	}

	for _comp in comp_handlers {
		// comp.join().unwrap();
	}


	// wait the future to finish.
	for sh in spawn_handles {
		match sh.wait() {
			Ok(Some(x)) => println!("Produced a timestamp: {:?}", x),
			_ => println!("Failed to produce a timestamp"),
		}
	}

	// Wait until the runtime becomes idle and shut it down.
	match rt.shutdown_on_idle().wait() {
		Ok(_) => (),
		Err(_) => panic!("Failed to shutdown properly"),
	}

}

pub fn run_mab_test(config_file: &str, task:&str, comp:&str, recode:&str, num_comp:i32, num_recode: i32){

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

	let buffer_budget = match config.lookup("comp_buffer") {
		Some(value) => value.lookup("buffer_budget").map_or(DEFAULT_BUF_SIZE, |v| v.as_integer().expect("The buffer size should be provided as an integer") as usize),
		None => DEFAULT_BUF_SIZE,
	};

	let buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<f64> + Send + Sync)>>>> = match fm {
		Some(fm) => {
			match config.lookup("buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<f64,rocksdb::DB>::new(buffer_size,*fm))))),
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
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<f64>::new(buffer_size))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run without a file manager", x),
					}
				}
				None => None,
			}
		}
	};

	/* Create buffer for compression segments*/
	let compre_buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<f64> + Send + Sync)>>>> = match fm_comp {
		Some(fm) => {
			match config.lookup("comp_buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<f64,rocksdb::DB>::new(buffer_size,*fm))))),
						"LRU" => Some(Box::new(Arc::new(Mutex::new(LRUBuffer::<f64,rocksdb::DB>::new(buffer_budget,*fm,task))))),
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
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<f64>::new(buffer_size))))),
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
						let dic = read_dict::<f64>(dict_str,delim);
						println!("dictionary shape: {} * {}", dic.rows(), dic.cols());
						Some(dic)
					},
					None => None,
				};

				testdict = dict.clone();

				let client: Box<(Stream<Item=f64,Error=()> + Sync + Send)> = match reader_type {
					"NewlineAndSkip" => {

						let skip_val = match params.lookup("skip") {
							Some(skip_val) => skip_val.as_integer().expect("The skip value must be provided as an integer") as usize,
							None => 0,
						};
						Box::new(construct_file_client_skip_newline::<f64>(path, skip_val, delim, amount, run_period, frequency).expect("Client could not be properly produced"))
					}
					"DeserializeDelim" => Box::new(construct_file_client::<f64>(path, delim as u8, amount, run_period, frequency).expect("Client could not be properly produced")),
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
				let client: Box<(dyn Stream<Item=f64,Error=()> + Sync + Send)> = match client_config.lookup("gen_type")
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

						Box::new(construct_gen_client::<f32,_,f64>(dist, amount, run_period, frequency))
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


	let batch = 5;



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

	let mut comp_handlers = Vec::new();
	let recoding = match recode{
		"paa" => {Methods::Paa(1)},
		"fft" => {Methods::Fourier(1.0)},
		"pla" => {Methods::Pla(1.0)},
		"rrd" => {Methods::Rrd_sample},
		"bufflossy" => {Methods::Bufflossy(10000, 32)},
		_ => {Methods::Uncompr}
	};

	for _x in 0..num_comp {
		println!("Init MAB compression demon {}", _x);
		match comp{
			"paa" => {
				let mut compress_daemon:MABCompressionDaemon<_,DB,_> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,PAACompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run paa compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"buff" => {
				let mut compress_daemon:MABCompressionDaemon<_,DB,_> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,SplitBDDoubleCompress::new(10,batch, 10000));
				let handle = thread::spawn(move || {
					println!("Run buff compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"fourier" => {
				let mut compress_daemon: MABCompressionDaemon<_, DB, _> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, FourierCompress::new(10, batch,1.0));
				let handle = thread::spawn(move || {
					println!("Run fourier compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"snappy" => {
				let mut compress_daemon: MABCompressionDaemon<_, DB, _> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SnappyCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run snappy compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gorilla" => {
				let mut compress_daemon: MABCompressionDaemon<_, DB, _> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, GorillaCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run gotilla compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"sprintz" => {
				let mut compress_daemon: MABCompressionDaemon<_, DB, _> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SprintzDoubleCompress::new(10, batch,10000));
				let handle = thread::spawn(move || {
					println!("Run sprintz compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gzip" => {
				let mut compress_daemon:MABCompressionDaemon<_,DB,_> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,GZipCompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run gzip compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}

			"kernel" => {
				let mut knl = Kernel::new(array![[1.0, 1.0],[1.0, 1.0]],1,4,30);
				if testdict != None{
					knl = Kernel::new(testdict.clone().unwrap(),1,4,30);
					knl.rbfdict_pre_process();
				}
				let mut compress_daemon:MABCompressionDaemon<_,DB,_> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,knl);
				let handle = thread::spawn(move || {
					println!("Run kernel compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			_ => {panic!("Compression not supported yet.")}
		}

	}

	for _x in 0..num_recode {
		let rec_name = recoding.clone();
		let mut rec:MABRecodingDaemon<_,DB> = MABRecodingDaemon::new(*(compre_buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.8,0.8,batch, recoding.clone());
		let handle = thread::spawn(move || {
			println!("Run recoding demon with lossy {}", rec_name );
			rec.run();
			println!("segment recoded: {}", rec.get_processed() );
		});
		comp_handlers.push(handle);
	}


	let executor = rt.executor();

	let mut spawn_handles: Vec<oneshot::SpawnHandle<Option<SystemTime>,()>> = Vec::new();

	for sig in signals {
		spawn_handles.push(oneshot::spawn(sig, &executor))
	}

	for _comp in comp_handlers {
		// comp.join().unwrap();
	}


	// wait the future to finish.
	for sh in spawn_handles {
		match sh.wait() {
			Ok(Some(x)) => println!("Produced a timestamp: {:?}", x),
			_ => println!("Failed to produce a timestamp"),
		}
	}

	// Wait until the runtime becomes idle and shut it down.
	match rt.shutdown_on_idle().wait() {
		Ok(_) => (),
		Err(_) => panic!("Failed to shutdown properly"),
	}

}

pub fn run_online_mab_test(config_file: &str, task:&str, comp:&str, recode:&str, num_comp:i32, num_recode: i32, tcr:f64){

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

	let buffer_budget = match config.lookup("comp_buffer") {
		Some(value) => value.lookup("buffer_budget").map_or(DEFAULT_BUF_SIZE, |v| v.as_integer().expect("The buffer size should be provided as an integer") as usize),
		None => DEFAULT_BUF_SIZE,
	};

	let buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<f64> + Send + Sync)>>>> = match fm {
		Some(fm) => {
			match config.lookup("buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<f64,rocksdb::DB>::new(buffer_size,*fm))))),
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
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<f64>::new(buffer_size))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run without a file manager", x),
					}
				}
				None => None,
			}
		}
	};

	/* Create buffer for compression segments*/
	let compre_buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<f64> + Send + Sync)>>>> = match fm_comp {
		Some(fm) => {
			match config.lookup("comp_buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<f64,rocksdb::DB>::new(buffer_size,*fm))))),
						"LRU" => Some(Box::new(Arc::new(Mutex::new(LRUBuffer::<f64,rocksdb::DB>::new(buffer_budget,*fm,task))))),
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
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<f64>::new(buffer_size))))),
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
						let dic = read_dict::<f64>(dict_str,delim);
						println!("dictionary shape: {} * {}", dic.rows(), dic.cols());
						Some(dic)
					},
					None => None,
				};

				testdict = dict.clone();

				let client: Box<(Stream<Item=f64,Error=()> + Sync + Send)> = match reader_type {
					"NewlineAndSkip" => {

						let skip_val = match params.lookup("skip") {
							Some(skip_val) => skip_val.as_integer().expect("The skip value must be provided as an integer") as usize,
							None => 0,
						};
						Box::new(construct_file_client_skip_newline::<f64>(path, skip_val, delim, amount, run_period, frequency).expect("Client could not be properly produced"))
					}
					"DeserializeDelim" => Box::new(construct_file_client::<f64>(path, delim as u8, amount, run_period, frequency).expect("Client could not be properly produced")),
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
				let client: Box<(dyn Stream<Item=f64,Error=()> + Sync + Send)> = match client_config.lookup("gen_type")
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

						Box::new(construct_gen_client::<f32,_,f64>(dist, amount, run_period, frequency))
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


	let batch = 5;



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

	let mut comp_handlers = Vec::new();
	let recoding = match recode{
		"paa" => {Methods::Paa(1)},
		"fft" => {Methods::Fourier(1.0)},
		"pla" => {Methods::Pla(1.0)},
		"rrd" => {Methods::Rrd_sample},
		"bufflossy" => {Methods::Bufflossy(10000, 32)},
		_ => {Methods::Uncompr}
	};

	for _x in 0..num_comp {
		println!("Init MAB compression demon {}", _x);
		match comp{
			"paa" => {
				let mut compress_daemon:MABCompressionDaemon<_,DB,_> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,PAACompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run paa compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"buff" => {
				let mut compress_daemon:MABCompressionDaemon<_,DB,_> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,SplitBDDoubleCompress::new(10,batch, 10000));
				let handle = thread::spawn(move || {
					println!("Run buff compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"fourier" => {
				let mut compress_daemon: MABCompressionDaemon<_, DB, _> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, FourierCompress::new(10, batch,1.0));
				let handle = thread::spawn(move || {
					println!("Run fourier compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"snappy" => {
				let mut compress_daemon: MABCompressionDaemon<_, DB, _> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SnappyCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run snappy compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gorilla" => {
				let mut compress_daemon: MABCompressionDaemon<_, DB, _> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, GorillaCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run gotilla compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"sprintz" => {
				let mut compress_daemon: MABCompressionDaemon<_, DB, _> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SprintzDoubleCompress::new(10, batch,10000));
				let handle = thread::spawn(move || {
					println!("Run sprintz compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gzip" => {
				let mut compress_daemon:MABCompressionDaemon<_,DB,_> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,GZipCompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run gzip compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}

			"kernel" => {
				let mut knl = Kernel::new(array![[1.0, 1.0],[1.0, 1.0]],1,4,30);
				if testdict != None{
					knl = Kernel::new(testdict.clone().unwrap(),1,4,30);
					knl.rbfdict_pre_process();
				}
				let mut compress_daemon:MABCompressionDaemon<_,DB,_> = MABCompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,knl);
				let handle = thread::spawn(move || {
					println!("Run kernel compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			_ => {panic!("Compression not supported yet.")}
		}

	}

	for _x in 0..num_recode {
		let rec_name = recoding.clone();
		let mut rec:MABRecodingDaemon<_,DB> = MABRecodingDaemon::new(*(compre_buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.8,0.8,batch, recoding.clone());
		rec.set_targetCR(tcr);
		let handle = thread::spawn(move || {
			println!("Run recoding demon with lossy {}", rec_name );
			rec.run();
			println!("segment recoded: {}", rec.get_processed() );
		});
		comp_handlers.push(handle);
	}


	let executor = rt.executor();

	let mut spawn_handles: Vec<oneshot::SpawnHandle<Option<SystemTime>,()>> = Vec::new();

	for sig in signals {
		spawn_handles.push(oneshot::spawn(sig, &executor))
	}

	for _comp in comp_handlers {
		// comp.join().unwrap();
	}


	// wait the future to finish.
	for sh in spawn_handles {
		match sh.wait() {
			Ok(Some(x)) => println!("Produced a timestamp: {:?}", x),
			_ => println!("Failed to produce a timestamp"),
		}
	}

	// Wait until the runtime becomes idle and shut it down.
	match rt.shutdown_on_idle().wait() {
		Ok(_) => (),
		Err(_) => panic!("Failed to shutdown properly"),
	}

}

pub fn run_online_test(config_file: &str, task:&str, comp:&str, recode:&str, num_comp:i32, num_recode: i32, tcr:f64)
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

	let buffer_budget = match config.lookup("comp_buffer") {
		Some(value) => value.lookup("buffer_budget").map_or(DEFAULT_BUF_SIZE, |v| v.as_integer().expect("The buffer size should be provided as an integer") as usize),
		None => DEFAULT_BUF_SIZE,
	};

	let buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<f64> + Send + Sync)>>>> = match fm {
		Some(fm) => {
			match config.lookup("buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<f64,rocksdb::DB>::new(buffer_size,*fm))))),
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
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<f64>::new(buffer_size))))),
						x => panic!("The buffer type, {:?}, is not currently supported to run without a file manager", x),
					}
				}
				None => None,
			}
		}
	};

	/* Create buffer for compression segments*/
	let compre_buf_option: Option<Box<Arc<Mutex<(SegmentBuffer<f64> + Send + Sync)>>>> = match fm_comp {
		Some(fm) => {
			match config.lookup("comp_buffer") {
				Some(config) => {
					let buf_type = config.lookup("type").expect("A buffer type must be provided");
					match buf_type.as_str().expect("Buffer type must be provided as string") {
						"Clock" => Some(Box::new(Arc::new(Mutex::new(ClockBuffer::<f64,rocksdb::DB>::new(buffer_size,*fm))))),
						"LRU" => Some(Box::new(Arc::new(Mutex::new(LRUBuffer::<f64,rocksdb::DB>::new(buffer_budget,*fm, task))))),
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
						"NoFmClock" => Some(Box::new(Arc::new(Mutex::new(NoFmClockBuffer::<f64>::new(buffer_size))))),
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
						let dic = read_dict::<f64>(dict_str,delim);
						println!("dictionary shape: {} * {}", dic.rows(), dic.cols());
						Some(dic)
					},
					None => None,
				};

				testdict = dict.clone();

				let client: Box<(Stream<Item=f64,Error=()> + Sync + Send)> = match reader_type {
					"NewlineAndSkip" => {

						let skip_val = match params.lookup("skip") {
							Some(skip_val) => skip_val.as_integer().expect("The skip value must be provided as an integer") as usize,
							None => 0,
						};
						Box::new(construct_file_client_skip_newline::<f64>(path, skip_val, delim, amount, run_period, frequency).expect("Client could not be properly produced"))
					}
					"DeserializeDelim" => Box::new(construct_file_client::<f64>(path, delim as u8, amount, run_period, frequency).expect("Client could not be properly produced")),
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
				let client: Box<(dyn Stream<Item=f64,Error=()> + Sync + Send)> = match client_config.lookup("gen_type")
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

						Box::new(construct_gen_client::<f32,_,f64>(dist, amount, run_period, frequency))
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


	let batch = 5;



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

	let mut comp_handlers = Vec::new();
	let recoding = match recode{
		"paa" => {Methods::Paa(1)},
		"fft" => {Methods::Fourier(1.0)},
		"pla" => {Methods::Pla(1.0)},
		"rrd" => {Methods::Rrd_sample},
		"bufflossy" => {Methods::Bufflossy(10000, 32)},
		_ => {Methods::Uncompr}
	};

	for _x in 0..num_comp {
		match comp{
			"paa" => {
				let mut compress_daemon:CompressionDaemon<_,DB,_> = CompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,PAACompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run paa compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"buff" => {
				let mut compress_daemon:CompressionDaemon<_,DB,_> = CompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,SplitBDDoubleCompress::new(10,batch, 10000));
				let handle = thread::spawn(move || {
					println!("Run buff compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			},
			"fourier" => {
				let mut compress_daemon: CompressionDaemon<_, DB, _> = CompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, FourierCompress::new(10, batch,1.0));
				let handle = thread::spawn(move || {
					println!("Run fourier compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"snappy" => {
				let mut compress_daemon: CompressionDaemon<_, DB, _> = CompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SnappyCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run snappy compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gorilla" => {
				let mut compress_daemon: CompressionDaemon<_, DB, _> = CompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, GorillaCompress::new(10, batch));
				let handle = thread::spawn(move || {
					println!("Run gotilla compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"sprintz" => {
				let mut compress_daemon: CompressionDaemon<_, DB, _> = CompressionDaemon::new(*(buf_option.clone().unwrap()), *(compre_buf_option.clone().unwrap()), None, 0.1, 0.0, SprintzDoubleCompress::new(10, batch,10000));
				let handle = thread::spawn(move || {
					println!("Run sprintz compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			"gzip" => {
				let mut compress_daemon:CompressionDaemon<_,DB,_> = CompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,GZipCompress::new(10,batch));
				let handle = thread::spawn(move || {
					println!("Run gzip compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}

			"kernel" => {
				let mut knl = Kernel::new(array![[1.0, 1.0],[1.0, 1.0]],1,4,30);
				if testdict != None{
					knl = Kernel::new(testdict.clone().unwrap(),1,4,30);
					knl.rbfdict_pre_process();
				}
				let mut compress_daemon:CompressionDaemon<_,DB,_> = CompressionDaemon::new(*(buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.1,0.0,knl);
				let handle = thread::spawn(move || {
					println!("Run kernel compression demon");
					compress_daemon.run();
					println!("segment commpressed: {}", compress_daemon.get_processed() );
				});
				comp_handlers.push(handle);
			}
			_ => {panic!("Compression not supported yet.")}
		}

	}

	for _x in 0..num_recode {
		let rec_name = recoding.clone();
		let mut rec:RecodingDaemon<_,DB> = RecodingDaemon::new(*(compre_buf_option.clone().unwrap()),*(compre_buf_option.clone().unwrap()),None,0.01,0.01,batch, recoding.clone());
		rec.set_targetCR(tcr);
		let handle = thread::spawn(move || {
			println!("Run recoding demon with lossy {}", rec_name );
			rec.run();
			println!("segment recoded: {}", rec.get_processed() );
		});
		comp_handlers.push(handle);
	}


	let executor = rt.executor();

	let mut spawn_handles: Vec<oneshot::SpawnHandle<Option<SystemTime>,()>> = Vec::new();

	for sig in signals {
		spawn_handles.push(oneshot::spawn(sig, &executor))
	}

	for _comp in comp_handlers {
		// comp.join().unwrap();
	}


	// wait the future to finish.
	for sh in spawn_handles {
		match sh.wait() {
			Ok(Some(x)) => println!("Produced a timestamp: {:?}", x),
			_ => println!("Failed to produce a timestamp"),
		}
	}

	// Wait until the runtime becomes idle and shut it down.
	match rt.shutdown_on_idle().wait() {
		Ok(_) => (),
		Err(_) => panic!("Failed to shutdown properly"),
	}

}


