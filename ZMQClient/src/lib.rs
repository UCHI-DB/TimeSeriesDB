#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate futures;
extern crate toml_loader;

use zmq::{Message, Socket};

use std::thread::sleep;
use std::time::Duration;

use rand::random;

use tokio::prelude::*;
use std::str::FromStr;
use std::fmt::Debug;
use serde::Serialize;
use serde::de::DeserializeOwned;
use bincode::{deserialize, serialize};

use toml_loader::{Loader};
use std::path::Path;

use rand::rngs::{ThreadRng};
use rand::{Rng, SeedableRng, StdRng, thread_rng};
use rand::distributions::Uniform;

mod client;
use crate::client::{construct_normal_gen_client, read_dict};
use crate::client::construct_gen_client;
use crate::client::construct_file_client;
use crate::client::construct_file_client_skip_newline;
use crate::client::{Client,Amount};

const DEFAULT_DELIM: char = '\n';

pub struct Config
{
    protocol: String,
    address: String,
    port: u16,
    id: [u8; 5],
    //data: dyn Stream<Item = T, Error = ()>
}

/* TODO
 * 1) Detect connection drop
 * 2) Issue in the main DB, but protocol for handling sent data limit
 * 3) Do something useful with initial "acknowledgement"
 */ 
pub fn run_client(config_file: &str) {
    
    let config = load_config(config_file);

    let context = zmq::Context::new();
    let requester = context.socket(zmq::DEALER).unwrap();
    requester.set_identity(&config.id).unwrap();

    // Attempt to connect every five seconds
    let five_sec = Duration::from_secs(5);
    loop {
        match requester.connect(format!("{}://{}:{}", config.protocol, config.address, config.port).as_str()) {
            Ok(_) => { break; }
            Err(_) => { sleep(five_sec);}
        }
    }
    
    // TODO Can loop to repeatedly send dummy data until server sends proper type configuration
    requester.send(Message::from(""), 0).unwrap();
    let mut msg = Message::new();
    let init_msg_size = match requester.recv(&mut msg, 0) {
        Ok(size) => size,
        Err(_e) => {
            panic!("Error: Failed to receive inital message from server");
        }
    };
    // End loop
    match msg.as_str() {
        Some(format_type) => {
            match format_type {
                "f32" => {
                    println!("Connected to server, type f32");
                    serialized_and_send::<f32>(requester);
                }
                "f64" => {
                    println!("Connected to server, type f64");
                    serialized_and_send::<f64>(requester);
                }
                _ => {
                    panic!("Error: received wrong data type");
                }
            }
        }
        None => {
            panic!("Error: Failed to receive inital message from server");
        }
        
    }
}

fn serialized_and_send<T>(requester: Socket) where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug {
    let five_sec = Duration::from_secs(5);
    let mut i: u64 = 0;
    let seed = thread_rng().gen::<u64>();
    loop {
        let data = get_random_data::<T>(seed, i as f32);
        let serialized = serialize(&data).unwrap();
        requester.send(&serialized, 0).unwrap();
        println!("Sent {:?}", data);
        i += 1;
        sleep(five_sec);
    }
}

fn get_data<T>() -> Vec<(u64, T)>
    where T: Copy + Send + Sync + Serialize + DeserializeOwned + From<f32> + Debug
{
    // TODO get something from file/gen
    let numbers: Vec<(u64, T)> = vec![(10, T::from(50.555)), (11, T::from(2.1)), (12, T::from(3.2)), (13, T::from(4.3))];
    numbers
}

// For testing
fn get_random_data<T>(seed: u64, i: f32) -> Vec<(u64, T)>
    where T: Copy + Send + Sync + Serialize + DeserializeOwned + From<f32> + Debug
{
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);
    let numbers: Vec<(u64, T)> = vec![(10, T::from(rng.gen_range::<f32, f32, f32>(0.0, 100.0) + i)), (11, T::from(rng.gen_range::<f32, f32, f32>(0.0, 100.0) + i)),
                                        (12, T::from(rng.gen_range::<f32, f32, f32>(0.0, 100.0) + i)), (13, T::from(rng.gen_range::<f32, f32, f32>(0.0, 100.0) + i))];
    numbers
}



pub fn load_config(config_file: &str) -> Config
{
    // Read from the client settings
    let config = match Loader::from_file(Path::new(config_file)) {
		Ok(config) => config,
		Err(e) => panic!("{:?}", e),
	};
    
    let protocol = match config.lookup("protocol") {
        Some(value) => {
            match value.as_str().expect("Protocol must be provided as a string") {
                "tcp" => "tcp",
                _ => panic!("Unsupported protocol")
            }
        }
        None => "tcp"
    };

    let addr = config.lookup("address")
                        .expect("Address must be provided")
                        .as_str()
                        .expect("Address must be provided as a string");

    let port = match config.lookup("port") {
        Some(value) => {
            let tmp = value.as_integer().expect("Port number must be specified as an integer") as u16;
            if tmp == 0 {
                panic!("Port number must not be 0");
            }
            tmp
        }
        None => panic!("Port number must be specified"),
    };

    
    let id = match config.lookup("id") {
        Some(value) => {
            let arr = value.as_slice().expect("The ID must be specified as an array of 5 integers");
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
    }
    /*
    let amount = match config.lookup("amount") {
        Some(value) => Amount::Limited (value.as_integer().expect("The client amount argument must be specified as an integer") as u64),
        None => Amount::Unlimited,
    };
    */
}
