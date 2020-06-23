#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate futures;
extern crate toml_loader;

use zmq::Message;

use std::thread::sleep;
use std::time::Duration;

use rand::random;

use tokio::prelude::*;
use std::str::FromStr;
use std::fmt::Debug;
use serde::Serialize;
use serde::de::DeserializeOwned;
use bincode::serialize;

use toml_loader::{Loader};
use std::path::Path;

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
 * 1) Get client.rs and load_config() working together! Implement reading files/generating data
 * 2) Detect connection drop
 * 3) Issue in the main DB, but protocol for handling sent data limit
 * 4) Do something useful with initial "acknowledgement"
 * 5) Rust technicals: how to do proper array initialization (without doing [0; 5]), proper array returning
 */ 
pub fn run_client<T>(config_file: &str) where T: Copy + Send + Sync + Serialize + DeserializeOwned + FromStr + From<f32> + Debug
{
    let context = zmq::Context::new();
    let requester = context.socket(zmq::DEALER).unwrap();
    
    let config = load_config(config_file);

    requester.set_identity(&config.id).unwrap();

    // Attempt to connect every five seconds
    let five_sec = Duration::from_secs(5);
    loop {
        match requester.connect(format!("{}://{}:{}", config.protocol, config.address, config.port).as_str()) {
            Ok(_) => { break; }
            Err(_) => { sleep(five_sec);}
        }
    }
    
    // TODO Do something with this
    // TODO Make sure it doesn't hang
    requester.send(Message::from("hello"), 0).unwrap();
    let mut msg = zmq::Message::new();
    requester.recv(&mut msg, 0).unwrap();
    match msg.as_str() {
        Some(s) => { println!("{}", s) }
        _ => ()
    }
 
    // Serialize and send data
    // TODO Handling serialized data size
    let mut msg: [u8; 128] = [0; 128];
    loop {
        let data = get_data::<T>();
        let serialized = serialize(&data).unwrap();

        requester.send(&serialized, 0).unwrap();
        sleep(five_sec);
    }
}

fn get_data<T>() -> Vec<T>
    where T: Copy + Send + Sync + Serialize + DeserializeOwned + From<f32> + Debug
{
    // TODO get something from file/gen
    let numbers: Vec<T> = vec![T::from(50.555), T::from(2.1), T::from(3.2), T::from(4.3)];
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
