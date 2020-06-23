use std::marker::PhantomData;
use serde::de::DeserializeOwned;
use futures::stream::iter_ok;
use std::str::FromStr;
use std::fs::File;
use std::io::{BufReader,BufRead};
use rand::distributions::*;
use rand::prelude::*;

use tokio::prelude::*;


use ndarray::{Array1, Array2};


// Implements a Client struct with trait Stream
// Basically a stripped version of client.rs in the main DB
// Can be replaced by the actual client.rs with minimal adjustments

#[derive(PartialEq)]
pub enum Amount {
	Limited (u64),
	Unlimited,
}

pub struct Client<T,U> 
	where T: Stream<Item=U,Error=()>
{
	producer: T,
	amount: Amount,
	produced: Option<u64>,
}

impl<T,U> Stream for Client<T,U> 
	where T: Stream<Item=U,Error=()>
{
	type Item = U;
	type Error = ();

	fn poll(&mut self) -> Poll<Option<U>,()> {
		/* Terminate stream if hit max production */
		if let Amount::Limited(max_items) = self.amount {
			if let Some(items) = self.produced {
				if items >= max_items { return Ok(Async::Ready(None)) }
			}
		}
		let poll_val = try_ready!(self.producer.poll());
		Ok(Async::Ready(poll_val))
	}
}


pub fn client_from_iter<T,U>(producer: T, amount: Amount) -> impl Stream<Item=U,Error=()>
	where T: Iterator<Item=U>
{
	let produced = match amount {
		Amount::Limited(_) => Some(0),
		Amount::Unlimited  => None,
	};

	Client { 
		producer:iter_ok(producer),
		amount: amount,
		produced: produced
	}
}

/* File Clients */

fn construct_file_iterator<T>(file: &str, delim: u8) -> Result<impl Iterator<Item=T>,()> 
	where T: DeserializeOwned
{
	let f = match File::open(file) {
		Ok(f) => f,
		Err(_) => return Err(()),
	};

	Ok(BufReader::new(f)
		.split(delim)
		.filter_map(|x| match x {
			Ok(val) => bincode::deserialize(&val).ok(),
			_ => None
		})
	)
}

/* Must use type annotation on function to declare what to 
 * parse CSV entries as 
 */
pub fn construct_file_iterator_skip_newline<T>(file: &str, skip_val: usize, delim: char) -> Result<impl Iterator<Item=T>,()>
	where T: FromStr
{
	let f = match File::open(file) {
		Ok(f) => f,
		Err(_) => return Err(()),
	};

	Ok(BufReader::new(f)
		.lines()
		.filter_map(Result::ok)
		.flat_map(move |line: String| {
			line.split(delim)
				.skip(skip_val)
				.filter_map(|item: &str| item.parse::<T>().ok())
				.collect::<Vec<T>>()
				.into_iter()
		})
	)
}

pub fn construct_file_iterator_int(file: &str, skip_val: usize, delim: char, scl:i32) -> Result<impl Iterator<Item=u32>,()>
{
	let f = match File::open(file) {
		Ok(f) => f,
		Err(_) => return Err(()),
	};

	Ok(BufReader::new(f)
		.lines()
		.filter_map(Result::ok)
		.flat_map(move |line: String| {
			line.split(delim)
				.skip(skip_val)
				.filter_map(|item: &str| item.parse::<f32>().ok())
				.map(|x| (x*scl as f32).ceil().abs() as u32)
				.collect::<Vec<u32>>()
				.into_iter()
		})
	)
}

pub fn construct_file_iterator_int_signed(file: &str, skip_val: usize, delim: char, scl:i32) -> Result<impl Iterator<Item=i32>,()>
{
	let f = match File::open(file) {
		Ok(f) => f,
		Err(_) => return Err(()),
	};

	Ok(BufReader::new(f)
		.lines()
		.filter_map(Result::ok)
		.flat_map(move |line: String| {
			line.split(delim)
				.skip(skip_val)
				.filter_map(|item: &str| item.parse::<f32>().ok())
				.map(|x| (x*scl as f32).ceil() as i32)
				.collect::<Vec<i32>>()
				.into_iter()
		})
	)
}

pub fn construct_file_client<T>(file: &str, delim: u8, amount: Amount)
						 -> Result<impl Stream<Item=T,Error=()>,()> 
	where T: DeserializeOwned + FromStr
{
	let producer = construct_file_iterator::<T>(file, delim)?;
	Ok(client_from_iter(producer, amount))
}

pub fn construct_file_client_skip_newline<T>(file: &str, skip_val: usize, delim: char, amount: Amount)
				    	 -> Result<impl Stream<Item=T,Error=()>,()>
	where T: FromStr + DeserializeOwned
{
	let producer = construct_file_iterator_skip_newline::<T>(file, skip_val, delim)?;
	Ok(client_from_iter(producer, amount))
}




/* Gen Client */

pub fn construct_gen_client<T,U,V>(dist: U, amount: Amount) 
		-> impl Stream<Item=V,Error=()>
	where V: From<T>,
		  U: Distribution<T>,
{
	let producer = BasicItemGenerator::new(dist);
	client_from_iter(producer, amount)
}


pub struct BasicItemGenerator<T,U,V>
	where V: From<T>,
		  U: Distribution<T>,
{
	rng: SmallRng,
	dist: U,
	phantom1: PhantomData<T>,
	phantom2: PhantomData<V>,
}

impl<T,U,V> BasicItemGenerator<T,U,V> 
	where V: From<T>,
		  U: Distribution<T>,
{
	pub fn new(dist: U) -> BasicItemGenerator<T,U,V> {
		BasicItemGenerator {
			rng: SmallRng::from_entropy(),
			dist: dist,
			phantom1: PhantomData,
			phantom2: PhantomData,
		}
	}
}

impl<T,U,V> Iterator for BasicItemGenerator<T,U,V> 
	where V: From<T>,
		  U: Distribution<T>,
{
	type Item = V;

	fn next(&mut self) -> Option<V> {
		Some(self.dist.sample(&mut self.rng).into())
	}
}


pub struct NormalDistItemGenerator<V>
	where V: From<f32>,
{
	rng: SmallRng,
	dist: Normal,
	phantom: PhantomData<V>,
}

impl<V> NormalDistItemGenerator<V> 
	where V: From<f32>,
{
	pub fn new(mean: f64, std: f64) -> NormalDistItemGenerator<V> {
		NormalDistItemGenerator {
			rng: SmallRng::from_entropy(),
			dist: Normal::new(mean,std),
			phantom: PhantomData,
		}
	}
}

impl<V> Iterator for NormalDistItemGenerator<V> 
	where V: From<f32>,
{
	type Item = V;

	fn next(&mut self) -> Option<V> {
		Some((self.dist.sample(&mut self.rng) as f32).into())
	}
}



pub fn construct_normal_gen_client<T>(mean: f64, std: f64, amount: Amount)
		-> impl Stream<Item=T,Error=()> 
	where T: From<f32>,
{
	let producer = NormalDistItemGenerator::<T>::new(mean,std);
	client_from_iter(producer, amount)
}


pub fn read_dict<T>(filename: &str, delim: char) -> Array2<T>
	where T: FromStr{
	//let filename = "src/main.rs";
	// Open the file in read-only mode (ignoring errors).
	let file = File::open(filename).unwrap();
	let reader = BufReader::new(file);
	let mut vec = Vec::new();
	let mut llen:usize = 0;

	// Read the file line by line using the lines() iterator from std::io::BufRead.
	for (index, line) in reader.lines().enumerate() {
		let line = line.unwrap(); // Ignore errors.
		let mut elms = line.split(delim)
			.filter_map(|item: &str| item.parse::<T>().ok())
			.collect::<Vec<T>>();

		llen = elms.len();
		vec.append(&mut elms);
		// Show the line and its number.
		//println!("{}. {}", index + 1, line);
	}
	let size = vec.len()/llen;
	Array2::from_shape_vec((size,llen),vec).unwrap()
}