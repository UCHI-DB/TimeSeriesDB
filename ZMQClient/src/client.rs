use tokio::time::Interval;

use futures::stream::{Stream, iter};
use futures::task::{Context, Poll};
use core::pin::Pin;

use std::marker::PhantomData;
use serde::de::DeserializeOwned;

use std::io::{BufReader,BufRead};
use std::str::FromStr;
use std::fs::File;

use rand::distributions::*;
use rand::prelude::*;

/* use ndarray::{Array2}; */

#[derive(PartialEq)]
pub enum Amount {
	Limited (u64),
	Unlimited,
}

pub enum Frequency {
	Immediate,
	Delayed(Interval),
}


pub struct Client<T,U> 
	where T: Stream<Item=U>
{
	producer: T,
	amount: Amount,
	frequency: Frequency,
	produced: u64,
}

pub fn client_from_stream<T,U>(producer: T, amount: Amount, frequency: Frequency)
			    -> impl Stream<Item=U>
	where T: Stream<Item=U> + Unpin,
{
	Client { 
		producer: producer,
		amount: amount,
		frequency: frequency,
		produced: 0,
	}
}

pub fn client_from_iter<T,U>(producer: T, amount: Amount, frequency: Frequency)
				    -> impl Stream<Item=U>
	where T: Iterator<Item=U>
{
	Client { 
		producer: iter(producer),
		amount: amount,
		frequency: frequency,
		produced: 0,
	}
}


impl<T: futures::Stream,U> Stream for Client<T,U> 
	where T: Stream<Item=U> + Unpin
{
	type Item = U;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		/* Terminate stream if hit max production */
		if let Amount::Limited(max_items) = self.amount {
			if self.produced >= max_items { return Poll::Ready(None) }
		}

		/* Either poll to determine if enough time has passed or
		 * immediately get the value depending on Frequency Mode
		 * Must call poll on the stream within the client
		 */
		let this_client = self.get_mut();
		match this_client.frequency {
			Frequency::Immediate => {
				match Pin::new(&mut this_client.producer).poll_next(cx) {
					Poll::Ready(value) => {
						this_client.produced += 1;
						Poll::Ready(value)
					}
					x => { x }
				}
			}
			Frequency::Delayed(ref mut interval) => {
				match Stream::poll_next(Pin::new(interval), cx) {
					Poll::Pending => Poll::Pending,
					Poll::Ready(_) =>  {
						match Pin::new(&mut this_client.producer).poll_next(cx) {
							Poll::Ready(value) => {
								this_client.produced += 1;
								Poll::Ready(value)
							}
							x => { x }
						}
					}
				}
			}
		}
	}
}




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



pub fn construct_file_client<T>(file: &str, delim: u8, amount: Amount, frequency: Frequency)
						 -> Result<impl Stream<Item=T>,()> 
	where T: DeserializeOwned
{
	let producer = construct_file_iterator::<T>(file, delim)?;
	Ok(client_from_iter(producer, amount, frequency))
}

/* An example of how to combine an iterator and IterClient constructor */
pub fn construct_file_client_skip_newline<T>(file: &str, skip_val: usize, delim: char, amount: Amount, frequency: Frequency)
				    	 -> Result<impl Stream<Item=T>,()>
	where T: FromStr,
{
	let producer = construct_file_iterator_skip_newline::<T>(file, skip_val, delim)?;
	Ok(client_from_iter(producer, amount, frequency))
}


/* First approach at enabling a framework for random generation 
 * Failed because f32 does not implement From<f64>
 * This lack of implementation prevents coverting f64 values 
 * which is the only type supported by rusts normal distribution
 * So this framework won't work for a f32 database setting with a 
 * normal distribution generator client
 */
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

pub fn construct_gen_client<T,U,V>(dist: U, 
		amount: Amount, frequency: Frequency) 
			-> impl Stream<Item=V>
		where V: From<T>,
			  U: Distribution<T>,

{
	let producer = BasicItemGenerator::new(dist);
	client_from_iter(producer, amount, frequency)
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

pub fn construct_normal_gen_client<T>(mean: f64, std: f64, amount: Amount, frequency: Frequency)
		-> impl Stream<Item=T> 
	where T: From<f32>,
{
	let producer = NormalDistItemGenerator::<T>::new(mean,std);
	client_from_iter(producer, amount, frequency)
}


/* pub fn read_dict<T>(filename: &str, delim: char) -> Array2<T>
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
} */