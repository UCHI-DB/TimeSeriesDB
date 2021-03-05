use criterion::{black_box, criterion_group, criterion_main, Criterion};
use criterion::BenchmarkId;
use time_series_start::{run_test, run_single_test, bench_test};



pub fn benchmark_comptime(c: &mut Criterion) {

    let mut group = c.benchmark_group("Compression time benchmark");
    let mut v = [0, 100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600];
    v.reverse();
    for delay_micros in v.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(delay_micros), delay_micros, |b, &delay_micros| {
            // bench_test(amount: u64, num_clients: u64, num_comp:i32, delay_micros: u64)
            b.iter(|| bench_test(10000000,1,1,delay_micros));
        });
    }
    group.finish();
}


pub fn benchmark_comptime_short(c: &mut Criterion) {

    let mut group = c.benchmark_group("Compression time benchmark short");
    let mut v = [0, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 200000, 400000, 600000, 800000, 1000000, 1100000, 1200000, 1225000, 1250000, 1275000, 1300000, 1350000, 1400000, 1450000, 1500000, 1550000, 1600000, 1800000, 2000000];
    v.reverse();
    for delay_nanos in v.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(delay_nanos), delay_nanos, |b, &delay_nanos| {
            // bench_test(amount: u64, num_clients: u64, num_comp:i32, delay_nanos: u64)
            b.iter(|| bench_test(10000000,1,1,delay_nanos));
        });
    }
    group.finish();
}

pub fn benchmark_comptime_4c(c: &mut Criterion) {
    let num_clients = 4;
    let mut group = c.benchmark_group("Compression time benchmark short - 4 Signal");
    let mut v = [0, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 200000, 225000, 250000, 275000, 300000, 325000, 350000, 375000, 400000, 500000, 600000, 800000, 1000000, 1200000, 1400000, 1600000, 1800000];
    v.reverse();
    for delay_nanos in v.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(delay_nanos), delay_nanos, |b, &delay_nanos| {
            // bench_test(amount: u64, num_clients: u64, num_comp:i32, delay_nanos: u64)
            b.iter(|| bench_test(10000000/num_clients,num_clients,1,delay_nanos));
        });
    }
    group.finish();
}


pub fn benchmark_comptime_2c(c: &mut Criterion) {
    let num_clients = 2;
    let mut group = c.benchmark_group("Compression time benchmark short - 2 Signal");
    let mut v = [0, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 200000, 400000, 600000, 625000, 650000, 675000, 700000, 725000, 750000, 775000, 800000, 900000, 1000000, 1200000, 1400000, 1600000, 1800000];
    v.reverse();
    for delay_nanos in v.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(delay_nanos), delay_nanos, |b, &delay_nanos| {
            // bench_test(amount: u64, num_clients: u64, num_comp:i32, delay_nanos: u64)
            b.iter(|| bench_test(10000000/num_clients,num_clients,1,delay_nanos));
        });
    }
    group.finish();
}


pub fn benchmark_comptime_8c(c: &mut Criterion) {
    let num_clients = 8;
    let mut group = c.benchmark_group("Compression time benchmark short - 8 Signal");
    let mut v = [0, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 125000, 150000, 175000, 200000, 225000, 250000, 275000, 300000, 325000, 350000, 400000, 600000, 800000, 1000000, 1200000, 1400000, 1600000];
    v.reverse();
    for delay_nanos in v.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(delay_nanos), delay_nanos, |b, &delay_nanos| {
            // bench_test(amount: u64, num_clients: u64, num_comp:i32, delay_nanos: u64)
            b.iter(|| bench_test(10000000/num_clients,num_clients,1,delay_nanos));
        });
    }
    group.finish();
}


pub fn benchmark_comptime_8c_ext(c: &mut Criterion) {
    let num_clients = 8;
    let mut group = c.benchmark_group("Compression time benchmark short - 8 Signal, longer");
    let mut v = [0, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 125000, 150000, 175000, 200000, 225000, 250000, 275000, 300000, 325000, 350000, 400000, 600000, 800000, 1000000, 1200000, 1400000, 1600000];
    v.reverse();
    for delay_nanos in v.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(delay_nanos), delay_nanos, |b, &delay_nanos| {
            // bench_test(amount: u64, num_clients: u64, num_comp:i32, delay_nanos: u64)
            b.iter(|| bench_test(10000000,num_clients,1,delay_nanos));
        });
    }
    group.finish();
}

pub fn benchmark_clients(c: &mut Criterion) {
    let delay_micros = 12800;
    let mut group = c.benchmark_group("Client number benchmark");
    for client in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(client), client, |b, &client| {
            // bench_test(amount: u64, num_clients: u64, num_comp:i32, delay_micros: u64)
            b.iter(|| bench_test(10000000/client, client, 1, delay_micros));
        });
    }
    group.finish();
}


criterion_group!(name = benches; config = Criterion::default().sample_size(50).measurement_time(std::time::Duration::from_secs(180)); targets = benchmark_comptime_short, benchmark_comptime_2c, benchmark_comptime_4c, benchmark_comptime_8c, benchmark_comptime_8c_ext);
criterion_main!(benches);
