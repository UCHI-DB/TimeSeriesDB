use criterion::{black_box, criterion_group, criterion_main, Criterion};
use criterion::BenchmarkId;
use time_series_start::{run_test, run_single_test, bench_test};



pub fn criterion_benchmark(c: &mut Criterion) {

    let mut group = c.benchmark_group("criterion_benchmark");
    for delay_micros in [1, 10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(delay_micros), delay_micros, |b, &delay_micros| {
            // bench_test(amount: u64, num_clients: u64, num_comp:i32, delay_micros: u64)
            b.iter(|| bench_test(30000,1,1,delay_micros));
        });
    }
    group.finish();
}



criterion_group!(name = benches; config = Criterion::default().measurement_time(std::time::Duration::from_secs(200)).sample_size(10); targets = criterion_benchmark);
criterion_main!(benches);
