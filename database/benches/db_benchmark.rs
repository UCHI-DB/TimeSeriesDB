use criterion::{black_box, criterion_group, criterion_main, Criterion};
use time_series_start::{run_test, run_single_test};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("default test", |b| b.iter(|| run_single_test::<f64>("bench_config.toml","paa",1)));
}

criterion_group!(name = benches; config = Criterion::default().measurement_time(std::time::Duration::from_secs(10000)).sample_size(10); targets = criterion_benchmark);
criterion_main!(benches);
