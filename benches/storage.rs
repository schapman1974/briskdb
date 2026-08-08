mod support;

use std::{hint::black_box, time::Duration};

use criterion::{Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use support::{
    BENCHMARK_SHARDS, BenchmarkFixture, EngineBenchmarkFixture, engine_benchmark_runtime,
};

fn storage_benchmarks(criterion: &mut Criterion) {
    let read_fixture = BenchmarkFixture::new().expect("initialize point-read benchmark");
    assert_eq!(
        read_fixture
            .point_read()
            .expect("preflight point read")
            .len(),
        1
    );
    assert_eq!(read_fixture.write_count(0).expect("preflight read row"), 0);
    for (shard, key) in read_fixture.keys_by_shard().iter().enumerate() {
        assert_eq!(usize::from(read_fixture.shard_for_key(key)), shard);
    }

    let write_fixture = BenchmarkFixture::new().expect("initialize point-write benchmark");
    assert_eq!(
        write_fixture.point_write().expect("preflight point write"),
        1
    );
    assert_eq!(
        write_fixture.write_count(0).expect("preflight written row"),
        1
    );

    let concurrent_fixture =
        BenchmarkFixture::new().expect("initialize concurrent-write benchmark");
    assert_eq!(
        concurrent_fixture
            .four_shard_concurrent_write_wave()
            .expect("preflight concurrent writes"),
        [1; BENCHMARK_SHARDS as usize]
    );
    for shard in 0..BENCHMARK_SHARDS as usize {
        assert_eq!(
            concurrent_fixture
                .write_count(shard)
                .expect("preflight concurrent row"),
            1
        );
    }

    let mut group = criterion.benchmark_group("storage");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    group.throughput(Throughput::Elements(1));
    group.bench_function("point_read", |bencher| {
        bencher.iter(|| {
            let rows = read_fixture.point_read().expect("benchmark point read");
            assert_eq!(rows.len(), 1);
            black_box(rows)
        });
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("point_write", |bencher| {
        bencher.iter(|| {
            let affected = write_fixture.point_write().expect("benchmark point write");
            assert_eq!(affected, 1);
            black_box(affected)
        });
    });

    group.throughput(Throughput::Elements(u64::from(BENCHMARK_SHARDS)));
    group.bench_function("four_shard_concurrent_writes", |bencher| {
        bencher.iter(|| {
            let affected = concurrent_fixture
                .four_shard_concurrent_write_wave()
                .expect("benchmark concurrent writes");
            assert_eq!(affected, [1; BENCHMARK_SHARDS as usize]);
            black_box(affected)
        });
    });

    group.finish();
}

fn engine_benchmarks(criterion: &mut Criterion) {
    let runtime = engine_benchmark_runtime().expect("initialize engine benchmark runtime");

    let read_fixture =
        EngineBenchmarkFixture::new(&runtime).expect("initialize engine point-read benchmark");
    assert_eq!(
        runtime
            .block_on(read_fixture.point_read())
            .expect("preflight engine point read")
            .len(),
        1
    );
    assert_eq!(
        runtime
            .block_on(read_fixture.write_count(0))
            .expect("preflight engine read row"),
        0
    );
    for (shard, key) in read_fixture.keys_by_shard().iter().enumerate() {
        assert_eq!(usize::from(read_fixture.shard_for_key(key)), shard);
    }

    let write_fixture =
        EngineBenchmarkFixture::new(&runtime).expect("initialize engine point-write benchmark");
    assert_eq!(
        runtime
            .block_on(write_fixture.point_write())
            .expect("preflight engine point write"),
        1
    );
    assert_eq!(
        runtime
            .block_on(write_fixture.write_count(0))
            .expect("preflight engine written row"),
        1
    );

    let concurrent_fixture = EngineBenchmarkFixture::new(&runtime)
        .expect("initialize engine concurrent-write benchmark");
    assert_eq!(
        runtime
            .block_on(concurrent_fixture.four_shard_concurrent_write_wave())
            .expect("preflight engine concurrent writes"),
        [1; BENCHMARK_SHARDS as usize]
    );
    for shard in 0..BENCHMARK_SHARDS as usize {
        assert_eq!(
            runtime
                .block_on(concurrent_fixture.write_count(shard))
                .expect("preflight engine concurrent row"),
            1
        );
    }

    let mut group = criterion.benchmark_group("engine");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    group.throughput(Throughput::Elements(1));
    group.bench_function("point_read", |bencher| {
        bencher.iter(|| {
            let rows = runtime
                .block_on(read_fixture.point_read())
                .expect("benchmark engine point read");
            assert_eq!(rows.len(), 1);
            black_box(rows)
        });
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("point_write", |bencher| {
        bencher.iter(|| {
            let affected = runtime
                .block_on(write_fixture.point_write())
                .expect("benchmark engine point write");
            assert_eq!(affected, 1);
            black_box(affected)
        });
    });

    group.throughput(Throughput::Elements(u64::from(BENCHMARK_SHARDS)));
    group.bench_function("four_shard_concurrent_writes", |bencher| {
        bencher.iter(|| {
            let affected = runtime
                .block_on(concurrent_fixture.four_shard_concurrent_write_wave())
                .expect("benchmark engine concurrent writes");
            assert_eq!(affected, [1; BENCHMARK_SHARDS as usize]);
            black_box(affected)
        });
    });

    group.finish();
}

criterion_group!(benches, storage_benchmarks, engine_benchmarks);
criterion_main!(benches);
