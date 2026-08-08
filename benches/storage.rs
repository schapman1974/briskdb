mod support;

use std::{hint::black_box, time::Duration};

use criterion::{Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use support::{BENCHMARK_SHARDS, BenchmarkFixture};

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

criterion_group!(benches, storage_benchmarks);
criterion_main!(benches);
