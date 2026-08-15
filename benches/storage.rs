mod support;

use std::{cell::Cell, hint::black_box, time::Duration};

use briskdb::core::{
    CanonicalIndexKey, Database, GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource,
    GlobalIndexKeyType, GlobalIndexOwner, GlobalIndexStorageTopology, GlobalOperationId,
    GlobalUniqueMutation, ShardKeyMetadata, ShardKeyType, TableDeclaration, UniqueNullSemantics,
    Value,
};

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

fn global_authority_benchmarks(criterion: &mut Criterion) {
    let root = tempfile::tempdir().expect("create global-authority benchmark root");
    let mut database = Database::open(root.path(), 4).expect("open benchmark database");
    database
        .broadcast(
            "CREATE TABLE authority_benchmark (
                 tenant_id TEXT NOT NULL,
                 global_value INTEGER NOT NULL,
                 PRIMARY KEY (tenant_id, global_value)
             ) STRICT",
        )
        .expect("create authority benchmark table");
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "authority_benchmark",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text)
                    .expect("declare benchmark shard key"),
            )
            .expect("declare benchmark table"),
        ])
        .expect("register benchmark table");
    let table = database
        .catalog()
        .table("default", "authority_benchmark")
        .expect("read benchmark catalog")
        .expect("find benchmark table")
        .id();
    let index_id = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "authority_benchmark_value_unique",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("global_value")
                        .expect("declare benchmark index column"),
                    GlobalIndexKeyType::Int64,
                )],
            )
            .expect("declare benchmark index")
            .unique(UniqueNullSemantics::NotDistinct)
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .expect("create benchmark global index");
    database
        .build_global_index(index_id)
        .expect("build benchmark global index");
    let hot_key =
        CanonicalIndexKey::encode_values(&[Value::from(0_i64)]).expect("encode benchmark hot key");
    let hot_owner =
        GlobalIndexOwner::new(0, b"hot-owner".to_vec()).expect("construct benchmark hot owner");
    let hot_operation =
        GlobalOperationId::new(1_u128.to_le_bytes()).expect("construct benchmark hot operation");
    database
        .reserve_global_unique(
            hot_operation,
            &GlobalUniqueMutation::claim(index_id, hot_key.clone(), hot_owner),
        )
        .expect("reserve benchmark hot key");
    database
        .finalize_global_unique(hot_operation)
        .expect("finalize benchmark hot key");

    let next_operation = Cell::new(2_u128);
    let mut group = criterion.benchmark_group("global_authority");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.bench_function("unique_claim_finalize_uncontended", |bencher| {
        bencher.iter(|| {
            let value = next_operation.get();
            next_operation.set(value + 1);
            let operation = GlobalOperationId::new(value.to_le_bytes()).unwrap();
            let key = CanonicalIndexKey::encode_values(&[Value::from(value as i64)]).unwrap();
            let owner = GlobalIndexOwner::new(0, value.to_le_bytes().to_vec()).unwrap();
            let reservation = database
                .reserve_global_unique(
                    operation,
                    &GlobalUniqueMutation::claim(index_id, key, owner),
                )
                .unwrap();
            let finalized = database.finalize_global_unique(operation).unwrap();
            black_box((reservation, finalized))
        });
    });
    group.bench_function("unique_hot_key_rejection", |bencher| {
        bencher.iter(|| {
            let value = next_operation.get();
            next_operation.set(value + 1);
            let operation = GlobalOperationId::new(value.to_le_bytes()).unwrap();
            let owner = GlobalIndexOwner::new(1, value.to_le_bytes().to_vec()).unwrap();
            black_box(
                database
                    .reserve_global_unique(
                        operation,
                        &GlobalUniqueMutation::claim(index_id, hot_key.clone(), owner),
                    )
                    .unwrap_err(),
            )
        });
    });
    group.throughput(Throughput::Elements(64));
    group.bench_function("value_lease_64_finalize", |bencher| {
        bencher.iter(|| {
            let value = next_operation.get();
            next_operation.set(value + 1);
            let operation = GlobalOperationId::new(value.to_le_bytes()).unwrap();
            let lease = database
                .lease_global_values(operation, index_id, 64)
                .unwrap();
            let finalized = database.finalize_global_value_lease(operation).unwrap();
            black_box((lease, finalized))
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    storage_benchmarks,
    engine_benchmarks,
    global_authority_benchmarks
);
criterion_main!(benches);
