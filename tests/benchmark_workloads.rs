#[path = "../benches/support/mod.rs"]
mod support;

use briskdb::core::{Column, DataType, Row, Value};
use support::{
    BENCHMARK_SHARDS, BenchmarkFixture, EngineBenchmarkFixture, engine_benchmark_runtime,
};

#[test]
fn point_read_returns_the_seeded_row() {
    let fixture = BenchmarkFixture::new().unwrap();

    let result = fixture.point_read().unwrap();

    assert_eq!(
        result.columns(),
        vec![
            Column::new("id", DataType::Unknown),
            Column::new("writes", DataType::Unknown),
            Column::new("payload", DataType::Unknown),
        ]
    );
    assert_eq!(
        result.rows(),
        vec![Row::new(vec![
            Value::from(fixture.keys_by_shard()[0].clone()),
            Value::from(0_i64),
            Value::from("baseline payload"),
        ])]
    );
}

#[test]
fn point_write_updates_exactly_one_row() {
    let fixture = BenchmarkFixture::new().unwrap();

    assert_eq!(fixture.point_write().unwrap(), 1);
    assert_eq!(fixture.point_write().unwrap(), 1);

    assert_eq!(fixture.write_count(0).unwrap(), 2);
}

#[test]
fn concurrent_write_wave_updates_one_key_on_every_shard() {
    let fixture = BenchmarkFixture::new().unwrap();
    for (shard, key) in fixture.keys_by_shard().iter().enumerate() {
        assert_eq!(usize::from(fixture.shard_for_key(key)), shard);
    }

    assert_eq!(
        fixture.four_shard_concurrent_write_wave().unwrap(),
        [1; BENCHMARK_SHARDS as usize]
    );
    assert_eq!(
        fixture.four_shard_concurrent_write_wave().unwrap(),
        [1; BENCHMARK_SHARDS as usize]
    );

    for shard in 0..BENCHMARK_SHARDS as usize {
        assert_eq!(fixture.write_count(shard).unwrap(), 2);
    }
}

#[test]
fn engine_point_read_returns_the_seeded_row_through_the_default_pool() {
    let runtime = engine_benchmark_runtime().unwrap();
    let fixture = EngineBenchmarkFixture::new(&runtime).unwrap();
    let storage_fixture = BenchmarkFixture::new().unwrap();

    assert_eq!(fixture.keys_by_shard(), storage_fixture.keys_by_shard());
    let result = runtime.block_on(fixture.point_read()).unwrap();

    assert_eq!(
        result.columns(),
        vec![
            Column::new("id", DataType::Unknown),
            Column::new("writes", DataType::Unknown),
            Column::new("payload", DataType::Unknown),
        ]
    );
    assert_eq!(
        result.rows(),
        vec![Row::new(vec![
            Value::from(fixture.keys_by_shard()[0].clone()),
            Value::from(0_i64),
            Value::from("baseline payload"),
        ])]
    );
}

#[test]
fn engine_point_write_reuses_the_fixture_and_updates_exactly_one_row() {
    let runtime = engine_benchmark_runtime().unwrap();
    let fixture = EngineBenchmarkFixture::new(&runtime).unwrap();

    assert_eq!(runtime.block_on(fixture.point_write()).unwrap(), 1);
    assert_eq!(runtime.block_on(fixture.point_write()).unwrap(), 1);

    assert_eq!(runtime.block_on(fixture.write_count(0)).unwrap(), 2);
}

#[test]
fn engine_concurrent_write_wave_updates_one_key_on_every_shard() {
    let runtime = engine_benchmark_runtime().unwrap();
    let fixture = EngineBenchmarkFixture::new(&runtime).unwrap();
    for (shard, key) in fixture.keys_by_shard().iter().enumerate() {
        assert_eq!(usize::from(fixture.shard_for_key(key)), shard);
    }

    assert_eq!(
        runtime
            .block_on(fixture.four_shard_concurrent_write_wave())
            .unwrap(),
        [1; BENCHMARK_SHARDS as usize]
    );
    assert_eq!(
        runtime
            .block_on(fixture.four_shard_concurrent_write_wave())
            .unwrap(),
        [1; BENCHMARK_SHARDS as usize]
    );

    for shard in 0..BENCHMARK_SHARDS as usize {
        assert_eq!(runtime.block_on(fixture.write_count(shard)).unwrap(), 2);
    }
}
