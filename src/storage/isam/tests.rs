//! Model comparisons, corruption checks, and local process-failure boundaries.
use super::*;
use std::{
    collections::BTreeMap,
    os::unix::fs::{FileExt, symlink},
    process::Command,
    time::Duration,
};

fn open_fixture(key_bytes: u16, value_bytes: u16) -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(
        directory.path().join("records.isam"),
        Layout::new(key_bytes, value_bytes).unwrap(),
    )
    .unwrap();
    (directory, store)
}

#[test]
fn native_catalog_persists_identity_and_versioned_schema_without_sqlite() {
    catalog_identity_and_indexes(false);
}

#[test]
fn packed_catalog_persists_identity_rows_and_indexes_without_sqlite() {
    catalog_identity_and_indexes(true);
}

fn catalog_identity_and_indexes(packed: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog.isam");
    let mut catalog = if packed {
        NativeCatalog::create_packed(&path)
    } else {
        NativeCatalog::create(&path)
    }
    .unwrap();
    let identity = catalog.identity();
    let initial_generation = catalog.generation().unwrap();
    let definition = TableDefinition {
        name: "verses".to_owned(),
        schema_version: 1,
        columns: vec![
            ColumnDefinition {
                name: "book".to_owned(),
                column_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                name: "number".to_owned(),
                column_type: ColumnType::UInt64,
                nullable: false,
            },
            ColumnDefinition {
                name: "body".to_owned(),
                column_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                name: "note".to_owned(),
                column_type: ColumnType::Text,
                nullable: true,
            },
        ],
        primary_key: vec!["book".to_owned(), "number".to_owned()],
        indexes: vec![
            IndexDefinition {
                name: "book_number".to_owned(),
                columns: vec!["book".to_owned(), "number".to_owned()],
                unique: true,
            },
            IndexDefinition {
                name: "unique_number".to_owned(),
                columns: vec!["number".to_owned()],
                unique: true,
            },
            IndexDefinition {
                name: "by_body".to_owned(),
                columns: vec!["body".to_owned()],
                unique: false,
            },
            IndexDefinition {
                name: "unique_note".to_owned(),
                columns: vec!["note".to_owned()],
                unique: true,
            },
        ],
    };
    catalog.create_table(&definition).unwrap();
    assert_eq!(catalog.generation().unwrap(), initial_generation + 1);
    assert_eq!(catalog.table("verses").unwrap(), Some(definition.clone()));
    assert_eq!(catalog.tables().unwrap(), vec![definition.clone()]);
    let committed_generation = catalog.generation().unwrap();
    assert!(matches!(
        catalog.create_table(&TableDefinition {
            name: "verses".to_owned(),
            schema_version: 2,
            columns: vec![ColumnDefinition {
                name: "body".to_owned(),
                column_type: ColumnType::Text,
                nullable: false,
            }],
            primary_key: vec!["body".to_owned()],
            indexes: Vec::new(),
        }),
        Err(Error::Duplicate)
    ));
    assert_eq!(catalog.generation().unwrap(), committed_generation);
    let first = vec![
        NativeValue::Text("Genesis".to_owned()),
        NativeValue::UInt64(1),
        NativeValue::Text("shared body".to_owned()),
        NativeValue::Null,
    ];
    let second = vec![
        NativeValue::Text("Genesis".to_owned()),
        NativeValue::UInt64(2),
        NativeValue::Text("shared body".to_owned()),
        NativeValue::Null,
    ];
    catalog.insert_row("verses", &first).unwrap();
    catalog.insert_row("verses", &second).unwrap();
    assert_eq!(
        catalog
            .get_row(
                "verses",
                &[
                    NativeValue::Text("Genesis".to_owned()),
                    NativeValue::UInt64(2),
                ],
            )
            .unwrap(),
        Some(second.clone())
    );
    assert_eq!(
        catalog
            .lookup_index(
                "verses",
                "by_body",
                &[NativeValue::Text("shared body".to_owned())],
                10,
            )
            .unwrap(),
        vec![first.clone(), second.clone()]
    );
    assert_eq!(
        catalog
            .lookup_index("verses", "unique_number", &[NativeValue::UInt64(2)], 10,)
            .unwrap(),
        vec![second.clone()]
    );
    assert_eq!(
        catalog
            .range_index(
                "verses",
                "book_number",
                &[
                    NativeValue::Text("Genesis".to_owned()),
                    NativeValue::UInt64(1),
                ],
                Some(&[
                    NativeValue::Text("Genesis".to_owned()),
                    NativeValue::UInt64(2),
                ]),
                10,
            )
            .unwrap(),
        vec![first.clone()]
    );
    let moved = vec![
        NativeValue::Text("Exodus".to_owned()),
        NativeValue::UInt64(1),
        NativeValue::Text("updated body".to_owned()),
        NativeValue::Text("note".to_owned()),
    ];
    assert!(
        catalog
            .update_row(
                "verses",
                &[
                    NativeValue::Text("Genesis".to_owned()),
                    NativeValue::UInt64(1),
                ],
                &moved,
            )
            .unwrap()
    );
    assert_eq!(
        catalog
            .lookup_index("verses", "unique_number", &[NativeValue::UInt64(1)], 10,)
            .unwrap(),
        vec![moved.clone()]
    );
    assert_eq!(
        catalog
            .lookup_index(
                "verses",
                "by_body",
                &[NativeValue::Text("shared body".to_owned())],
                10,
            )
            .unwrap(),
        vec![second.clone()]
    );
    assert_eq!(
        catalog
            .lookup_index(
                "verses",
                "unique_note",
                &[NativeValue::Text("note".to_owned())],
                10,
            )
            .unwrap(),
        vec![moved.clone()]
    );
    let duplicate = catalog.insert_row(
        "verses",
        &[
            NativeValue::Text("Exodus".to_owned()),
            NativeValue::UInt64(2),
            NativeValue::Text("duplicate unique index".to_owned()),
            NativeValue::Null,
        ],
    );
    assert!(matches!(duplicate, Err(Error::Duplicate)), "{duplicate:?}");
    assert!(
        catalog
            .get_row(
                "verses",
                &[
                    NativeValue::Text("Exodus".to_owned()),
                    NativeValue::UInt64(2),
                ],
            )
            .unwrap()
            .is_none()
    );
    assert!(
        catalog
            .delete_row(
                "verses",
                &[
                    NativeValue::Text("Genesis".to_owned()),
                    NativeValue::UInt64(2),
                ],
            )
            .unwrap()
    );
    assert!(
        catalog
            .lookup_index(
                "verses",
                "by_body",
                &[NativeValue::Text("shared body".to_owned())],
                10,
            )
            .unwrap()
            .is_empty()
    );
    let batch_first = vec![
        NativeValue::Text("Psalms".to_owned()),
        NativeValue::UInt64(10),
        NativeValue::Text("batch first".to_owned()),
        NativeValue::Null,
    ];
    let batch_second = vec![
        NativeValue::Text("Psalms".to_owned()),
        NativeValue::UInt64(20),
        NativeValue::Text("batch second".to_owned()),
        NativeValue::Null,
    ];
    let before_batch_insert = catalog.generation().unwrap();
    catalog.reset_operation_stats();
    catalog
        .insert_rows("verses", &[batch_first.clone(), batch_second.clone()])
        .unwrap();
    let batch_insert_stats = catalog.operation_stats();
    assert_eq!(batch_insert_stats.syncs, 2);
    assert_eq!(batch_insert_stats.root_writes, 1);
    assert_eq!(catalog.generation().unwrap(), before_batch_insert + 1);
    let swapped_first = vec![
        NativeValue::Text("Psalms".to_owned()),
        NativeValue::UInt64(20),
        NativeValue::Text("batch first updated".to_owned()),
        NativeValue::Text("batch note 1".to_owned()),
    ];
    let swapped_second = vec![
        NativeValue::Text("Psalms".to_owned()),
        NativeValue::UInt64(10),
        NativeValue::Text("batch second updated".to_owned()),
        NativeValue::Text("batch note 2".to_owned()),
    ];
    let batch_updates = vec![
        (
            vec![
                NativeValue::Text("Psalms".to_owned()),
                NativeValue::UInt64(10),
            ],
            swapped_first.clone(),
        ),
        (
            vec![
                NativeValue::Text("Psalms".to_owned()),
                NativeValue::UInt64(20),
            ],
            swapped_second.clone(),
        ),
    ];
    let before_batch_update = catalog.generation().unwrap();
    catalog.reset_operation_stats();
    assert!(catalog.update_rows("verses", &batch_updates).unwrap());
    let batch_update_stats = catalog.operation_stats();
    assert_eq!(batch_update_stats.syncs, 2);
    assert_eq!(batch_update_stats.root_writes, 1);
    assert_eq!(catalog.generation().unwrap(), before_batch_update + 1);
    assert_eq!(
        catalog
            .lookup_index("verses", "unique_number", &[NativeValue::UInt64(10)], 10)
            .unwrap(),
        vec![swapped_second.clone()]
    );
    assert_eq!(
        catalog
            .lookup_index(
                "verses",
                "by_body",
                &[NativeValue::Text("batch first".to_owned())],
                10,
            )
            .unwrap(),
        Vec::<Vec<NativeValue>>::new()
    );
    let generation_before_missing = catalog.generation().unwrap();
    assert!(
        !catalog
            .update_rows(
                "verses",
                &[
                    batch_updates[0].clone(),
                    (
                        vec![
                            NativeValue::Text("Psalms".to_owned()),
                            NativeValue::UInt64(99),
                        ],
                        swapped_first.clone(),
                    ),
                ],
            )
            .unwrap()
    );
    assert_eq!(catalog.generation().unwrap(), generation_before_missing);
    assert_eq!(
        catalog
            .lookup_index(
                "verses",
                "by_body",
                &[NativeValue::Text("batch first updated".to_owned())],
                10,
            )
            .unwrap(),
        vec![swapped_first.clone()]
    );
    let delete_keys = vec![
        vec![
            NativeValue::Text("Psalms".to_owned()),
            NativeValue::UInt64(20),
        ],
        vec![
            NativeValue::Text("Psalms".to_owned()),
            NativeValue::UInt64(10),
        ],
    ];
    let generation_before_missing_delete = catalog.generation().unwrap();
    assert!(
        !catalog
            .delete_rows(
                "verses",
                &[
                    delete_keys[0].clone(),
                    vec![
                        NativeValue::Text("Psalms".to_owned()),
                        NativeValue::UInt64(99),
                    ],
                ],
            )
            .unwrap()
    );
    assert_eq!(
        catalog.generation().unwrap(),
        generation_before_missing_delete
    );
    assert!(catalog.delete_rows("verses", &delete_keys).unwrap());
    assert_eq!(
        catalog.generation().unwrap(),
        generation_before_missing_delete + 1
    );
    assert!(
        catalog
            .lookup_index(
                "verses",
                "unique_note",
                &[NativeValue::Text("batch note 1".to_owned())],
                10,
            )
            .unwrap()
            .is_empty()
    );
    let before_failed_insert = catalog.generation().unwrap();
    assert!(matches!(
        catalog.insert_rows(
            "verses",
            &[
                vec![
                    NativeValue::Text("Proverbs".to_owned()),
                    NativeValue::UInt64(1),
                    NativeValue::Text("same unique".to_owned()),
                    NativeValue::Null,
                ],
                vec![
                    NativeValue::Text("Proverbs".to_owned()),
                    NativeValue::UInt64(2),
                    NativeValue::Text("same unique".to_owned()),
                    NativeValue::Null,
                ],
            ],
        ),
        Err(Error::Duplicate)
    ));
    assert_eq!(catalog.generation().unwrap(), before_failed_insert);
    assert!(matches!(
        catalog.drop_table("verses"),
        Err(Error::Invalid(_))
    ));
    drop(catalog);

    let mut reopened = NativeCatalog::open(&path).unwrap();
    assert_eq!(reopened.identity(), identity);
    assert_eq!(reopened.tables().unwrap().len(), 1);
    assert_eq!(reopened.table("verses").unwrap(), Some(definition));
}

#[test]
fn native_catalog_rejects_invalid_schema_and_non_catalog_files() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog.isam");
    let mut catalog = NativeCatalog::create(&path).unwrap();
    let before = catalog.generation().unwrap();
    let invalid = TableDefinition {
        name: "items".to_owned(),
        schema_version: 1,
        columns: vec![
            ColumnDefinition {
                name: "id".to_owned(),
                column_type: ColumnType::UInt64,
                nullable: false,
            },
            ColumnDefinition {
                name: "id".to_owned(),
                column_type: ColumnType::Text,
                nullable: true,
            },
        ],
        primary_key: Vec::new(),
        indexes: Vec::new(),
    };
    assert!(matches!(
        catalog.create_table(&invalid),
        Err(Error::Invalid(_))
    ));
    assert_eq!(catalog.generation().unwrap(), before);

    let other_path = directory.path().join("records.isam");
    let _other = Store::create(&other_path, Layout::new(128, 1024).unwrap()).unwrap();
    assert!(matches!(
        NativeCatalog::open(other_path),
        Err(Error::Corrupt(_))
    ));
}

fn typed_catalog_fixture() -> (tempfile::TempDir, NativeCatalog) {
    let directory = tempfile::tempdir().unwrap();
    let mut catalog = NativeCatalog::create(directory.path().join("catalog.isam")).unwrap();
    catalog
        .create_table(&TableDefinition {
            name: "items".into(),
            schema_version: 1,
            columns: vec![
                ColumnDefinition {
                    name: "id".into(),
                    column_type: ColumnType::UInt64,
                    nullable: false,
                },
                ColumnDefinition {
                    name: "tag".into(),
                    column_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDefinition {
                    name: "body".into(),
                    column_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
            indexes: vec![
                IndexDefinition {
                    name: "unique_tag".into(),
                    columns: vec!["tag".into()],
                    unique: true,
                },
                IndexDefinition {
                    name: "by_tag".into(),
                    columns: vec!["tag".into()],
                    unique: false,
                },
            ],
        })
        .unwrap();
    (directory, catalog)
}

fn typed_row(id: u64, tag: &str, body: &str) -> Vec<NativeValue> {
    vec![
        NativeValue::UInt64(id),
        NativeValue::Text(tag.into()),
        NativeValue::Text(body.into()),
    ]
}

#[test]
fn native_bulk_rejections_leave_rows_indexes_and_generation_unchanged() {
    let (directory, mut catalog) = typed_catalog_fixture();
    let first = typed_row(1, "one", "original");
    let second = typed_row(2, "two", "original");
    catalog
        .insert_rows("items", &[first.clone(), second.clone()])
        .unwrap();
    let generation = catalog.generation().unwrap();
    catalog.reset_operation_stats();

    // A later conflict must not leak an earlier, otherwise valid mutation.
    for rows in [
        vec![
            typed_row(3, "three", "new"),
            typed_row(4, "one", "conflict"),
        ],
        vec![
            typed_row(3, "three", "new"),
            typed_row(3, "four", "duplicate PK"),
        ],
        vec![
            typed_row(3, "shared", "new"),
            typed_row(4, "shared", "duplicate index"),
        ],
    ] {
        assert!(matches!(
            catalog.insert_rows("items", &rows),
            Err(Error::Duplicate)
        ));
    }
    assert!(matches!(
        catalog.update_rows(
            "items",
            &[
                (vec![NativeValue::UInt64(1)], typed_row(1, "changed", "new")),
                (
                    vec![NativeValue::UInt64(2)],
                    typed_row(1, "other", "conflicting target")
                ),
            ]
        ),
        Err(Error::Duplicate)
    ));
    assert!(matches!(
        catalog.update_rows(
            "items",
            &[
                (vec![NativeValue::UInt64(1)], typed_row(1, "changed", "new")),
                (
                    vec![NativeValue::UInt64(2)],
                    typed_row(2, "changed", "conflicting index")
                ),
            ]
        ),
        Err(Error::Duplicate)
    ));
    assert!(matches!(
        catalog.update_rows(
            "items",
            &[
                (vec![NativeValue::UInt64(1)], typed_row(1, "one", "new")),
                (
                    vec![NativeValue::UInt64(1)],
                    typed_row(2, "two", "duplicate source")
                ),
            ]
        ),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        catalog.delete_rows(
            "items",
            &[vec![NativeValue::UInt64(1)], vec![NativeValue::UInt64(1)],]
        ),
        Err(Error::Invalid(_))
    ));
    assert!(
        !catalog
            .delete_rows(
                "items",
                &[vec![NativeValue::UInt64(1)], vec![NativeValue::UInt64(99)],]
            )
            .unwrap()
    );
    assert!(matches!(
        catalog.insert_rows(
            "items",
            &[
                typed_row(3, "three", "new"),
                typed_row(4, "four", &"x".repeat(1024)),
            ]
        ),
        Err(Error::Invalid(_))
    ));
    let mut wrong_type = typed_row(4, "four", "new");
    wrong_type[0] = NativeValue::Text("not an integer".into());
    assert!(matches!(
        catalog.insert_rows("items", &[typed_row(3, "three", "new"), wrong_type,]),
        Err(Error::Invalid(_))
    ));
    let too_many_physical_entries: Vec<_> = (10..(10 + MAX_BATCH_RECORDS as u64 / 3 + 1))
        .map(|id| typed_row(id, &format!("tag-{id}"), "body"))
        .collect();
    assert!(matches!(
        catalog.insert_rows("items", &too_many_physical_entries),
        Err(Error::Invalid(_))
    ));
    catalog.insert_rows("items", &[]).unwrap();
    assert!(catalog.update_rows("items", &[]).unwrap());
    assert!(catalog.delete_rows("items", &[]).unwrap());
    let stats = catalog.operation_stats();
    assert_eq!(
        (stats.root_writes, stats.page_writes, stats.syncs),
        (0, 0, 0)
    );
    assert_eq!(catalog.generation().unwrap(), generation);
    drop(catalog);
    let mut reopened = NativeCatalog::open(directory.path().join("catalog.isam")).unwrap();
    assert_eq!(
        reopened
            .get_row("items", &[NativeValue::UInt64(1)])
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        reopened
            .get_row("items", &[NativeValue::UInt64(2)])
            .unwrap(),
        Some(second)
    );
    assert_eq!(
        reopened
            .get_row("items", &[NativeValue::UInt64(3)])
            .unwrap(),
        None
    );
    assert_eq!(
        reopened
            .lookup_index(
                "items",
                "unique_tag",
                &[NativeValue::Text("one".into())],
                10
            )
            .unwrap(),
        vec![first]
    );
    assert!(
        reopened
            .lookup_index(
                "items",
                "by_tag",
                &[NativeValue::Text("changed".into())],
                10
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn native_payload_updates_keep_unchanged_index_entries_and_owners() {
    let (_directory, mut catalog) = typed_catalog_fixture();
    catalog
        .insert_row("items", &typed_row(1, "one", "before"))
        .unwrap();
    catalog.reset_operation_stats();
    let changed = typed_row(1, "one", "after");
    assert!(
        catalog
            .update_row("items", &[NativeValue::UInt64(1)], &changed)
            .unwrap()
    );
    let stats = catalog.operation_stats();
    assert_eq!(
        stats.write_lock_keys, 2,
        "only row delete/insert, no index rewrites"
    );
    assert_eq!(stats.write_lock_stripes_acquired, 1);
    assert_eq!((stats.syncs, stats.root_writes), (2, 1));
    for index in ["unique_tag", "by_tag"] {
        assert_eq!(
            catalog
                .lookup_index("items", index, &[NativeValue::Text("one".into())], 10)
                .unwrap(),
            vec![changed.clone()]
        );
    }
    // Same index value but a changed primary key must update the index owner.
    let moved = typed_row(2, "one", "moved");
    assert!(
        catalog
            .update_row("items", &[NativeValue::UInt64(1)], &moved)
            .unwrap()
    );
    assert_eq!(
        catalog.get_row("items", &[NativeValue::UInt64(1)]).unwrap(),
        None
    );
    for index in ["unique_tag", "by_tag"] {
        assert_eq!(
            catalog
                .lookup_index("items", index, &[NativeValue::Text("one".into())], 10)
                .unwrap(),
            vec![moved.clone()]
        );
    }
}

#[test]
fn native_bulk_null_indexes_publish_once_and_survive_reopen() {
    let (directory, mut catalog) = typed_catalog_fixture();
    let rows: Vec<_> = (0..36)
        .map(|id| {
            vec![
                NativeValue::UInt64(id),
                NativeValue::Null,
                NativeValue::Text("body".into()),
            ]
        })
        .collect();
    catalog.reset_operation_stats();
    catalog.insert_rows("items", &rows).unwrap();
    assert_eq!(
        (
            catalog.operation_stats().syncs,
            catalog.operation_stats().root_writes
        ),
        (2, 1)
    );
    drop(catalog);
    let mut catalog = NativeCatalog::open(directory.path().join("catalog.isam")).unwrap();
    assert!(
        catalog
            .lookup_index("items", "unique_tag", &[NativeValue::Null], 100)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        catalog
            .lookup_index("items", "by_tag", &[NativeValue::Null], 100)
            .unwrap(),
        rows
    );
    catalog.reset_operation_stats();
    let keys: Vec<_> = (0..36).map(|id| vec![NativeValue::UInt64(id)]).collect();
    assert!(catalog.delete_rows("items", &keys).unwrap());
    assert_eq!(
        (
            catalog.operation_stats().syncs,
            catalog.operation_stats().root_writes
        ),
        (2, 1)
    );
    assert!(
        catalog
            .lookup_index("items", "by_tag", &[NativeValue::Null], 100)
            .unwrap()
            .is_empty()
    );
    catalog.drop_table("items").unwrap();
}

#[test]
fn native_concurrent_unique_claims_commit_exactly_one_whole_batch() {
    let (_directory, mut observer) = typed_catalog_fixture();
    let path = _directory.path().join("catalog.isam");
    let mut peers: Vec<_> = (0..2)
        .map(|_| NativeCatalog::open(&path).unwrap())
        .collect();
    for peer in &mut peers {
        peer.set_lock_policy(
            LockPolicy::new(Duration::from_secs(10), Duration::from_millis(1)).unwrap(),
        );
    }
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = peers
            .into_iter()
            .enumerate()
            .map(|(id, mut peer)| {
                let barrier = std::sync::Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    peer.insert_rows(
                        "items",
                        &[
                            typed_row(id as u64 * 2, "contested", "owner"),
                            typed_row(id as u64 * 2 + 1, &format!("companion-{id}"), "other"),
                        ],
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        results.iter().filter(|r| r.is_ok()).count(),
        1,
        "{results:?}"
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::Duplicate)))
            .count(),
        1,
        "{results:?}"
    );
    for (id, result) in results.iter().enumerate() {
        for offset in 0..2 {
            assert_eq!(
                observer
                    .get_row("items", &[NativeValue::UInt64(id as u64 * 2 + offset)])
                    .unwrap()
                    .is_some(),
                result.is_ok()
            );
        }
    }
    assert_eq!(
        observer
            .lookup_index(
                "items",
                "unique_tag",
                &[NativeValue::Text("contested".into())],
                10
            )
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn layout_and_mutation_bounds_are_checked_without_publishing() {
    for (key, value) in [(0, 1), (129, 1), (1, 1025)] {
        assert!(Layout::new(key, value).is_err());
    }
    let (_directory, mut store) = open_fixture(2, 4);
    for mutations in [
        vec![Mutation::insert(b"a", b"ok")],
        vec![Mutation::insert(b"ab", b"12345")],
        vec![Mutation::delete(b"abc")],
        vec![Mutation::put(b"ab", b"x"); MAX_BATCH_RECORDS + 1],
    ] {
        assert!(matches!(
            store.write_batch(&mutations),
            Err(Error::Invalid(_))
        ));
        assert_eq!(store.read_batch().unwrap().generation(), 1);
    }
    let read = store.read_batch().unwrap();
    assert!(read.get(b"a").is_err());
    assert!(read.range(b"zz", Some(b"aa"), 1).is_err());
    assert!(read.range(b"aa", Some(b"x"), 1).is_err());
    assert!(read.range(b"aa", None, MAX_BATCH_RECORDS + 1).is_err());
    assert!(read.range(b"aa", None, 0).unwrap().is_empty());
    assert_eq!(read.verify().unwrap(), 0);
}

#[test]
fn accepts_the_batch_limit_and_orders_binary_composite_keys_bytewise() {
    let (_directory, mut store) = open_fixture(4, 4);
    let keys = [
        [0x00, 0x01, 0x00, 0xff],
        [0x00, 0x01, 0xff, 0x00],
        [0x00, 0x02, 0x00, 0x00],
        [0xff, 0x00, 0x00, 0x00],
    ];
    let mut model = BTreeMap::new();
    let mutations: Vec<_> = keys
        .into_iter()
        .enumerate()
        .map(|(index, key)| {
            let value = [index as u8];
            model.insert(key.to_vec(), value.to_vec());
            Mutation::insert(key, value)
        })
        .collect();
    store.write_batch(&mutations).unwrap();
    let read = store.read_batch().unwrap();
    let actual = read.range(&[0; 4], None, MAX_BATCH_RECORDS).unwrap();
    let expected: Vec<_> = model
        .into_iter()
        .map(|(key, value)| Record { key, value })
        .collect();
    assert_eq!(actual, expected);

    let (directory, mut bounded) = open_fixture(4, 4);
    let mutations: Vec<_> = (0..MAX_BATCH_RECORDS as u32)
        .map(|id| Mutation::insert(id.to_be_bytes(), id.to_le_bytes()))
        .collect();
    bounded.write_batch(&mutations).unwrap();
    let read = bounded.read_batch().unwrap();
    assert_eq!(read.verify().unwrap(), MAX_BATCH_RECORDS as u64);
    assert_eq!(
        read.range(&[0; 4], None, MAX_BATCH_RECORDS).unwrap().len(),
        MAX_BATCH_RECORDS
    );
    drop(bounded);
    let mut reopened = Store::open(directory.path().join("records.isam")).unwrap();
    assert_eq!(
        reopened.read_batch().unwrap().verify().unwrap(),
        MAX_BATCH_RECORDS as u64
    );
}

#[test]
fn persists_ordered_ranges_and_reopens_without_sqlite() {
    let (directory, mut store) = open_fixture(9, 768);
    let mutations: Vec<_> = (1..=120)
        .rev()
        .map(|i| {
            Mutation::insert(
                format!("JHN003{i:03}").as_bytes(),
                format!("verse {i}").as_bytes(),
            )
        })
        .collect();
    store.write_batch(&mutations).unwrap();
    let path = directory.path().join("records.isam");
    drop(store);
    let mut reopened = Store::open(&path).unwrap();
    let read = reopened.read_batch().unwrap();
    assert_eq!(read.get(b"JHN003016").unwrap(), Some(b"verse 16".to_vec()));
    assert_eq!(read.get(b"JHN004000").unwrap(), None);
    let rows = read.range(b"JHN003014", Some(b"JHN003018"), 100).unwrap();
    assert_eq!(
        rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        [b"JHN003014", b"JHN003015", b"JHN003016", b"JHN003017"]
    );
    assert_eq!(read.range(b"JHN003014", None, 2).unwrap().len(), 2);
    assert!(
        read.range(b"JHN003014", Some(b"JHN003014"), 3)
            .unwrap()
            .is_empty()
    );
    assert_eq!(read.verify().unwrap(), 120);
    let mut names: Vec<_> = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "records.isam",
            "records.isam.keylocks",
            "records.isam.writer.lock"
        ]
    );
}

#[test]
fn a_failed_batch_is_atomic_and_orphan_pages_are_reusable() {
    let (_directory, mut store) = open_fixture(2, 8);
    store
        .write_batch(&[Mutation::insert(b"aa", b"old")])
        .unwrap();
    assert!(matches!(
        store.write_batch(&[
            Mutation::put(b"aa", b"new"),
            Mutation::insert(b"bb", b"new"),
            Mutation::insert(b"aa", b"dup")
        ]),
        Err(Error::Duplicate)
    ));
    {
        let read = store.read_batch().unwrap();
        assert_eq!(read.generation(), 2);
        assert_eq!(read.get(b"aa").unwrap(), Some(b"old".to_vec()));
        assert_eq!(read.get(b"bb").unwrap(), None);
    }
    store
        .write_batch(&[
            Mutation::delete(b"aa"),
            Mutation::insert(b"aa", b"again"),
            Mutation::delete(b"zz"),
        ])
        .unwrap();
    assert_eq!(
        store.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"again".to_vec())
    );
    store.write_batch(&[Mutation::delete(b"aa")]).unwrap();
    assert_eq!(store.read_batch().unwrap().verify().unwrap(), 0);
}

#[test]
fn snapshots_do_not_block_writes_and_reused_handles_refresh() {
    let (directory, mut writer) = open_fixture(2, 8);
    writer
        .write_batch(&[Mutation::insert(b"aa", b"before")])
        .unwrap();
    let mut reader = Store::open_read_only(directory.path().join("records.isam")).unwrap();
    let read = reader.read_batch().unwrap();
    // The read scope deliberately stays live across two writer commits.
    writer
        .write_batch(&[
            Mutation::put(b"aa", b"after"),
            Mutation::insert(b"bb", b"new"),
        ])
        .unwrap();
    writer.write_batch(&[Mutation::delete(b"aa")]).unwrap();
    assert_eq!(read.get(b"aa").unwrap(), Some(b"before".to_vec()));
    assert_eq!(read.get(b"bb").unwrap(), None);
    let fresh = reader.read_batch().unwrap();
    assert_eq!(fresh.get(b"aa").unwrap(), None);
    assert_eq!(fresh.get(b"bb").unwrap(), Some(b"new".to_vec()));
    assert!(matches!(
        reader.write_batch(&[Mutation::delete(b"bb")]),
        Err(Error::ReadOnly)
    ));
}

#[test]
fn open_snapshot_reuses_validated_root_without_changing_later_read_batches() {
    for packed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("records.isam");
        let layout = Layout::new(2, 32).unwrap();
        let mut writer = if packed {
            Store::create_packed(&path, layout)
        } else {
            Store::create(&path, layout)
        }
        .unwrap();
        writer.write_batch(&[Mutation::put(b"aa", b"old")]).unwrap();

        let mut ordinary = Store::open_read_only(&path).unwrap();
        assert_eq!(
            ordinary.read_batch().unwrap().get(b"aa").unwrap(),
            Some(b"old".to_vec())
        );
        let before = ordinary.operation_stats();
        assert_eq!(
            (before.root_reads, before.page_reads, before.lock_requests),
            (2, 2, 2)
        );

        let (_, after) = Store::with_open_read_only_snapshot(&path, |actual_layout, read| {
            assert_eq!(actual_layout, layout);
            writer.write_batch(&[Mutation::put(b"aa", b"new")]).unwrap();
            for _ in 0..3 {
                assert_eq!(read.get(b"aa").unwrap(), Some(b"old".to_vec()));
            }
        })
        .unwrap();
        assert_eq!(
            (after.root_reads, after.page_reads, after.lock_requests),
            (1, 1, 1)
        );
        assert_eq!((after.file_opens, after.file_closes), (2, 2));
        assert_eq!(
            ordinary.read_batch().unwrap().get(b"aa").unwrap(),
            Some(b"new".to_vec())
        );
        let (latest, _) =
            Store::with_open_read_only_snapshot(&path, |_, read| read.get(b"aa")).unwrap();
        assert_eq!(latest.unwrap(), Some(b"new".to_vec()));
    }
}

#[test]
fn open_snapshot_validates_corruption_before_invoking_callback() {
    for damage in ["root", "page", "truncate"] {
        let (directory, mut store) = open_fixture(2, 32);
        store.write_batch(&[Mutation::put(b"aa", b"old")]).unwrap();
        let snapshot = read_snapshot(&store.file, None).unwrap();
        match damage {
            "root" => store
                .file
                .write_all_at(
                    &[0xff],
                    snapshot.generation % 2 * format::PAGE_BYTES as u64 + 48,
                )
                .unwrap(),
            "page" => store
                .file
                .write_all_at(&[0xff], snapshot.root + 40)
                .unwrap(),
            "truncate" => store.file.set_len(snapshot.root + 40).unwrap(),
            _ => unreachable!(),
        }
        let result =
            Store::with_open_read_only_snapshot(directory.path().join("records.isam"), |_, _| {
                panic!("corrupt snapshot must not reach callback")
            });
        assert!(
            matches!(result, Err(Error::Corrupt(_))),
            "{damage}: {result:?}"
        );
    }
}

#[test]
fn writable_open_migrates_a_missing_key_lock_sidecar_after_read_only_open() {
    let (directory, store) = open_fixture(2, 4);
    let path = directory.path().join("records.isam");
    drop(store);
    let key_locks = directory.path().join("records.isam.keylocks");
    std::fs::remove_file(&key_locks).unwrap();

    let mut reader = Store::open_read_only(&path).unwrap();
    assert!(!key_locks.exists());
    assert!(matches!(
        reader.write_batch(&[Mutation::insert(b"aa", b"v")]),
        Err(Error::ReadOnly)
    ));
    drop(reader);

    let mut writer = Store::open(&path).unwrap();
    assert_eq!(std::fs::metadata(&key_locks).unwrap().len(), 4096);
    writer
        .write_batch(&[Mutation::insert(b"aa", b"v")])
        .unwrap();
    assert_eq!(
        writer.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn writable_open_rejects_a_malformed_key_lock_sidecar() {
    let (directory, store) = open_fixture(2, 4);
    let key_locks = directory.path().join("records.isam.keylocks");
    drop(store);
    std::fs::write(&key_locks, [0_u8; 4096]).unwrap();

    assert!(matches!(
        Store::open(directory.path().join("records.isam")),
        Err(Error::Corrupt(_))
    ));
    assert_eq!(std::fs::metadata(key_locks).unwrap().len(), 4096);
}

#[test]
fn read_cache_reuses_pages_without_crossing_snapshot_or_file_boundaries() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ReadBatch<'_>>();
    let (directory, mut store) = open_fixture(2, 1024);
    store
        .write_batch(
            &(0..90_u16)
                .map(|key| Mutation::insert(key.to_be_bytes(), b"old"))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let mut writer = Store::open(directory.path().join("records.isam")).unwrap();
    store.reset_operation_stats();
    let stats = store.operation_stats_handle();
    let batch = store.read_batch().unwrap();
    assert_eq!(batch.range(&[0, 0], None, 90).unwrap().len(), 90);
    let reads = stats.snapshot().page_reads;
    assert!(reads > 1);
    for key in 0..90_u16 {
        assert_eq!(
            batch.get(&key.to_be_bytes()).unwrap(),
            Some(b"old".to_vec())
        );
    }
    assert_eq!(batch.range(&[0, 10], Some(&[0, 50]), 90).unwrap().len(), 40);
    assert_eq!(stats.snapshot().page_reads, reads);
    writer
        .write_batch(&[Mutation::put([0, 1], b"new")])
        .unwrap();
    assert_eq!(batch.get(&[0, 1]).unwrap(), Some(b"old".to_vec()));
    drop(batch);
    let batch = store.read_batch().unwrap();
    assert_eq!(batch.get(&[0, 1]).unwrap(), Some(b"new".to_vec()));
    assert!(stats.snapshot().page_reads > reads);

    let (_other_directory, mut other) = open_fixture(2, 1024);
    other
        .write_batch(&[Mutation::put([0, 1], b"other")])
        .unwrap();
    assert_eq!(
        other.read_batch().unwrap().get(&[0, 1]).unwrap(),
        Some(b"other".to_vec())
    );
}

#[test]
fn verification_bypasses_cached_pages_and_errors_are_not_cached() {
    let (_directory, mut store) = open_fixture(2, 32);
    store
        .write_batch(&[Mutation::put(b"aa", b"original")])
        .unwrap();
    let batch = store.read_batch().unwrap();
    assert_eq!(batch.get(b"aa").unwrap(), Some(b"original".to_vec()));
    let mut original = [0; format::PAGE_BYTES];
    batch
        .file
        .read_exact_at(&mut original, batch.snapshot.root)
        .unwrap();
    let mut damaged = original;
    damaged[40] ^= 1;
    batch
        .file
        .write_all_at(&damaged, batch.snapshot.root)
        .unwrap();
    // Cached immutable data remains usable, but verify always checks the file.
    assert_eq!(batch.get(b"aa").unwrap(), Some(b"original".to_vec()));
    assert!(matches!(batch.verify(), Err(Error::Corrupt(_))));
    drop(batch);
    let batch = store.read_batch().unwrap();
    assert!(matches!(batch.get(b"aa"), Err(Error::Corrupt(_))));
    batch
        .file
        .write_all_at(&original, batch.snapshot.root)
        .unwrap();
    assert_eq!(batch.get(b"aa").unwrap(), Some(b"original".to_vec()));
    assert_eq!(batch.verify().unwrap(), 1);
}

#[test]
fn one_read_admission_covers_many_records_and_counts_logical_io() {
    let (_directory, mut store) = open_fixture(9, 32);
    let mutations: Vec<_> = (0..36_u16)
        .map(|verse| {
            Mutation::insert(
                format!("JHN003{verse:03}").into_bytes(),
                format!("verse {verse}").into_bytes(),
            )
        })
        .collect();
    store.write_batch(&mutations).unwrap();
    store.reset_operation_stats();

    let batch = store.read_batch().unwrap();
    let verses = batch.range(b"JHN003000", Some(b"JHN004000"), 36).unwrap();
    assert_eq!(verses.len(), 36);
    drop(batch);

    let stats = store.operation_stats();
    assert_eq!(stats.file_opens, 0);
    assert_eq!(stats.root_reads, 1);
    assert!(stats.root_read_ns > 0);
    assert_eq!(stats.lock_requests, 1);
    assert_eq!(stats.lock_retries, 0);
    assert!(stats.page_reads < 36);
    assert!(stats.page_read_ns > 0);
}

#[test]
fn disjoint_same_file_writers_prepare_concurrently_and_rebase_before_flushing() {
    let (directory, _created) = open_fixture(2, 4);
    let path = directory.path().join("records.isam");
    let first_key = b"aa";
    let second_key = (0_u8..=u8::MAX)
        .map(|byte| [b'k', byte])
        .find(|candidate| {
            locking::key_lock_stripe(first_key) != locking::key_lock_stripe(candidate)
        })
        .unwrap();
    let first = Store::open(&path).unwrap();
    let second = Store::open(&path).unwrap();
    let mut observer = Store::open(&path).unwrap();
    let first_paused = std::sync::atomic::AtomicBool::new(false);
    let (prepared_tx, prepared_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let first_writer = std::thread::spawn(move || {
        let mut first = first;
        first.reset_operation_stats();
        first
            .write_with_hook(&[Mutation::insert(first_key, b"one")], |point| {
                if point == CommitPoint::PlanPrepared
                    && !first_paused.swap(true, std::sync::atomic::Ordering::Relaxed)
                {
                    prepared_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }
                Ok(())
            })
            .unwrap();
        let stats = first.operation_stats();
        assert_eq!(stats.preflight_rebases, 1);
        assert_eq!(stats.publication_retries, 0);
        assert_eq!(
            (stats.syncs, stats.root_writes, stats.page_writes),
            (2, 1, 1)
        );
    });
    if prepared_rx.recv_timeout(Duration::from_secs(3)).is_err() {
        let _ = release_tx.send(());
        let _ = first_writer.join();
        panic!("first same-file writer did not reach page preparation");
    }

    let (second_prepared_tx, second_prepared_rx) = std::sync::mpsc::channel();
    let second_writer = std::thread::spawn(move || {
        let mut second = second;
        second.write_with_hook(&[Mutation::insert(second_key, b"two")], |point| {
            if point == CommitPoint::PagesSynced {
                second_prepared_tx.send(()).unwrap();
            }
            Ok(())
        })
    });
    if second_prepared_rx
        .recv_timeout(Duration::from_secs(3))
        .is_err()
    {
        release_tx.send(()).unwrap();
        let _ = first_writer.join();
        let _ = second_writer.join();
        panic!("disjoint same-file writer could not prepare concurrently");
    }
    second_writer.join().unwrap().unwrap();
    assert_eq!(
        observer.read_batch().unwrap().get(&second_key).unwrap(),
        Some(b"two".to_vec()),
        "a reader must observe a disjoint writer while another writer is prepared"
    );
    release_tx.send(()).unwrap();
    first_writer.join().unwrap();

    let read = observer.read_batch().unwrap();
    assert_eq!(read.get(first_key).unwrap(), Some(b"one".to_vec()));
    assert_eq!(read.get(&second_key).unwrap(), Some(b"two".to_vec()));
    assert_eq!(read.verify().unwrap(), 2);
}

#[test]
fn commit_gate_blocks_writes_before_io_but_not_readers() {
    let (directory, mut owner) = open_fixture(2, 8);
    owner
        .write_batch(&[Mutation::insert(b"aa", b"old")])
        .unwrap();
    let path = directory.path().join("records.isam");
    let mut peer = Store::open_with_policy(
        &path,
        LockPolicy::new(Duration::from_millis(15), Duration::from_millis(1)).unwrap(),
    )
    .unwrap();
    let sibling = Store::open(&path).unwrap();
    let mut reader = Store::open_read_only(&path).unwrap();
    let policy = LockPolicy::default();
    let guard = owner
        .key_locks
        .as_ref()
        .unwrap()
        .acquire_commit(policy.deadline(), policy.interval(), &owner.counters)
        .unwrap();
    drop(sibling);
    let original_len = owner.file.metadata().unwrap().len();
    peer.reset_operation_stats();
    assert!(matches!(
        peer.write_batch(&[Mutation::insert(b"bb", b"new")]),
        Err(Error::Busy)
    ));
    let stats = peer.operation_stats();
    assert_eq!(
        (
            stats.page_writes,
            stats.root_writes,
            stats.syncs,
            stats.file_stats
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(owner.file.metadata().unwrap().len(), original_len);
    assert!(stats.commit_lock_wait_ns > 0);
    assert!(stats.lock_retries > 0);
    assert_eq!(
        reader.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(reader.read_batch().unwrap().get(b"bb").unwrap(), None);
    drop(guard);
    peer.write_batch(&[Mutation::insert(b"bb", b"new")])
        .unwrap();
    assert_eq!(
        (
            peer.operation_stats().syncs,
            peer.operation_stats().publication_retries
        ),
        (2, 0)
    );
    peer.reset_operation_stats();
    assert_eq!(peer.operation_stats(), OperationStats::default());
}

#[test]
fn publication_recheck_preserves_a_commit_from_an_older_ungated_writer() {
    let (directory, mut first) = open_fixture(2, 4);
    let second = Store::open(directory.path().join("records.isam")).unwrap();
    let second_key = (0_u8..=u8::MAX)
        .map(|byte| [b'k', byte])
        .find(|key| locking::key_lock_stripe(b"aa") != locking::key_lock_stripe(key))
        .unwrap();
    let mut older_committed = false;
    first.reset_operation_stats();
    first
        .write_with_hook(&[Mutation::insert(b"aa", b"one")], |point| {
            if point == CommitPoint::PagesSynced && !older_committed {
                older_committed = true;
                // Reproduce the older writer's protocol without the new gate.
                // This is intentionally test-only; normal writes cannot bypass it.
                let policy = LockPolicy::default();
                let _legacy = Guard::acquire(&second.writer_lock, false, policy).unwrap();
                let _stripe = second
                    .key_locks
                    .as_ref()
                    .unwrap()
                    .acquire_stripes(
                        &[&second_key],
                        policy.deadline(),
                        policy.interval(),
                        &second.counters,
                    )
                    .unwrap();
                let base = second.snapshot().unwrap();
                let plan = tree::prepare_batch(
                    &second.file,
                    base,
                    &[Mutation::insert(second_key, b"two")],
                    &second.counters,
                    None,
                )
                .unwrap();
                let start = second
                    .reserve_page_range(base, plan.page_count(), policy.deadline())
                    .unwrap()
                    .unwrap();
                let mut next = base;
                next.generation += 1;
                let next =
                    tree::write_plan(&second.file, base, next, start, &plan, &second.counters)
                        .unwrap();
                sync_file(&second.file, &second.counters).unwrap();
                let _publish = Guard::acquire(&second.file, true, policy).unwrap();
                assert_eq!(second.snapshot().unwrap(), base);
                write_snapshot(&second.file, next, &second.counters).unwrap();
                sync_file(&second.file, &second.counters).unwrap();
            }
            Ok(())
        })
        .unwrap();
    let stats = first.operation_stats();
    assert_eq!(stats.publication_retries, 1);
    assert_eq!(stats.preflight_rebases, 0);
    assert_eq!(stats.syncs, 3);
    let read = first.read_batch().unwrap();
    assert_eq!(read.get(b"aa").unwrap(), Some(b"one".to_vec()));
    assert_eq!(read.get(&second_key).unwrap(), Some(b"two".to_vec()));
    assert_eq!(read.verify().unwrap(), 2);
}

#[test]
fn same_key_writers_serialize_and_the_waiter_rebases() {
    let (directory, mut created) = open_fixture(2, 4);
    created
        .write_batch(&[Mutation::insert(b"aa", b"old")])
        .unwrap();
    let path = directory.path().join("records.isam");
    let first = Store::open(&path).unwrap();
    let sibling = Store::open(&path).unwrap();
    let mut second = Store::open_with_policy(
        &path,
        LockPolicy::new(Duration::from_secs(2), Duration::from_millis(2)).unwrap(),
    )
    .unwrap();
    let (prepared_tx, prepared_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let first_writer = std::thread::spawn(move || {
        let mut first = first;
        first.write_with_hook(&[Mutation::put(b"aa", b"one")], |point| {
            if point == CommitPoint::PagesSynced {
                prepared_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }
            Ok(())
        })
    });
    prepared_rx.recv().unwrap();
    drop(sibling);

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let second_writer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = second.write_batch(&[Mutation::put(b"aa", b"two")]);
        done_tx.send(result.is_ok()).unwrap();
        result
    });
    started_rx.recv().unwrap();
    assert!(
        done_rx.try_recv().is_err(),
        "same-key writer must wait for the owning stripe"
    );
    release_tx.send(()).unwrap();
    first_writer.join().unwrap().unwrap();
    second_writer.join().unwrap().unwrap();
    assert_eq!(
        created.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"two".to_vec())
    );
}

#[test]
fn key_lock_worker() {
    let Some(path) = std::env::var_os("BRISK_ISAM_KEY_LOCK_PATH") else {
        return;
    };
    let key = std::env::var("BRISK_ISAM_KEY_LOCK_KEY").unwrap();
    let ready = PathBuf::from(std::env::var_os("BRISK_ISAM_KEY_LOCK_READY").unwrap());
    let store = Store::open(path).unwrap();
    let key_lock = store
        .key_locks
        .as_ref()
        .expect("writable test store has key locks")
        .acquire_stripes(
            &[key.as_bytes()],
            LockPolicy::default().deadline(),
            LockPolicy::default().interval(),
            &store.counters,
        )
        .unwrap();
    std::fs::write(ready, b"ready").unwrap();
    std::thread::sleep(Duration::from_millis(250));
    drop(key_lock);
}

#[test]
fn key_lock_ranges_are_process_visible_and_release_on_exit() {
    let (directory, _created) = open_fixture(2, 4);
    let path = directory.path().join("records.isam");
    let ready = directory.path().join("child-ready");
    let key = b"aa";
    let store = Store::open(&path).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "storage::isam::tests::key_lock_worker"])
        .env("BRISK_ISAM_KEY_LOCK_PATH", &path)
        .env("BRISK_ISAM_KEY_LOCK_KEY", std::str::from_utf8(key).unwrap())
        .env("BRISK_ISAM_KEY_LOCK_READY", &ready)
        .spawn()
        .unwrap();
    while !ready.exists() {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(matches!(
        store
            .key_locks
            .as_ref()
            .expect("writable test store has key locks")
            .acquire_stripes(
                &[key],
                LockPolicy::new(Duration::from_millis(15), Duration::from_millis(2))
                    .unwrap()
                    .deadline(),
                Duration::from_millis(2),
                &store.counters,
            ),
        Err(Error::Busy)
    ));
    let failed_stats = store.operation_stats();
    let stripe = locking::key_lock_stripe(key);
    assert_eq!(failed_stats.write_lock_batches, 1);
    assert!(failed_stats.write_lock_requests > 0);
    assert!(failed_stats.write_lock_retries > 0);
    assert_eq!(failed_stats.write_lock_local_retries, 0);
    assert!(failed_stats.write_lock_range_retries > 0);
    assert_eq!(
        failed_stats.write_lock_retries,
        failed_stats.write_lock_local_retries + failed_stats.write_lock_range_retries
    );
    assert!(failed_stats.write_lock_range_wait_ns > 0);
    assert_eq!(failed_stats.write_lock_stripes_acquired, 0);
    assert_eq!(failed_stats.write_lock_stripe_acquisitions[stripe], 0);
    assert!(failed_stats.write_lock_stripe_retries[stripe] > 0);
    let distinct_key = (0_u8..=u8::MAX)
        .map(|byte| [b'k', byte])
        .find(|candidate| locking::key_lock_stripe(key) != locking::key_lock_stripe(candidate))
        .unwrap();
    let distinct_lock = store
        .key_locks
        .as_ref()
        .expect("writable test store has key locks")
        .acquire_stripes(
            &[&distinct_key],
            LockPolicy::default().deadline(),
            LockPolicy::default().interval(),
            &store.counters,
        )
        .unwrap();
    drop(distinct_lock);
    assert!(child.wait().unwrap().success());
    store
        .key_locks
        .as_ref()
        .expect("writable test store has key locks")
        .acquire_stripes(
            &[key],
            LockPolicy::default().deadline(),
            LockPolicy::default().interval(),
            &store.counters,
        )
        .unwrap();
}

#[test]
fn write_lock_stats_count_deduplicated_stripes_by_id() {
    let (_directory, mut store) = open_fixture(2, 8);
    let first = b"aa";
    let same_stripe = (0_u8..=u8::MAX)
        .map(|byte| [b'k', byte])
        .find(|candidate| {
            *candidate != *first
                && locking::key_lock_stripe(first) == locking::key_lock_stripe(candidate)
        })
        .unwrap();
    let other_stripe = (0_u8..=u8::MAX)
        .map(|byte| [b'z', byte])
        .find(|candidate| locking::key_lock_stripe(first) != locking::key_lock_stripe(candidate))
        .unwrap();
    let first_stripe = locking::key_lock_stripe(first);
    let other_stripe_id = locking::key_lock_stripe(&other_stripe);

    store.reset_operation_stats();
    store
        .write_batch(&[
            Mutation::insert(first, b"one"),
            Mutation::insert(&same_stripe, b"two"),
            Mutation::insert(&other_stripe, b"three"),
        ])
        .unwrap();

    let stats = store.operation_stats();
    assert_eq!(stats.write_lock_batches, 1);
    assert_eq!(stats.write_lock_keys, 3);
    assert_eq!(stats.write_lock_requests, 2);
    assert_eq!(stats.write_lock_stripes_acquired, 2);
    assert_eq!(stats.write_lock_stripe_acquisitions[first_stripe], 1);
    assert_eq!(stats.write_lock_stripe_acquisitions[other_stripe_id], 1);
    assert_eq!(stats.write_lock_stripe_acquisitions.iter().sum::<u64>(), 2);
    assert_eq!(stats.write_lock_retries, 0);
    assert_eq!(stats.write_lock_local_retries, 0);
    assert_eq!(stats.write_lock_range_retries, 0);
    assert_eq!(
        stats.write_lock_retries,
        stats.write_lock_local_retries + stats.write_lock_range_retries
    );
}

#[test]
fn operation_stats_handle_observes_close_after_store_drop() {
    let (directory, mut store) = open_fixture(2, 4);
    store
        .write_batch(&[Mutation::insert(b"aa", b"val")])
        .unwrap();
    drop(store);
    let store = Store::open(directory.path().join("records.isam")).unwrap();
    let stats = store.operation_stats_handle();
    assert_eq!(stats.snapshot().file_opens, 3);
    assert!(stats.snapshot().file_stats >= 4);
    assert!(stats.snapshot().root_read_ns > 0);
    assert!(stats.snapshot().page_read_ns > 0);
    assert_eq!(stats.snapshot().file_closes, 0);
    drop(store);
    assert_eq!(stats.snapshot().file_closes, 3);
}

#[test]
fn lock_wait_and_retries_are_included_in_operation_stats() {
    let (directory, store) = open_fixture(2, 4);
    let mut peer = Store::open_with_policy(
        directory.path().join("records.isam"),
        LockPolicy::new(Duration::from_millis(15), Duration::from_millis(2)).unwrap(),
    )
    .unwrap();
    let lock = Guard::acquire(&store.writer_lock, true, LockPolicy::default()).unwrap();
    assert!(matches!(
        peer.write_batch(&[Mutation::insert(b"aa", b"val")]),
        Err(Error::Busy)
    ));
    drop(lock);

    let stats = peer.operation_stats();
    assert!(stats.lock_requests > 0);
    assert!(stats.lock_retries > 0);
    assert!(stats.lock_wait_ns > 0);
    peer.write_batch(&[Mutation::insert(b"aa", b"val")])
        .unwrap();
    let stats = peer.operation_stats();
    assert!(stats.root_write_ns > 0);
    assert!(stats.page_write_ns > 0);
    assert!(stats.sync_ns > 0);
    assert!(stats.publication_ns > 0);
}

#[test]
fn readers_and_independent_file_writers_progress_during_write_preparation() {
    let (directory, mut writer) = open_fixture(2, 8);
    writer
        .write_batch(&[Mutation::insert(b"aa", b"before")])
        .unwrap();
    let mut reader = Store::open_read_only(directory.path().join("records.isam")).unwrap();
    let (_other_directory, mut independent) = open_fixture(2, 8);
    let mut overlapped = false;
    writer
        .write_with_hook(&[Mutation::put(b"aa", b"after")], |point| {
            if point == CommitPoint::PagesWritten {
                assert_eq!(
                    reader.read_batch().unwrap().get(b"aa").unwrap(),
                    Some(b"before".to_vec())
                );
                independent
                    .write_batch(&[Mutation::insert(b"bb", b"other")])
                    .unwrap();
                overlapped = true;
            } else if point == CommitPoint::RootWritten {
                // Only this publication/sync interval excludes new read snapshots.
                assert!(matches!(reader.read_batch(), Err(Error::Busy)));
            }
            Ok(())
        })
        .unwrap();
    assert!(overlapped);
    assert_eq!(
        reader.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"after".to_vec())
    );
    assert_eq!(
        independent.read_batch().unwrap().get(b"bb").unwrap(),
        Some(b"other".to_vec())
    );
}

#[test]
fn ambiguous_alternate_root_damage_fails_closed_instead_of_rolling_back() {
    let (directory, mut store) = open_fixture(2, 4);
    store
        .write_batch(&[Mutation::insert(b"aa", b"old")])
        .unwrap();
    let committed = read_snapshot(&store.file, None).unwrap();
    // This could be an interrupted next-generation write or later corruption
    // of an acknowledged root. The format cannot prove which, so it must refuse
    // to expose the older generation as though it were current.
    let inactive = ((committed.generation + 1) % 2) * format::PAGE_BYTES as u64;
    store.file.write_all_at(&[0xff; 123], inactive).unwrap();
    drop(store);
    assert!(matches!(
        Store::open(directory.path().join("records.isam")),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn corruption_of_the_latest_acknowledged_root_never_falls_back() {
    let (directory, mut store) = open_fixture(2, 4);
    store
        .write_batch(&[Mutation::insert(b"aa", b"ack")])
        .unwrap();
    let committed = read_snapshot(&store.file, None).unwrap();
    let current_slot = committed.generation % 2 * format::PAGE_BYTES as u64;
    store.file.write_all_at(&[0xff], current_slot + 48).unwrap();
    drop(store);
    assert!(matches!(
        Store::open(directory.path().join("records.isam")),
        Err(Error::Corrupt(_))
    ));
}

fn wide_key(value: u64) -> Vec<u8> {
    let mut key = vec![0; 128];
    key[..8].copy_from_slice(&value.to_be_bytes());
    key
}

#[test]
fn multilevel_splits_updates_and_deletes_match_an_independent_map() {
    let (directory, mut store) = open_fixture(128, 1024);
    let mut model = BTreeMap::new();
    let initial: Vec<_> = (0..600_u64)
        .rev()
        .map(|i| {
            model.insert(wide_key(i), i.to_le_bytes().to_vec());
            Mutation::insert(wide_key(i), i.to_le_bytes())
        })
        .collect();
    store.write_batch(&initial).unwrap();
    let initial_snapshot = read_snapshot(&store.file, None).unwrap();
    // Small leaf/branch fanout forces both kinds of split in this fixture.
    let mut root_level = [0];
    store
        .file
        .read_exact_at(&mut root_level, initial_snapshot.root + 24)
        .unwrap();
    assert!(root_level[0] >= 2);
    // Bulk construction must coalesce writes, not emit 600 copied root paths.
    assert!(initial_snapshot.end < 230 * format::PAGE_BYTES as u64);
    let mut seed = 19_u64;
    for batch in 0..30 {
        let mut operations = Vec::new();
        for _ in 0..25 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let key = wide_key(seed % 800);
            if seed % 3 == 0 {
                model.remove(&key);
                operations.push(Mutation::delete(key));
            } else {
                let value = seed.to_le_bytes().to_vec();
                model.insert(key.clone(), value.clone());
                operations.push(Mutation::put(key, value));
            }
        }
        store.write_batch(&operations).unwrap();
        let read = store.read_batch().unwrap();
        assert_eq!(read.verify().unwrap(), model.len() as u64, "batch {batch}");
        let actual = read.range(&wide_key(0), None, MAX_BATCH_RECORDS).unwrap();
        let expected: Vec<_> = model
            .iter()
            .map(|(k, v)| Record {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        assert_eq!(actual, expected, "batch {batch}");
        for (key, value) in &model {
            assert_eq!(read.get(key).unwrap().as_ref(), Some(value));
        }
    }
    let deletes: Vec<_> = model.keys().cloned().map(Mutation::delete).collect();
    store.write_batch(&deletes).unwrap();
    assert_eq!(store.read_batch().unwrap().verify().unwrap(), 0);
    drop(store);
    let mut reopened = Store::open(directory.path().join("records.isam")).unwrap();
    reopened
        .write_batch(&[Mutation::insert(wide_key(9999), b"reborn")])
        .unwrap();
    assert_eq!(reopened.read_batch().unwrap().verify().unwrap(), 1);
}

#[test]
fn shared_publication_locks_coexist_and_writer_lock_is_separate() {
    let (directory, writer) = open_fixture(2, 4);
    let mut reader = Store::open(directory.path().join("records.isam")).unwrap();
    let first = Guard::acquire(&writer.file, false, LockPolicy::default()).unwrap();
    let second = Guard::acquire(&reader.file, false, LockPolicy::default()).unwrap();
    assert!(matches!(
        Guard::acquire(&reader.file, true, LockPolicy::default()),
        Err(Error::Busy)
    ));
    drop(second);
    drop(first);
    let preparing = Guard::acquire(&writer.writer_lock, true, LockPolicy::default()).unwrap();
    assert!(
        reader.read_batch().is_ok(),
        "writer preparation must not exclude readers"
    );
    assert!(matches!(
        reader.write_batch(&[Mutation::insert(b"aa", b"x")]),
        Err(Error::Busy)
    ));
    drop(preparing);
    reader
        .write_batch(&[Mutation::insert(b"aa", b"x")])
        .unwrap();
}

#[test]
fn packed_values_split_grow_shrink_reopen_and_match_a_map() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("packed.isam");
    let layout = Layout::new(128, 1024).unwrap();
    let mut store = Store::create_packed(&path, layout).unwrap();
    assert_eq!(store.format_version(), 3);
    let mut model = BTreeMap::new();
    let initial: Vec<_> = (0..600_u64)
        .rev()
        .map(|i| {
            let value = vec![i as u8; [0, 1, 31, 128, 512, 1024][i as usize % 6]];
            model.insert(wide_key(i), value.clone());
            Mutation::insert(wide_key(i), value)
        })
        .collect();
    store.write_batch(&initial).unwrap();
    let mut fixed = Store::create(directory.path().join("fixed.isam"), layout).unwrap();
    fixed.write_batch(&initial).unwrap();
    assert!(store.snapshot().unwrap().end < fixed.snapshot().unwrap().end);
    let initial_snapshot = store.snapshot().unwrap();
    let mut root_level = [0];
    store
        .file
        .read_exact_at(&mut root_level, initial_snapshot.root + 24)
        .unwrap();
    assert!(root_level[0] >= 2);

    let mut seed = 19_u64;
    for batch_number in 0..30 {
        let mut operations = Vec::new();
        for _ in 0..25 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let key = wide_key(seed % 800);
            if seed % 3 == 0 {
                model.remove(&key);
                operations.push(Mutation::delete(key));
            } else {
                let value = vec![seed as u8; (seed % 1025) as usize];
                model.insert(key.clone(), value.clone());
                operations.push(Mutation::put(key, value));
            }
        }
        store.write_batch(&operations).unwrap();
        let read = store.read_batch().unwrap();
        assert_eq!(
            read.verify().unwrap(),
            model.len() as u64,
            "batch {batch_number}"
        );
        let expected: Vec<_> = model
            .iter()
            .map(|(key, value)| Record {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        assert_eq!(
            read.range(&wide_key(0), None, MAX_BATCH_RECORDS).unwrap(),
            expected
        );
        for (key, value) in &model {
            assert_eq!(read.get(key).unwrap().as_ref(), Some(value));
        }
        drop(read);
        if batch_number % 5 == 0 {
            drop(store);
            store = Store::open(&path).unwrap();
            assert_eq!(store.format_version(), 3);
        }
    }
    let generation = store.snapshot().unwrap().generation;
    let key = model.first_key_value().unwrap().0.clone();
    assert!(matches!(
        store.write_batch(&[Mutation::insert(key, b"duplicate")]),
        Err(Error::Duplicate)
    ));
    assert!(matches!(
        store.write_batch(&[Mutation::put(wide_key(9000), vec![0; 1025])]),
        Err(Error::Invalid(_))
    ));
    assert_eq!(store.snapshot().unwrap().generation, generation);
    store
        .write_batch(
            &model
                .keys()
                .cloned()
                .map(Mutation::delete)
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert_eq!(store.read_batch().unwrap().verify().unwrap(), 0);
    drop(store);
    let mut store = Store::open(&path).unwrap();
    store
        .write_batch(&[Mutation::put(wide_key(1), b"reborn")])
        .unwrap();
    assert_eq!(
        store.read_batch().unwrap().get(&wide_key(1)).unwrap(),
        Some(b"reborn".to_vec())
    );
}

#[test]
fn packed_empty_values_support_dense_pages_and_maximum_batches() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dense.isam");
    let mut store = Store::create_packed(&path, Layout::new(2, 1024).unwrap()).unwrap();
    store
        .write_batch(
            &(0..MAX_BATCH_RECORDS as u16)
                .map(|key| Mutation::insert(key.to_be_bytes(), []))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    store.reset_operation_stats();
    let stats = store.operation_stats_handle();
    let read = store.read_batch().unwrap();
    let records = read.range(&[0, 0], None, MAX_BATCH_RECORDS).unwrap();
    assert_eq!(records.len(), MAX_BATCH_RECORDS);
    for (key, record) in records.iter().enumerate() {
        assert_eq!(record.key, (key as u16).to_be_bytes());
        assert!(record.value.is_empty());
    }
    // 1006 minimum-sized records per leaf, five leaves and one branch.
    assert_eq!(stats.snapshot().page_reads, 6);
    assert_eq!(read.verify().unwrap(), MAX_BATCH_RECORDS as u64);
    drop(read);
    store
        .write_batch(&[
            Mutation::put(2048_u16.to_be_bytes(), vec![7; 1024]),
            Mutation::delete(2048_u16.to_be_bytes()),
            Mutation::insert(2048_u16.to_be_bytes(), b"last"),
        ])
        .unwrap();
    drop(store);
    let mut store = Store::open(path).unwrap();
    let read = store.read_batch().unwrap();
    assert_eq!(
        read.get(&2048_u16.to_be_bytes()).unwrap(),
        Some(b"last".to_vec())
    );
    assert_eq!(read.verify().unwrap(), MAX_BATCH_RECORDS as u64);
}

#[test]
fn packed_page_decoder_rejects_bad_lengths_counts_magic_padding_and_truncation() {
    for damage in 0..8 {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("packed.isam");
        let mut store = Store::create_packed(&path, Layout::new(128, 1024).unwrap()).unwrap();
        store
            .write_batch(&[
                Mutation::put(wide_key(1), vec![1; 1024]),
                Mutation::put(wide_key(2), vec![2; 1024]),
                Mutation::put(wide_key(3), vec![3; 1024]),
                Mutation::put(wide_key(4), []),
            ])
            .unwrap();
        let snapshot = store.snapshot().unwrap();
        let mut bytes = [0; format::PAGE_BYTES];
        store.file.read_exact_at(&mut bytes, snapshot.root).unwrap();
        assert_eq!(bytes[24], 0);
        assert_eq!(format::u16_at(&bytes, 26), 4);
        match damage {
            0 => bytes[168..170].copy_from_slice(&1025_u16.to_le_bytes()),
            1 => bytes[3630..3632].copy_from_slice(&1024_u16.to_le_bytes()),
            2 => bytes[26..28].copy_from_slice(&u16::MAX.to_le_bytes()),
            3 => bytes[format::CHECKSUM_START - 1] = 1,
            4 => bytes[..8].copy_from_slice(b"BRIPAGE1"),
            5 => bytes[1194..1322].copy_from_slice(&wide_key(1)),
            6 => bytes[40] ^= 1,
            7 => {}
            _ => unreachable!(),
        }
        if damage != 6 {
            format::seal(&mut bytes);
        }
        store.file.write_all_at(&bytes, snapshot.root).unwrap();
        if damage == 7 {
            store
                .file
                .set_len(snapshot.root + format::PAGE_BYTES as u64 - 1)
                .unwrap();
        }
        assert!(
            matches!(store.read_batch().unwrap().verify(), Err(Error::Corrupt(_))),
            "damage {damage}"
        );
        drop(store);
        assert!(
            matches!(Store::open(&path), Err(Error::Corrupt(_))),
            "damage {damage}"
        );
    }
}

#[test]
fn format_versions_are_not_mixed_or_implicitly_converted() {
    let (directory, mut fixed) = open_fixture(2, 8);
    fixed.write_batch(&[Mutation::put(b"aa", b"v2")]).unwrap();
    let path = directory.path().join("records.isam");
    assert!(Store::create_packed(&path, fixed.layout()).is_err());
    drop(fixed);
    let mut fixed = Store::open(&path).unwrap();
    assert_eq!(fixed.format_version(), 2);
    fixed
        .write_batch(&[Mutation::put(b"aa", b"still v2")])
        .unwrap();
    assert_eq!(fixed.snapshot().unwrap().format_version, 2);
    let snapshot = fixed.snapshot().unwrap();
    let slot = snapshot.generation % 2 * format::PAGE_BYTES as u64;
    let mut bytes = [0; format::PAGE_BYTES];
    fixed.file.read_exact_at(&mut bytes, slot).unwrap();
    bytes[..8].copy_from_slice(b"BRISAM03");
    bytes[8..10].copy_from_slice(&3_u16.to_le_bytes());
    format::seal(&mut bytes);
    fixed.file.write_all_at(&bytes, slot).unwrap();
    assert!(matches!(fixed.read_batch(), Err(Error::Corrupt(_))));
    drop(fixed);
    assert!(matches!(Store::open(&path), Err(Error::Corrupt(_))));
}

#[test]
fn lock_retries_are_bounded_and_can_succeed_after_release() {
    assert!(LockPolicy::new(Duration::from_secs(301), Duration::from_millis(1)).is_err());
    assert!(LockPolicy::new(Duration::ZERO, Duration::ZERO).is_err());
    let (directory, writer) = open_fixture(2, 4);
    let path = directory.path().join("records.isam");
    let mut peer = Store::open(&path).unwrap();
    peer.set_lock_policy(
        LockPolicy::new(Duration::from_millis(15), Duration::from_millis(2)).unwrap(),
    );
    let lock = Guard::acquire(&writer.writer_lock, true, LockPolicy::default()).unwrap();
    assert!(matches!(
        peer.write_batch(&[Mutation::insert(b"aa", b"x")]),
        Err(Error::Busy)
    ));
    drop(lock);
    peer.write_batch(&[Mutation::insert(b"aa", b"x")]).unwrap();
    let key_lock = writer
        .key_locks
        .as_ref()
        .unwrap()
        .acquire_stripes(
            &[b"aa"],
            LockPolicy::default().deadline(),
            LockPolicy::default().interval(),
            &writer.counters,
        )
        .unwrap();
    assert!(matches!(
        peer.write_batch(&[Mutation::put(b"aa", b"y")]),
        Err(Error::Busy)
    ));
    drop(key_lock);
    peer.write_batch(&[Mutation::put(b"aa", b"y")]).unwrap();
}

#[test]
fn rejects_foreign_truncated_symlinked_and_aliased_files_without_changing_them() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("foreign");
    std::fs::write(&path, b"SQLite format 3\0not our data").unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(Store::create(&path, Layout::new(2, 4).unwrap()).is_err());
    assert!(Store::open(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!writer_lock_path(&path).unwrap().exists());
    let link = directory.path().join("symlink");
    symlink(&path, &link).unwrap();
    assert!(Store::open(&link).is_err());
    let (_good_directory, store) = open_fixture(2, 4);
    store.file.set_len(10).unwrap();
    assert!(matches!(
        read_snapshot(&store.file, None),
        Err(Error::Corrupt(_))
    ));
    let (directory, _store) = open_fixture(2, 4);
    std::fs::hard_link(
        directory.path().join("records.isam"),
        directory.path().join("alias"),
    )
    .unwrap();
    assert!(Store::open(directory.path().join("records.isam")).is_err());
}

#[test]
fn rejects_group_or_world_access_on_data_and_lock_files() {
    for lock_file in [false, true] {
        let (directory, store) = open_fixture(2, 4);
        let path = directory.path().join(if lock_file {
            "records.isam.writer.lock"
        } else {
            "records.isam"
        });
        drop(store);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(matches!(
            Store::open(directory.path().join("records.isam")),
            Err(Error::Invalid(_))
        ));
    }
}

#[test]
fn rejects_checksummed_unknown_format_versions_and_reserved_header_bytes() {
    for version in [0_u16, 1_u16, 3_u16] {
        let (_directory, store) = open_fixture(2, 4);
        let snapshot = read_snapshot(&store.file, None).unwrap();
        let slot = snapshot.generation % 2 * format::PAGE_BYTES as u64;
        let mut header = [0; format::PAGE_BYTES];
        store.file.read_exact_at(&mut header, slot).unwrap();
        header[8..10].copy_from_slice(&version.to_le_bytes());
        format::seal(&mut header);
        store.file.write_all_at(&header, slot).unwrap();
        assert!(matches!(
            read_snapshot(&store.file, None),
            Err(Error::Corrupt(_))
        ));
    }

    let (_directory, store) = open_fixture(2, 4);
    let snapshot = read_snapshot(&store.file, None).unwrap();
    let slot = snapshot.generation % 2 * format::PAGE_BYTES as u64;
    let mut header = [0; format::PAGE_BYTES];
    store.file.read_exact_at(&mut header, slot).unwrap();
    header[40] = 1;
    format::seal(&mut header);
    store.file.write_all_at(&header, slot).unwrap();
    assert!(matches!(
        read_snapshot(&store.file, None),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn rejects_experimental_v1_files_without_migration() {
    let (directory, store) = open_fixture(2, 4);
    let snapshot = read_snapshot(&store.file, None).unwrap();
    let slot = snapshot.generation % 2 * format::PAGE_BYTES as u64;
    let mut header = [0; format::PAGE_BYTES];
    store.file.read_exact_at(&mut header, slot).unwrap();
    header[..8].copy_from_slice(b"BRISAM01");
    header[8..10].copy_from_slice(&1_u16.to_le_bytes());
    format::seal(&mut header);
    store.file.write_all_at(&header, slot).unwrap();
    assert!(matches!(
        Store::open(directory.path().join("records.isam")),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn failed_initialization_leaves_an_incomplete_file_that_open_does_not_adopt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("incomplete.isam");
    std::fs::write(directory.path().join("incomplete.isam.writer.lock"), []).unwrap();
    std::fs::set_permissions(
        directory.path().join("incomplete.isam.writer.lock"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    assert!(Store::create(&path, Layout::new(2, 4).unwrap()).is_err());
    assert!(path.exists());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    assert!(matches!(Store::open(&path), Err(Error::Corrupt(_))));
}

#[test]
fn refuses_corrupt_pages_instead_of_falling_back_to_old_committed_data() {
    let (directory, mut store) = open_fixture(2, 4);
    store
        .write_batch(&[Mutation::insert(b"aa", b"old")])
        .unwrap();
    store.write_batch(&[Mutation::put(b"aa", b"new")]).unwrap();
    let snapshot = read_snapshot(&store.file, None).unwrap();
    store
        .file
        .write_all_at(&[0xff], snapshot.root + 50)
        .unwrap();
    assert!(matches!(
        store.read_batch().unwrap().get(b"aa"),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(
        Store::open(directory.path().join("records.isam")),
        Err(Error::Corrupt(_))
    ));
    store
        .file
        .write_all_at(&[0xff; format::HEADER_BYTES as usize], 0)
        .unwrap();
    assert!(matches!(
        read_snapshot(&store.file, None),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn rejects_checksummed_invalid_root_bounds_and_page_cycles() {
    let (_directory, mut store) = open_fixture(128, 1024);
    let operations: Vec<_> = (0..30)
        .map(|i| Mutation::insert(wide_key(i), b"value"))
        .collect();
    store.write_batch(&operations).unwrap();
    let snapshot = read_snapshot(&store.file, None).unwrap();
    let mut page = [0; format::PAGE_BYTES];
    store.file.read_exact_at(&mut page, snapshot.root).unwrap();
    assert!(page[24] > 0);
    page[40 + 128..40 + 128 + 8].copy_from_slice(&snapshot.root.to_le_bytes());
    format::seal(&mut page);
    store.file.write_all_at(&page, snapshot.root).unwrap();
    assert!(matches!(
        store.read_batch().unwrap().verify(),
        Err(Error::Corrupt(_))
    ));
    let header_offset = snapshot.generation % 2 * format::PAGE_BYTES as u64;
    store.file.read_exact_at(&mut page, header_offset).unwrap();
    page[32..40].copy_from_slice(&1_u64.to_le_bytes());
    format::seal(&mut page);
    store.file.write_all_at(&page, header_offset).unwrap();
    assert!(matches!(
        read_snapshot(&store.file, None),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn publication_failures_distinguish_uncommitted_and_unknown_outcomes() {
    for point in [
        CommitPoint::PlanPrepared,
        CommitPoint::PagesWritten,
        CommitPoint::PagesSynced,
        CommitPoint::RootWritten,
    ] {
        let (_directory, mut store) = open_fixture(2, 4);
        let result = store.write_with_hook(&[Mutation::insert(b"aa", b"new")], |at| {
            if at == point {
                Err(io::Error::other("injected I/O failure"))
            } else {
                Ok(())
            }
        });
        if point == CommitPoint::RootWritten {
            assert!(matches!(result, Err(Error::CommitUnknown(_))));
            assert_eq!(
                store.read_batch().unwrap().get(b"aa").unwrap(),
                Some(b"new".to_vec())
            );
        } else {
            assert!(matches!(result, Err(Error::Io(_))));
            assert_eq!(store.read_batch().unwrap().get(b"aa").unwrap(), None);
        }
    }
}

#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("BRISK_ISAM_CRASH_PATH") else {
        return;
    };
    let point = std::env::var("BRISK_ISAM_CRASH_POINT").unwrap();
    let mut store = Store::open(path).unwrap();
    store
        .write_with_hook(
            &[
                Mutation::put(b"aa", b"new"),
                Mutation::insert(b"bb", b"new"),
            ],
            |at| {
                if format!("{at:?}") == point {
                    std::process::exit(86);
                }
                Ok(())
            },
        )
        .unwrap();
    panic!("crash boundary not reached");
}

#[test]
fn process_exit_releases_locks_and_preserves_commit_boundary() {
    process_exit_boundaries(false);
}

#[test]
fn packed_process_exit_releases_locks_and_preserves_commit_boundary() {
    process_exit_boundaries(true);
}

fn process_exit_boundaries(packed: bool) {
    for point in [
        CommitPoint::PlanPrepared,
        CommitPoint::PagesWritten,
        CommitPoint::PagesSynced,
        CommitPoint::RootWritten,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("records.isam");
        let layout = Layout::new(2, 4).unwrap();
        let mut parent = if packed {
            Store::create_packed(&path, layout)
        } else {
            Store::create(&path, layout)
        }
        .unwrap();
        parent
            .write_batch(&[Mutation::insert(b"aa", b"old")])
            .unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "storage::isam::tests::crash_worker"])
            .env(
                "BRISK_ISAM_CRASH_PATH",
                directory.path().join("records.isam"),
            )
            .env("BRISK_ISAM_CRASH_POINT", format!("{point:?}"))
            .output()
            .unwrap()
            .status;
        assert_eq!(status.code(), Some(86));
        // Process exit is not power loss: a written, unsynced root is visible
        // in the live OS cache. Do not infer fsync durability from this test.
        let published = point == CommitPoint::RootWritten;
        let read = parent.read_batch().unwrap();
        assert_eq!(
            read.get(b"aa").unwrap(),
            Some(if published {
                b"new".to_vec()
            } else {
                b"old".to_vec()
            })
        );
        assert_eq!(read.get(b"bb").unwrap().is_some(), published);
        parent.write_batch(&[Mutation::put(b"cc", b"ok")]).unwrap();
    }
}

#[test]
fn inherited_handle_and_snapshot_operations_are_rejected() {
    let (_directory, mut store) = open_fixture(2, 4);
    store.owner_pid = store.owner_pid.wrapping_add(1);
    assert!(matches!(store.read_batch(), Err(Error::WrongProcess)));
    assert!(matches!(store.write_batch(&[]), Err(Error::WrongProcess)));
    store.owner_pid = std::process::id();
    let mut read = store.read_batch().unwrap();
    read.owner_pid = read.owner_pid.wrapping_add(1);
    assert!(matches!(read.get(b"aa"), Err(Error::WrongProcess)));
    assert!(matches!(
        read.range(b"aa", None, 1),
        Err(Error::WrongProcess)
    ));
    assert!(matches!(read.verify(), Err(Error::WrongProcess)));
}

#[test]
fn empty_root_encoding_has_stable_fields_and_padding() {
    let (_directory, store) = open_fixture(9, 768);
    let mut bytes = [0; format::PAGE_BYTES];
    store
        .file
        .read_exact_at(&mut bytes, format::PAGE_BYTES as u64)
        .unwrap();
    assert_eq!(&bytes[..16], b"BRISAM02\x02\x00\x09\x00\x00\x03\x00\x00");
    assert_eq!(format::u64_at(&bytes, 16), 1);
    assert_eq!(format::u64_at(&bytes, 24), 0);
    assert_eq!(format::u64_at(&bytes, 32), 8192);
    assert!(bytes[40..format::CHECKSUM_START].iter().all(|b| *b == 0));
    assert!(format::checksum_valid(&bytes));
}
