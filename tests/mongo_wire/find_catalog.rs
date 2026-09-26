//! A find resolves absence in the engine, never by bypassing catalog health.

use super::metrics::doc;
use super::*;

fn find(collection: &str, zero_batch: bool, single_batch: bool) -> BsonDocument {
    let mut body = find_command(collection, BsonValue::Int32(1));
    body.push("singleBatch", BsonValue::Boolean(single_batch))
        .unwrap();
    if zero_batch {
        body.push("batchSize", BsonValue::Int32(0)).unwrap();
    }
    body
}

#[tokio::test]
async fn missing_find_remains_empty_across_drop_recreate_and_batch_modes() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    for (zero, single) in [(false, false), (false, true), (true, false), (true, true)] {
        assert!(
            first_batch(&send_command(&mut stream, &find("items", zero, single)).await).is_empty()
        );
    }
    let row = doc([("_id", BsonValue::Int32(1))]);
    for _ in 0..2 {
        let inserted = send_command(&mut stream, &insert_command("items", row.clone())).await;
        assert_eq!(inserted.get_first("n"), Some(&BsonValue::Int32(1)));
        for single in [false, true] {
            let reply = send_command(&mut stream, &find("items", false, single)).await;
            assert_eq!(first_batch(&reply), &[BsonValue::Document(row.clone())]);
        }
        assert!(
            first_batch(&send_command(&mut stream, &find("items", true, true)).await).is_empty()
        );
        // An ordinary zero batch must still register an engine-owned cursor.
        let opened = send_command(&mut stream, &find("items", true, false)).await;
        let Some(BsonValue::Document(cursor)) = opened.get_first("cursor") else {
            panic!("{opened:?}")
        };
        let Some(BsonValue::Int64(id)) = cursor.get_first("id") else {
            panic!("{cursor:?}")
        };
        assert_ne!(*id, 0);
        assert_eq!(
            cursor.get_first("firstBatch"),
            Some(&BsonValue::Array(vec![]))
        );
        let next = send_command(
            &mut stream,
            &doc([
                ("getMore", BsonValue::Int64(*id)),
                ("collection", BsonValue::from("items")),
                ("$db", BsonValue::from("wire")),
            ]),
        )
        .await;
        let Some(BsonValue::Document(cursor)) = next.get_first("cursor") else {
            panic!("{next:?}")
        };
        assert_eq!(cursor.get_first("id"), Some(&BsonValue::Int64(0)));
        assert_eq!(
            cursor.get_first("nextBatch"),
            Some(&BsonValue::Array(vec![BsonValue::Document(row.clone())]))
        );
        let dropped = send_command(
            &mut stream,
            &doc([
                ("drop", BsonValue::from("items")),
                ("$db", BsonValue::from("wire")),
            ]),
        )
        .await;
        assert_eq!(dropped.get_first("ok"), Some(&BsonValue::Double(1.0)));
        assert!(
            first_batch(&send_command(&mut stream, &find("items", false, false)).await).is_empty()
        );
    }
    assert_eq!(server.metrics().cursors.active, 0);
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn find_never_turns_a_corrupt_catalog_into_an_empty_result() {
    for (zero, single) in [(false, false), (true, false), (true, true)] {
        for collection in ["items", "absent"] {
            let (root, database, mut server) = setup().await;
            let mut stream = TcpStream::connect(server.address()).await.unwrap();
            let inserted = send_command(
                &mut stream,
                &insert_command("items", doc([("_id", BsonValue::Int32(1))])),
            )
            .await;
            assert_eq!(inserted.get_first("n"), Some(&BsonValue::Int32(1)));
            // Corrupt only this test-owned catalog after the engine is Ready.
            let manifest = rusqlite::Connection::open(root.path().join("manifest.sqlite")).unwrap();
            manifest
                .execute(
                    "UPDATE briskdb_integrity SET manifest_digest = zeroblob(32)",
                    [],
                )
                .unwrap();
            drop(manifest);
            let reply = send_command(&mut stream, &find(collection, zero, single)).await;
            assert_eq!(
                reply.get_first("ok"),
                Some(&BsonValue::Double(0.0)),
                "{reply:?}"
            );
            assert_eq!(
                reply.get_first("code"),
                Some(&BsonValue::Int32(1)),
                "{reply:?}"
            );
            assert!(reply.get_first("cursor").is_none());
            assert!(!format!("{reply:?}").contains(root.path().to_str().unwrap()));
            assert!(!server.readiness().ready());
            server.close().await.unwrap();
            database.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn all_find_batch_modes_still_require_host_document_support() {
    let root = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    for (zero, single) in [(false, false), (true, false), (true, true)] {
        let reply = send_command(&mut stream, &find("absent", zero, single)).await;
        assert_eq!(reply.get_first("code"), Some(&BsonValue::Int32(20)));
        assert!(reply.get_first("cursor").is_none());
    }
    server.close().await.unwrap();
    database.close().await.unwrap();
}
