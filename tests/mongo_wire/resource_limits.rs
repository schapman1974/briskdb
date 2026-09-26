use super::*;
use briskdb::protocol::mongo::MongoResourceLimits;

#[tokio::test]
async fn narrowed_cursor_quotas_survive_rejection_handoff_and_disconnect() {
    let (_root, database, mut normal) = setup().await;
    let limits = MongoResourceLimits::default()
        .with_cursor_limits(3, 2)
        .unwrap();
    let mut narrow =
        MongoServer::start_with_limits(&database, "127.0.0.1:0".parse().unwrap(), limits)
            .await
            .unwrap();
    assert_eq!(narrow.resource_limits(), limits);
    let mut writer = TcpStream::connect(normal.address()).await.unwrap();
    let documents: Vec<_> = (0..12)
        .map(|id| BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap())
        .collect();
    writer
        .write_all(&insert_sequence("cursor_policy", &documents))
        .await
        .unwrap();
    assert_eq!(
        response(&mut writer).await.1.get_first("n"),
        Some(&BsonValue::Int32(12))
    );
    let mut first = TcpStream::connect(narrow.address()).await.unwrap();
    let mut second = TcpStream::connect(narrow.address()).await.unwrap();
    let mut third = TcpStream::connect(narrow.address()).await.unwrap();
    let first_id =
        live_cursor_id(&send_command(&mut first, &cursor_find("cursor_policy", 0)).await);
    let retained_id = live_cursor_id(
        &send_command(&mut first, &cursor_aggregate("cursor_policy", 0, true)).await,
    );
    assert!(first_id > 0 && retained_id > 0);
    // More failed registrations than the default wire quota: rejection must
    // release the freshly allocated native cursor/session, not just its metrics.
    for _ in 0..40 {
        let rejected = send_command(&mut first, &cursor_find("cursor_policy", 0)).await;
        assert_eq!(rejected.get_first("code"), Some(&BsonValue::Int32(10334)));
        assert_eq!(narrow.metrics().cursors.active, 2);
    }
    let moved_id =
        live_cursor_id(&send_command(&mut second, &cursor_find("cursor_policy", 0)).await);
    assert!(moved_id > 0);
    let rejected = send_command(&mut third, &cursor_find("cursor_policy", 0)).await;
    assert_eq!(rejected.get_first("code"), Some(&BsonValue::Int32(10334)));
    let rejected = send_command(&mut first, &cursor_more("cursor_policy", moved_id, 1)).await;
    assert_eq!(rejected.get_first("code"), Some(&BsonValue::Int32(10334)));
    assert_eq!(
        live_cursor_id(
            &send_command(&mut second, &cursor_more("cursor_policy", moved_id, 1)).await
        ),
        moved_id
    );
    let kill = BsonDocument::from_entries([
        ("killCursors", BsonValue::from("cursor_policy")),
        (
            "cursors",
            BsonValue::Array(vec![BsonValue::Int64(first_id)]),
        ),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap();
    assert_eq!(
        send_command(&mut first, &kill)
            .await
            .get_first("cursorsKilled"),
        Some(&BsonValue::Array(vec![BsonValue::Int64(first_id)]))
    );
    assert_eq!(
        live_cursor_id(&send_command(&mut first, &cursor_more("cursor_policy", moved_id, 1)).await),
        moved_id
    );
    second.shutdown().await.unwrap();
    disconnected(&mut second).await;
    assert_eq!(
        live_cursor_id(&send_command(&mut first, &cursor_more("cursor_policy", moved_id, 1)).await),
        moved_id
    );
    assert!(live_cursor_id(&send_command(&mut third, &cursor_find("cursor_policy", 0)).await) > 0);
    assert_eq!(narrow.metrics().cursors.active, 3);
    assert_eq!(narrow.metrics().cursors.limit_rejections, 42);
    // Another listener has its own policy while sharing the same native engine.
    for _ in 0..4 {
        assert!(
            live_cursor_id(&send_command(&mut writer, &cursor_find("cursor_policy", 0)).await) > 0
        );
    }
    for stream in [&mut first, &mut third] {
        stream.shutdown().await.unwrap();
        disconnected(stream).await;
    }
    timeout(Duration::from_secs(3), async {
        while narrow.metrics().cursors.active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut replacement = TcpStream::connect(narrow.address()).await.unwrap();
    for _ in 0..2 {
        assert!(
            live_cursor_id(&send_command(&mut replacement, &cursor_find("cursor_policy", 0)).await)
                > 0
        );
    }
    let mut neighbor = TcpStream::connect(narrow.address()).await.unwrap();
    assert!(
        live_cursor_id(&send_command(&mut neighbor, &cursor_find("cursor_policy", 0)).await) > 0
    );
    narrow.close().await.unwrap();
    let cursors = narrow.metrics().cursors;
    assert_eq!(
        (
            cursors.registered,
            cursors.closed,
            cursors.active,
            cursors.peak
        ),
        (7, 7, 0, 3)
    );
    normal.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn narrowed_connection_caps_are_listener_local_and_reusable() {
    let (_root, database, mut normal) = setup().await;
    let limits = MongoResourceLimits::new(1, Duration::from_secs(15)).unwrap();
    let mut narrow =
        MongoServer::start_with_limits(&database, "127.0.0.1:0".parse().unwrap(), limits)
            .await
            .unwrap();
    assert_eq!(narrow.resource_limits(), limits);
    assert_eq!(normal.resource_limits(), MongoResourceLimits::default());
    for _ in 0..3 {
        let mut peer = TcpStream::connect(narrow.address()).await.unwrap();
        assert_eq!(
            send_command(&mut peer, &command("ping"))
                .await
                .get_first("ok"),
            Some(&BsonValue::Double(1.0))
        );
        let mut overflow = TcpStream::connect(narrow.address()).await.unwrap();
        disconnected(&mut overflow).await;
        let mut other = TcpStream::connect(normal.address()).await.unwrap();
        assert_eq!(
            send_command(&mut other, &command("ping"))
                .await
                .get_first("ok"),
            Some(&BsonValue::Double(1.0))
        );
        peer.shutdown().await.unwrap();
        disconnected(&mut peer).await;
        timeout(Duration::from_secs(3), async {
            while narrow.metrics().active_connections != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(narrow.metrics().peak_connections, 1);
    assert_eq!(narrow.metrics().rejected_connections, 3);
    narrow.close().await.unwrap();
    normal.close().await.unwrap();
    assert_eq!(narrow.metrics().closed_connections, 3);
    database.close().await.unwrap();
}

#[tokio::test]
async fn host_deadline_rejects_writes_without_poisoning_discovery_or_other_listeners() {
    let (_root, database, mut normal) = setup().await;
    // Smaller than the clock resolution / mandatory bounded frame decode;
    // no sleeps or large adversarial workload needed to force expiry.
    let limits = MongoResourceLimits::new(2, Duration::from_nanos(1)).unwrap();
    let mut narrow =
        MongoServer::start_with_limits(&database, "127.0.0.1:0".parse().unwrap(), limits)
            .await
            .unwrap();
    let mut peer = TcpStream::connect(narrow.address()).await.unwrap();
    let mut other = TcpStream::connect(normal.address()).await.unwrap();
    let value = BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap();
    let mut insert = insert_command("host_limits", value);
    // Neither maxTimeMS=0 nor a larger client timeout may relax the host policy.
    for client_ms in [0, 10_000] {
        let mut request = insert.clone();
        request
            .push("maxTimeMS", BsonValue::Int32(client_ms))
            .unwrap();
        let reply = send_command(&mut peer, &request).await;
        assert_eq!(reply.get_first("code"), Some(&BsonValue::Int32(50)));
        assert_eq!(
            send_command(&mut peer, &command("ping"))
                .await
                .get_first("ok"),
            Some(&BsonValue::Double(1.0))
        );
        let found = send_command(
            &mut other,
            &find_command("host_limits", BsonValue::Int32(1)),
        )
        .await;
        let Some(BsonValue::Document(cursor)) = found.get_first("cursor") else {
            panic!("missing cursor: {found:?}");
        };
        assert_eq!(
            cursor.get_first("firstBatch"),
            Some(&BsonValue::Array(vec![]))
        );
    }
    insert.push("maxTimeMS", BsonValue::Int32(0)).unwrap();
    let reply = send_command(&mut other, &insert).await;
    assert_eq!(reply.get_first("n"), Some(&BsonValue::Int32(1)));
    assert_eq!(narrow.metrics().errors_with_code(50), Some(2));
    assert_eq!(narrow.metrics().cursors.active, 0);
    narrow.close().await.unwrap();
    normal.close().await.unwrap();
    database.close().await.unwrap();
}
