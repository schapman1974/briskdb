use super::*;
use briskdb::protocol::mongo::MongoResourceLimits;

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
