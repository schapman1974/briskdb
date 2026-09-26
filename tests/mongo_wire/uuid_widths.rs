use super::*;
use briskdb::document::BsonBinary;

fn assert_uuid_error(reply: &BsonDocument) {
    assert_eq!(reply.get_first("code"), Some(&BsonValue::Int32(22)));
    assert_eq!(
        reply.get_first("codeName"),
        Some(&BsonValue::from("InvalidBSON"))
    );
    assert_eq!(
        reply.get_first("errmsg"),
        Some(&BsonValue::from(
            "reply contains a UUID binary value without a 16-byte payload"
        ))
    );
    assert!(reply.get_first("cursor").is_none());
}

#[tokio::test]
async fn opaque_uuid_inputs_remain_queryable_and_unsafe_replies_keep_socket_usable() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    for subtype in [3, 4] {
        for length in [0, 7, 15, 17] {
            for sequence in [false, true] {
                let name = format!("uuid_{subtype}_{length}_{sequence}");
                let value = BsonValue::Binary(BsonBinary::new(subtype, vec![0; length]));
                let document = BsonDocument::from_entries([
                    ("_id", BsonValue::Int32(1)),
                    ("value", value.clone()),
                ])
                .unwrap();
                let bytes = if sequence {
                    insert_sequence(&name, &[document])
                } else {
                    packet(&insert_command(&name, document), 17, 0)
                };
                stream.write_all(&bytes).await.unwrap();
                assert_eq!(
                    response(&mut stream).await.1.get_first("n"),
                    Some(&BsonValue::Int32(1))
                );
                assert_uuid_error(
                    &send_command(&mut stream, &find_command(&name, BsonValue::Int32(1))).await,
                );
                let projected = BsonDocument::from_entries([
                    ("find", BsonValue::from(name.as_str())),
                    (
                        "filter",
                        BsonValue::Document(
                            BsonDocument::from_entries([("value", value)]).unwrap(),
                        ),
                    ),
                    (
                        "sort",
                        BsonValue::Document(
                            BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
                        ),
                    ),
                    (
                        "projection",
                        BsonValue::Document(
                            BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
                        ),
                    ),
                    ("$db", BsonValue::from("wire")),
                ])
                .unwrap();
                assert_eq!(
                    first_batch(&send_command(&mut stream, &projected).await),
                    &[BsonValue::Document(
                        BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap()
                    )]
                );
            }
        }
    }
    let rows: Vec<_> = [3, 4]
        .into_iter()
        .map(|subtype| {
            BsonDocument::from_entries([
                ("_id", BsonValue::Int32(i32::from(subtype))),
                (
                    "value",
                    BsonValue::Binary(BsonBinary::new(subtype, vec![0; 16])),
                ),
            ])
            .unwrap()
        })
        .collect();
    stream
        .write_all(&insert_sequence("valid_uuid", &rows))
        .await
        .unwrap();
    assert_eq!(
        response(&mut stream).await.1.get_first("n"),
        Some(&BsonValue::Int32(2))
    );
    assert_eq!(
        first_batch(&send_command(&mut stream, &cursor_find("valid_uuid", 10)).await),
        &rows
            .into_iter()
            .map(BsonValue::Document)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        send_command(&mut stream, &command("ping"))
            .await
            .get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    drop(stream);
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn uuid_reply_errors_reclaim_initial_and_continuation_cursors() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let documents = [
        BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
        BsonDocument::from_entries([
            ("_id", BsonValue::Int32(2)),
            ("value", BsonValue::Binary(BsonBinary::new(4, vec![0; 7]))),
        ])
        .unwrap(),
        BsonDocument::from_entries([("_id", BsonValue::Int32(3))]).unwrap(),
    ];
    stream
        .write_all(&insert_sequence("items", &documents))
        .await
        .unwrap();
    assert_eq!(
        response(&mut stream).await.1.get_first("n"),
        Some(&BsonValue::Int32(3))
    );
    // Each first batch would retain a continuation. None may leak a cursor slot.
    for _ in 0..10 {
        assert_uuid_error(&send_command(&mut stream, &cursor_find("items", 2)).await);
        assert_eq!(server.metrics().cursors.active, 0);
    }
    for command in [cursor_find("items", 1), cursor_aggregate("items", 1, false)] {
        let id = live_cursor_id(&send_command(&mut stream, &command).await);
        assert!(id > 0);
        assert_uuid_error(&send_command(&mut stream, &cursor_more("items", id, 1)).await);
        assert_eq!(server.metrics().cursors.active, 0);
        assert_eq!(
            send_command(&mut stream, &cursor_more("items", id, 1))
                .await
                .get_first("code"),
            Some(&BsonValue::Int32(43))
        );
    }
    assert_eq!(server.metrics().response_limit_rejections, 12);
    assert_eq!(
        server.metrics().cursors.registered,
        server.metrics().cursors.closed
    );
    assert_eq!(
        send_command(&mut stream, &command("ping"))
            .await
            .get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    drop(stream);
    server.close().await.unwrap();
    database.close().await.unwrap();
}
