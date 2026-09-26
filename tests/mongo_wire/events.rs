use super::*;
use tracing::instrument::WithSubscriber;

mod capture {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/mongo_trace_capture.rs"
    ));
}

#[tokio::test]
async fn correlated_events_keep_host_dispatchers_payloads_and_listener_lifetimes_separate() {
    let first_capture = capture::Capture::default();
    let other_capture = capture::Capture::default();
    let (_root, database, mut server) = setup().with_subscriber(first_capture.clone()).await;
    let mut other = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .with_subscriber(other_capture.clone())
        .await
        .unwrap();
    // No subscriber is current here. Each listener must preserve the host's
    // dispatcher through async scheduling and blocking reply workers itself.
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let secret = "private-namespace-document-command-comment-or-credential";
    send_command(&mut stream, &command("hello")).await;
    send_command(&mut stream, &command(secret)).await;
    let seed = BsonDocument::from_entries([
        ("_id", BsonValue::Int32(1)),
        ("sensitive", BsonValue::from(secret)),
    ])
    .unwrap();
    let insert = insert_command(secret, seed);
    send_command(&mut stream, &insert).await;
    send_command(&mut stream, &insert).await;
    let mut one_way = insert;
    one_way
        .push(
            "writeConcern",
            BsonValue::Document(BsonDocument::from_entries([("w", BsonValue::Int32(0))]).unwrap()),
        )
        .unwrap();
    stream.write_all(&packet(&one_way, 77, 2)).await.unwrap();
    send_command(&mut stream, &command("ping")).await;
    let mut invalid = command("ping");
    invalid
        .push("private_option", BsonValue::from(secret))
        .unwrap();
    send_command(&mut stream, &invalid).await;
    let mut other_stream = TcpStream::connect(other.address()).await.unwrap();
    send_command(&mut other_stream, &command("ping")).await;
    server.close().await.unwrap();
    other.close().await.unwrap();
    database.close().await.unwrap();
    let first = first_capture.0.lock().unwrap();
    let other = other_capture.0.lock().unwrap();
    assert_eq!(
        (first.events.len(), first.spans.len(), first.live.len()),
        (7, 7, 0)
    );
    assert_eq!(
        (other.events.len(), other.spans.len(), other.live.len()),
        (1, 1, 0)
    );
    assert!(
        !format!(
            "{:?}{:?}{:?}{:?}",
            first.events, first.spans, other.events, other.spans
        )
        .contains(secret)
    );
    let connection = &first.events[0]["connection_id"];
    assert_ne!(connection, &other.events[0]["connection_id"]);
    for (number, event) in first.events.iter().enumerate() {
        assert_eq!(&event["connection_id"], connection);
        assert_eq!(event["wire_request_id"], "77");
        assert_eq!(event["sequence"], (number + 1).to_string());
        assert_eq!(event.len(), 11);
    }
    assert_eq!(first.events[1]["command"], "other");
    assert_eq!(first.events[1]["error_code"], "59");
    for index in [3, 4] {
        assert_eq!(first.events[index]["outcome"], "failed");
        assert_eq!(first.events[index]["error_code"], "11000");
        assert_eq!(first.events[index]["write_errors"], "1");
    }
    assert_eq!(first.events[4]["response_suppressed"], "true");
    assert_eq!(first.events[6]["error_code"], "72");
    assert_eq!(other.events[0]["sequence"], "1");
    assert_eq!(other.events[0]["outcome"], "completed");
}
