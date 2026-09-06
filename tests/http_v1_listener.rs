#![cfg(feature = "listeners")]

use std::net::SocketAddr;

use briskdb::{
    BriskDb,
    server::{AttachedServer, ListenerConfig},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Duration, timeout},
};

async fn post_json(address: SocketAddr, path: &str, body: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(body).unwrap();
    let headers = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(10), stream.read_to_end(&mut response))
        .await
        .expect("the HTTP listener should complete a small query response")
        .unwrap();
    response
}

fn split_response(response: &[u8]) -> (&str, &[u8]) {
    let boundary = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response must contain a header terminator");
    let headers = std::str::from_utf8(&response[..boundary]).unwrap();
    (headers, &response[boundary + 4..])
}

fn tagged(tag: &str, value: &str) -> Value {
    json!({"$briskdb_type": tag, "value": value})
}

#[tokio::test]
async fn attached_listener_serves_the_lossless_query_contract_over_tcp() {
    let temp = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(temp.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = AttachedServer::start(
        &database,
        ListenerConfig {
            http_listen: "127.0.0.1:0".parse().unwrap(),
            admin_listen: None,
            postgres_listen: None,
        },
    )
    .await
    .unwrap();

    let response = post_json(
        server.addresses().http(),
        "/v1/query",
        &json!({
            "shard_key": "tcp-lossless",
            "sql": "SELECT 9007199254740993 AS exact_integer, X'0001ff' AS payload, NULL AS nullable",
            "value_encoding": "lossless-json-v1"
        }),
    )
    .await;
    let (headers, body) = split_response(&response);
    let lower_headers = headers.to_ascii_lowercase();
    assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(lower_headers.contains("\r\ncontent-type: application/json\r\n"));
    assert!(lower_headers.contains("\r\nbriskdb-api-version: 1\r\n"));

    let mut body: Value = serde_json::from_slice(body).unwrap();
    assert!(body["shard"].as_u64().is_some());
    body.as_object_mut().unwrap().remove("shard");
    assert_eq!(
        body,
        json!({
            "value_encoding": "lossless-json-v1",
            "columns": [
                {"name": "exact_integer", "data_type": "unknown"},
                {"name": "payload", "data_type": "unknown"},
                {"name": "nullable", "data_type": "unknown"}
            ],
            "rows": [[
                tagged("int64", "9007199254740993"),
                tagged("binary", "AAH/"),
                null
            ]]
        })
    );

    server.close().await.unwrap();
    database.close().await.unwrap();
}
