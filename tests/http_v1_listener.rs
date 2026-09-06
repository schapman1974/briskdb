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
    post_json_with_headers(address, path, body, &[]).await
}

async fn post_json_with_headers(
    address: SocketAddr,
    path: &str,
    body: &Value,
    extra_headers: &[(&str, &str)],
) -> Vec<u8> {
    let body = serde_json::to_vec(body).unwrap();
    let mut headers = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len(),
    );
    for (name, value) in extra_headers {
        headers.push_str(&format!("{name}: {value}\r\n"));
    }
    headers.push_str("\r\n");
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

fn header_values<'a>(headers: &'a str, expected_name: &str) -> Vec<&'a str> {
    headers
        .split("\r\n")
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case(expected_name))
        .map(|(_, value)| value.trim())
        .collect()
}

fn assert_request_id(headers: &str, expected: Option<&str>) {
    let values = header_values(headers, "briskdb-request-id");
    assert_eq!(values.len(), 1);
    let value = values[0];
    assert_eq!(value.len(), 32);
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_ne!(value, "00000000000000000000000000000000");
    if let Some(expected) = expected {
        assert_eq!(value, expected);
    }
}

fn decode_chunked(mut body: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::new();
    loop {
        let line_end = body
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("each HTTP chunk has a size line");
        let size = std::str::from_utf8(&body[..line_end])
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        let size = usize::from_str_radix(size, 16).unwrap();
        body = &body[line_end + 2..];
        if size == 0 {
            assert!(body.starts_with(b"\r\n"));
            break;
        }
        assert!(body.len() >= size + 2);
        decoded.extend_from_slice(&body[..size]);
        assert_eq!(&body[size..size + 2], b"\r\n");
        body = &body[size + 2..];
    }
    decoded
}

fn ndjson_records(body: &[u8]) -> Vec<Value> {
    assert_eq!(body.last(), Some(&b'\n'));
    body.split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
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
    assert_request_id(headers, None);

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

    let supplied_request_id = "1234567890abcdef1234567890abcdef";
    let response = post_json_with_headers(
        server.addresses().http(),
        "/v1/query/stream",
        &json!({
            "shard_key": "tcp-stream",
            "sql": "SELECT 9007199254740993 AS exact_integer, X'0001ff' AS payload",
            "value_encoding": "lossless-json-v1"
        }),
        &[("BriskDB-Request-ID", supplied_request_id)],
    )
    .await;
    let (headers, body) = split_response(&response);
    let lower_headers = headers.to_ascii_lowercase();
    assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(lower_headers.contains("\r\ncontent-type: application/x-ndjson; charset=utf-8\r\n"));
    assert!(lower_headers.contains("\r\ntransfer-encoding: chunked\r\n"));
    assert!(lower_headers.contains("\r\nbriskdb-api-version: 1\r\n"));
    assert_request_id(headers, Some(supplied_request_id));
    let records = ndjson_records(&decode_chunked(body));
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["kind"], "meta");
    assert!(records[0]["shard"].is_u64());
    assert!(records[0].get("shards").is_none());
    assert_eq!(records[0]["value_encoding"], "lossless-json-v1");
    assert_eq!(
        records[0]["columns"],
        json!([
            {"name":"exact_integer", "data_type":"unknown"},
            {"name":"payload", "data_type":"unknown"}
        ])
    );
    assert_eq!(
        records[1],
        json!({
            "kind":"row",
            "values":[tagged("int64", "9007199254740993"), tagged("binary", "AAH/")]
        })
    );
    assert_eq!(records[2], json!({"kind":"complete", "rows":1}));

    server.close().await.unwrap();
    database.close().await.unwrap();
}
