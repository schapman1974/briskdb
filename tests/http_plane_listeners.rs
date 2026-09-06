#![cfg(feature = "listeners")]

use std::{collections::HashMap, net::SocketAddr};

use briskdb::{
    BriskDb,
    server::{AttachedServer, ListenerConfig},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Duration, timeout},
};

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

async fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> HttpResponse {
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(content_type) = content_type {
        head.push_str(&format!("Content-Type: {content_type}\r\n"));
    }
    for (name, value) in extra_headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");

    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut wire = Vec::new();
    let read = timeout(Duration::from_secs(10), stream.read_to_end(&mut wire))
        .await
        .expect("the HTTP listener should complete the request");
    if let Err(error) = read {
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "{method} {path}: {error}"
        );
        assert!(
            wire.windows(4).any(|window| window == b"\r\n\r\n"),
            "{method} {path} reset before returning an HTTP response"
        );
    }

    let boundary = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response must contain a header terminator");
    let response_head = std::str::from_utf8(&wire[..boundary]).unwrap();
    let mut lines = response_head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let header_lines = lines.collect::<Vec<_>>();
    let request_ids = header_lines
        .iter()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("briskdb-request-id"))
        .map(|(_, value)| value.trim())
        .collect::<Vec<_>>();
    assert_eq!(request_ids.len(), 1, "{method} {path}");
    let request_id = request_ids[0];
    assert_eq!(request_id.len(), 32, "{method} {path}");
    assert!(
        request_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{method} {path}"
    );
    assert_ne!(
        request_id, "00000000000000000000000000000000",
        "{method} {path}"
    );
    let headers = header_lines
        .into_iter()
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_ascii_lowercase(), value.trim().to_owned())
        })
        .collect();
    HttpResponse {
        status,
        headers,
        body: wire[boundary + 4..].to_vec(),
    }
}

async fn get(address: SocketAddr, path: &str) -> HttpResponse {
    request(address, "GET", path, None, &[], &[]).await
}

async fn post_json(address: SocketAddr, path: &str, body: Value) -> HttpResponse {
    let body = serde_json::to_vec(&body).unwrap();
    request(address, "POST", path, Some("application/json"), &body, &[]).await
}

fn assert_versioned_not_found(response: &HttpResponse) {
    assert_eq!(response.status, 404);
    assert_eq!(response.header("briskdb-api-version"), Some("1"));
    assert_eq!(response.json()["code"], "not_found");
}

fn assert_versioned_problem(response: &HttpResponse, expected_status: u16, expected_code: &str) {
    assert_eq!(response.status, expected_status);
    assert_eq!(response.header("briskdb-api-version"), Some("1"));
    assert_eq!(
        response.header("content-type"),
        Some("application/problem+json")
    );
    let body = response.json();
    assert_eq!(body["status"], expected_status);
    assert_eq!(body["code"], expected_code);
    assert_eq!(body.as_object().unwrap().len(), 5);
    assert!(body["type"].as_str().unwrap().contains(':'));
    assert!(!body.to_string().contains("private-sentinel"));
}

fn assert_cancelled_problem(response: &HttpResponse) {
    assert_versioned_problem(response, 500, "cancelled");
}

async fn wait_for_active_query_count(address: SocketAddr, expected: usize) -> Vec<Value> {
    timeout(Duration::from_secs(5), async {
        loop {
            let response = get(address, "/v1/admin/queries").await;
            assert_eq!(response.status, 200);
            assert_eq!(response.header("briskdb-api-version"), Some("1"));
            let body = response.json();
            let queries = body["queries"].as_array().unwrap();
            if queries.len() == expected {
                return queries.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("active-query registry should reach the expected size")
}

fn operation_id(query: &Value) -> &str {
    let id = query["operation_id"].as_str().unwrap();
    assert_eq!(id.len(), 32);
    assert!(
        id.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    id
}

async fn start_unread_query(address: SocketAddr, sql: &str) -> tokio::net::TcpStream {
    let body = serde_json::to_vec(&json!({"shard_key":"dropped-owner", "sql":sql})).unwrap();
    let head = format!(
        "POST /v1/query HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    stream
}

async fn start_unread_stream(address: SocketAddr, sql: &str) -> tokio::net::TcpStream {
    let body = serde_json::to_vec(&json!({"shard_key":"stream-owner", "sql":sql})).unwrap();
    let head = format!(
        "POST /v1/query/stream HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), async {
        loop {
            let mut chunk = [0_u8; 4096];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(
                read > 0,
                "streaming response closed before its metadata frame"
            );
            response.extend_from_slice(&chunk[..read]);
            if response
                .windows(b"\"kind\":\"meta\"".len())
                .any(|window| window == b"\"kind\":\"meta\"")
            {
                break;
            }
        }
    })
    .await
    .expect("the streaming response should produce metadata promptly");
    let boundary = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let headers = std::str::from_utf8(&response[..boundary])
        .unwrap()
        .to_ascii_lowercase();
    assert!(headers.starts_with("http/1.1 200 ok\r\n"));
    assert!(headers.contains("\r\ncontent-type: application/x-ndjson; charset=utf-8\r\n"));
    assert!(headers.contains("\r\nbriskdb-api-version: 1\r\n"));
    let request_ids = headers
        .split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name == &"briskdb-request-id")
        .map(|(_, value)| value.trim())
        .collect::<Vec<_>>();
    assert_eq!(request_ids.len(), 1);
    assert_eq!(request_ids[0].len(), 32);
    assert!(
        request_ids[0]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_ne!(request_ids[0], "00000000000000000000000000000000");
    stream
}

#[tokio::test]
async fn attached_data_and_admin_planes_are_isolated_over_real_tcp() {
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
            admin_listen: Some("127.0.0.1:0".parse().unwrap()),
            postgres_listen: None,
        },
    )
    .await
    .unwrap();
    let data = server.addresses().http();
    let admin = server.addresses().admin().unwrap();
    assert_ne!(data.port(), 0);
    assert_ne!(admin.port(), 0);
    assert_ne!(data, admin);

    let supplied_request_id = "1234567890abcdef1234567890abcdef";
    let fallback = request(
        data,
        "GET",
        "/private-sentinel",
        None,
        &[],
        &[("BriskDB-Request-ID", supplied_request_id)],
    )
    .await;
    assert_eq!(fallback.status, 404);
    assert_eq!(
        fallback.header("briskdb-request-id"),
        Some(supplied_request_id)
    );

    for path in ["/admin", "/admin/assets/app.js"] {
        let browser = request(
            admin,
            "GET",
            path,
            None,
            &[],
            &[("BriskDB-Request-ID", supplied_request_id)],
        )
        .await;
        assert_eq!(browser.status, 200, "{path}");
        assert_eq!(
            browser.header("briskdb-request-id"),
            Some(supplied_request_id),
            "{path}"
        );
    }

    let invalid_browser_id = request(
        admin,
        "GET",
        "/admin/assets/app.js",
        None,
        &[],
        &[("BriskDB-Request-ID", "ABC")],
    )
    .await;
    assert_eq!(invalid_browser_id.status, 400);
    assert_ne!(invalid_browser_id.header("briskdb-request-id"), Some("ABC"));
    assert!(invalid_browser_id.header("briskdb-api-version").is_none());
    assert_eq!(invalid_browser_id.json()["code"], "invalid_argument");
    assert_eq!(invalid_browser_id.json().as_object().unwrap().len(), 5);

    let duplicate_data_id = request(
        data,
        "GET",
        "/v1",
        None,
        &[],
        &[
            ("BriskDB-Request-ID", supplied_request_id),
            ("BriskDB-Request-ID", supplied_request_id),
        ],
    )
    .await;
    assert_versioned_problem(&duplicate_data_id, 400, "invalid_argument");
    assert_ne!(
        duplicate_data_id.header("briskdb-request-id"),
        Some(supplied_request_id)
    );

    for (headers, expected_status, echoed) in [
        (vec![("BriskDB-Request-ID", "ABC")], 400, None),
        (
            vec![
                ("BriskDB-Request-ID", supplied_request_id),
                ("BriskDB-Request-ID", supplied_request_id),
            ],
            400,
            None,
        ),
        (
            vec![
                ("BriskDB-Request-ID", supplied_request_id),
                ("BriskDB-Idempotency-Key", supplied_request_id),
            ],
            501,
            Some(supplied_request_id),
        ),
    ] {
        let response = request(data, "HEAD", "/v1/query", None, &[], &headers).await;
        assert_eq!(response.status, expected_status);
        assert_eq!(response.header("briskdb-api-version"), Some("1"));
        assert_eq!(
            response.header("content-type"),
            Some("application/problem+json")
        );
        assert!(
            response
                .header("content-length")
                .unwrap()
                .parse::<usize>()
                .unwrap()
                > 0
        );
        assert!(response.header("briskdb-idempotency-status").is_none());
        assert!(response.body.is_empty());
        if let Some(echoed) = echoed {
            assert_eq!(response.header("briskdb-request-id"), Some(echoed));
        } else {
            assert_ne!(
                response.header("briskdb-request-id"),
                Some(supplied_request_id)
            );
            assert_ne!(response.header("briskdb-request-id"), Some("ABC"));
        }
    }

    let padded_request_id = format!("  {supplied_request_id}\t");
    let normalized_ows = request(
        data,
        "HEAD",
        "/v1",
        None,
        &[],
        &[("BriskDB-Request-ID", &padded_request_id)],
    )
    .await;
    assert_eq!(normalized_ows.status, 200);
    assert_eq!(
        normalized_ows.header("briskdb-request-id"),
        Some(supplied_request_id)
    );
    assert!(normalized_ows.body.is_empty());

    let discovery = get(data, "/v1").await;
    assert_eq!(discovery.status, 200);
    assert_eq!(discovery.header("briskdb-api-version"), Some("1"));
    assert_eq!(discovery.json()["api_version"], "1");
    let discovery_slash = get(data, "/v1/").await;
    assert_eq!(discovery_slash.status, 200);
    assert_eq!(discovery_slash.json(), discovery.json());
    let discovery_head = request(data, "HEAD", "/v1", None, &[], &[]).await;
    assert_eq!(discovery_head.status, 200);
    assert_eq!(discovery_head.header("briskdb-api-version"), Some("1"));
    assert_eq!(
        discovery_head.header("content-type"),
        Some("application/json")
    );
    assert_eq!(
        discovery_head.header("content-length"),
        discovery.header("content-length")
    );
    assert!(discovery_head.body.is_empty());

    let data_query = post_json(
        data,
        "/v1/query",
        json!({"shard_key":"plane-owner", "sql":"SELECT 7 AS value"}),
    )
    .await;
    assert_eq!(data_query.status, 200);
    assert_eq!(data_query.json()["rows"], json!([[7]]));
    let data_stream = post_json(
        data,
        "/v1/query/stream",
        json!({"shard_key":"plane-owner", "sql":"SELECT 8 AS value"}),
    )
    .await;
    assert_eq!(data_stream.status, 200);
    assert_eq!(
        data_stream.header("content-type"),
        Some("application/x-ndjson; charset=utf-8")
    );

    for path in ["/health", "/ready", "/metrics", "/admin"] {
        let response = get(data, path).await;
        assert_eq!(response.status, 404, "{path}");
        assert!(response.header("briskdb-api-version").is_none(), "{path}");
        assert!(response.header("set-cookie").is_none(), "{path}");
    }
    let leaked_login = post_json(
        data,
        "/admin/api/login",
        json!({"username":"admin", "password":"admin"}),
    )
    .await;
    assert_eq!(leaked_login.status, 404);
    assert!(leaked_login.header("set-cookie").is_none());
    assert_versioned_not_found(&get(data, "/v1/health").await);
    for path in [
        "/v1/ready",
        "/v1/admin/global-indexes",
        "/v1/admin/catalog",
        "/v1/admin/migrations",
        "/v1/admin/migrations/1",
        "/v1/admin/shards",
        "/v1/admin/queries",
        "/v1/admin/backup",
    ] {
        assert_versioned_not_found(&get(data, path).await);
    }
    assert_versioned_not_found(
        &post_json(
            data,
            "/v1/admin/broadcast",
            json!({"sql":"CREATE TABLE leaked_from_data (id INTEGER)"}),
        )
        .await,
    );
    assert_versioned_not_found(
        &post_json(data, "/v1/admin/maintenance/checkpoint", json!({})).await,
    );
    assert_versioned_not_found(
        &request(
            data,
            "POST",
            "/v1/admin/queries/0123456789abcdef0123456789abcdef/cancel",
            None,
            &[],
            &[],
        )
        .await,
    );

    let health = get(admin, "/health").await;
    assert_eq!(health.status, 200);
    assert!(health.header("briskdb-api-version").is_none());
    assert_eq!(health.json()["status"], "ok");
    let versioned_health = get(admin, "/v1/health").await;
    assert_eq!(versioned_health.status, 200);
    assert_eq!(versioned_health.header("briskdb-api-version"), Some("1"));
    let health_head = request(admin, "HEAD", "/v1/health", None, &[], &[]).await;
    assert_eq!(health_head.status, 200);
    assert_eq!(health_head.header("briskdb-api-version"), Some("1"));
    assert_eq!(health_head.header("content-type"), Some("application/json"));
    assert_eq!(
        health_head.header("content-length"),
        versioned_health.header("content-length")
    );
    assert!(health_head.body.is_empty());
    let ready = get(admin, "/ready").await;
    assert_eq!(ready.status, 200);
    assert!(ready.header("briskdb-api-version").is_none());
    assert_eq!(ready.json()["ready"], true);
    let versioned_ready = get(admin, "/v1/ready").await;
    assert_eq!(versioned_ready.status, 200);
    assert_eq!(versioned_ready.header("briskdb-api-version"), Some("1"));
    assert_eq!(versioned_ready.json(), ready.json());
    let metrics = get(admin, "/metrics").await;
    assert_eq!(metrics.status, 200);
    assert!(metrics.header("briskdb-api-version").is_none());
    assert!(
        metrics
            .header("content-type")
            .unwrap()
            .starts_with("text/plain")
    );
    let shell = get(admin, "/admin").await;
    assert_eq!(shell.status, 200);
    assert!(
        String::from_utf8(shell.body)
            .unwrap()
            .contains("BriskDB Data Browser")
    );

    let login = post_json(
        admin,
        "/admin/api/login",
        json!({"username":"admin", "password":"admin"}),
    )
    .await;
    assert_eq!(login.status, 200);
    let cookie = login
        .header("set-cookie")
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let session = request(
        admin,
        "GET",
        "/admin/api/session",
        None,
        &[],
        &[("Cookie", cookie)],
    )
    .await;
    assert_eq!(session.status, 200);
    assert_eq!(session.json()["authenticated"], true);

    assert_versioned_not_found(&get(admin, "/v1").await);
    assert_versioned_not_found(&get(admin, "/v1/").await);
    assert_versioned_not_found(
        &post_json(
            admin,
            "/v1/query",
            json!({"shard_key":"plane-owner", "sql":"SELECT 99"}),
        )
        .await,
    );
    assert_versioned_not_found(
        &post_json(
            admin,
            "/v1/query/stream",
            json!({"shard_key":"plane-owner", "sql":"SELECT 99"}),
        )
        .await,
    );
    assert_versioned_not_found(
        &post_json(
            admin,
            "/v1/execute",
            json!({"shard_key":"plane-owner", "sql":"INSERT INTO plane_items VALUES (99)"}),
        )
        .await,
    );

    let migration = post_json(
        admin,
        "/v1/admin/broadcast",
        json!({"sql":"CREATE TABLE plane_items (id INTEGER PRIMARY KEY)"}),
    )
    .await;
    assert_eq!(migration.status, 200);
    assert_eq!(migration.header("briskdb-api-version"), Some("1"));
    assert_eq!(migration.json()["completed_shards"], json!([0, 1]));
    assert_eq!(get(admin, "/v1/admin/global-indexes").await.status, 200);
    let catalog = get(admin, "/v1/admin/catalog").await;
    assert_eq!(catalog.status, 200);
    assert_eq!(catalog.json()["schema_generation"], "1");
    assert_eq!(catalog.json()["databases"][0]["name"], "default");
    let migrations = get(admin, "/v1/admin/migrations").await;
    assert_eq!(migrations.status, 200);
    let migrations_body = migrations.json();
    assert_eq!(migrations_body["schema_generation"], "1");
    assert!(migrations_body["active"].is_null());
    let latest_complete = &migrations_body["latest_complete"];
    assert_eq!(latest_complete["generation"], "1");
    assert_eq!(latest_complete.as_object().unwrap().len(), 8);
    assert!(latest_complete.get("id").is_none());
    assert!(latest_complete.get("digest").is_none());
    assert!(!String::from_utf8_lossy(&migrations.body).contains("CREATE TABLE"));
    let exact_migration = get(admin, "/v1/admin/migrations/1").await;
    assert_eq!(exact_migration.status, 200);
    assert_eq!(exact_migration.json(), latest_complete.clone());
    let shards = get(admin, "/v1/admin/shards").await;
    assert_eq!(shards.status, 200);
    assert_eq!(
        shards.json(),
        json!({
            "schema_generation":"1",
            "shards":[{"id":0,"state":"ready"},{"id":1,"state":"ready"}]
        })
    );
    assert_eq!(
        get(admin, "/v1/admin/queries").await.json(),
        json!({"queries":[]})
    );
    let backup = get(admin, "/v1/admin/backup").await;
    assert_eq!(backup.status, 200);
    assert_eq!(backup.json()["online"], false);
    assert_eq!(backup.json()["requires_all_processes_stopped"], true);
    assert_eq!(backup.json()["checkpoint_is_recovery_point"], false);
    let checkpoint = post_json(admin, "/v1/admin/maintenance/checkpoint", json!({})).await;
    assert_eq!(checkpoint.status, 200);
    assert_eq!(checkpoint.json()["operation"], "passive_checkpoint");
    assert_eq!(checkpoint.json()["recovery_point"], false);

    let write = post_json(
        data,
        "/v1/execute",
        json!({
            "shard_key":"plane-owner",
            "sql":"INSERT INTO plane_items (id) VALUES (?1)",
            "params":[1]
        }),
    )
    .await;
    assert_eq!(write.status, 200);
    assert_eq!(write.json()["rows_affected"], 1);
    let read = post_json(
        data,
        "/v1/query",
        json!({"shard_key":"plane-owner", "sql":"SELECT id FROM plane_items"}),
    )
    .await;
    assert_eq!(read.status, 200);
    assert_eq!(read.json()["rows"], json!([[1]]));

    let (concurrent_data, concurrent_admin) = tokio::join!(
        post_json(
            data,
            "/v1/query",
            json!({"shard_key":"plane-owner", "sql":"SELECT id FROM plane_items"}),
        ),
        get(admin, "/v1/health")
    );
    assert_eq!(concurrent_data.status, 200);
    assert_eq!(concurrent_admin.status, 200);

    assert!(!server.close().await.unwrap());
    assert!(tokio::net::TcpStream::connect(data).await.is_err());
    assert!(tokio::net::TcpStream::connect(admin).await.is_err());
    assert_eq!(database.state(), briskdb::core::EngineState::Running);
    database.close().await.unwrap();
}

#[tokio::test]
async fn admin_cancels_only_the_selected_data_query_and_disconnect_unregisters_its_handle() {
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
            admin_listen: Some("127.0.0.1:0".parse().unwrap()),
            postgres_listen: None,
        },
    )
    .await
    .unwrap();
    let data = server.addresses().data();
    let admin = server.addresses().admin().unwrap();
    let slow_sql = "WITH RECURSIVE numbers(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM numbers WHERE value < 1000000000) SELECT sum(value) FROM numbers";
    let streaming_sql = "WITH RECURSIVE numbers(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM numbers WHERE value < 1000000000) SELECT value FROM numbers";

    let first = tokio::spawn(async move {
        post_json(
            data,
            "/v1/query",
            json!({"shard_key":"cancel-one", "sql":slow_sql}),
        )
        .await
    });
    let first_status = wait_for_active_query_count(admin, 1).await;
    assert_eq!(first_status[0].as_object().unwrap().len(), 4);
    assert!(first_status[0].get("sql_digest").is_none());
    assert!(first_status[0].get("digest").is_none());
    let first_id = operation_id(&first_status[0]).to_owned();
    assert_eq!(first_status[0]["sql_bytes"], slow_sql.len());
    assert_eq!(first_status[0]["cancellation_requested"], false);
    assert!(
        !serde_json::to_string(&first_status)
            .unwrap()
            .contains(slow_sql)
    );

    let rejected_nonempty = request(
        admin,
        "POST",
        &format!("/v1/admin/queries/{first_id}/cancel"),
        None,
        b"private-sentinel",
        &[],
    )
    .await;
    assert_versioned_problem(&rejected_nonempty, 400, "invalid_argument");
    let still_active = wait_for_active_query_count(admin, 1).await;
    assert_eq!(operation_id(&still_active[0]), first_id);
    assert_eq!(still_active[0]["cancellation_requested"], false);

    let oversized_cancel_body = vec![b'x'; 2 * 1024 * 1024 + 1];
    let rejected_oversized = request(
        admin,
        "POST",
        &format!("/v1/admin/queries/{first_id}/cancel"),
        None,
        &oversized_cancel_body,
        &[],
    )
    .await;
    assert_versioned_problem(&rejected_oversized, 413, "request_too_large");
    let still_active = wait_for_active_query_count(admin, 1).await;
    assert_eq!(operation_id(&still_active[0]), first_id);
    assert_eq!(still_active[0]["cancellation_requested"], false);

    let second = tokio::spawn(async move {
        post_json(
            data,
            "/v1/query",
            json!({"shard_key":"cancel-two", "sql":slow_sql}),
        )
        .await
    });
    let both = wait_for_active_query_count(admin, 2).await;
    let second_id = both
        .iter()
        .map(operation_id)
        .find(|id| *id != first_id)
        .unwrap()
        .to_owned();

    let cancel_second = request(
        admin,
        "POST",
        &format!("/v1/admin/queries/{second_id}/cancel"),
        None,
        &[],
        &[],
    )
    .await;
    assert_eq!(cancel_second.status, 202);
    assert_eq!(cancel_second.header("briskdb-api-version"), Some("1"));
    assert_eq!(
        cancel_second.json(),
        json!({"operation_id":second_id, "newly_requested":true})
    );
    let second_response = timeout(Duration::from_secs(5), second)
        .await
        .expect("selected query should stop promptly")
        .unwrap();
    assert_cancelled_problem(&second_response);

    let still_active = wait_for_active_query_count(admin, 1).await;
    assert_eq!(operation_id(&still_active[0]), first_id);
    assert_eq!(still_active[0]["cancellation_requested"], false);
    assert_versioned_not_found(
        &request(
            admin,
            "POST",
            &format!("/v1/admin/queries/{second_id}/cancel"),
            None,
            &[],
            &[],
        )
        .await,
    );

    let cancel_first = request(
        admin,
        "POST",
        &format!("/v1/admin/queries/{first_id}/cancel"),
        None,
        &[],
        &[],
    )
    .await;
    assert_eq!(cancel_first.status, 202);
    assert_eq!(cancel_first.json()["newly_requested"], true);
    let first_response = timeout(Duration::from_secs(5), first)
        .await
        .expect("remaining query should stop promptly")
        .unwrap();
    assert_cancelled_problem(&first_response);
    assert!(wait_for_active_query_count(admin, 0).await.is_empty());

    let later = post_json(
        data,
        "/v1/query",
        json!({"shard_key":"cancel-one", "sql":"SELECT 7 AS value"}),
    )
    .await;
    assert_eq!(later.status, 200);
    assert_eq!(later.json()["rows"], json!([[7]]));

    let unread = start_unread_query(data, slow_sql).await;
    let dropped = wait_for_active_query_count(admin, 1).await;
    let dropped_id = operation_id(&dropped[0]).to_owned();
    drop(unread);
    assert!(wait_for_active_query_count(admin, 0).await.is_empty());
    assert_versioned_not_found(
        &request(
            admin,
            "POST",
            &format!("/v1/admin/queries/{dropped_id}/cancel"),
            None,
            &[],
            &[],
        )
        .await,
    );
    let after_disconnect = post_json(
        data,
        "/v1/query",
        json!({"shard_key":"dropped-owner", "sql":"SELECT 11 AS value"}),
    )
    .await;
    assert_eq!(after_disconnect.status, 200);
    assert_eq!(after_disconnect.json()["rows"], json!([[11]]));

    let unread_stream = start_unread_stream(data, streaming_sql).await;
    let streamed = wait_for_active_query_count(admin, 1).await;
    let streamed_id = operation_id(&streamed[0]).to_owned();
    drop(unread_stream);
    assert!(wait_for_active_query_count(admin, 0).await.is_empty());
    assert_versioned_not_found(
        &request(
            admin,
            "POST",
            &format!("/v1/admin/queries/{streamed_id}/cancel"),
            None,
            &[],
            &[],
        )
        .await,
    );
    let after_stream_disconnect = post_json(
        data,
        "/v1/query",
        json!({"shard_key":"stream-owner", "sql":"SELECT 13 AS value"}),
    )
    .await;
    assert_eq!(after_stream_disconnect.status, 200);
    assert_eq!(after_stream_disconnect.json()["rows"], json!([[13]]));

    assert!(!server.close().await.unwrap());
    assert_eq!(database.state(), briskdb::core::EngineState::Running);
    database.close().await.unwrap();
}

#[tokio::test]
async fn attached_server_can_disable_the_admin_plane_and_restart_it() {
    let temp = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(temp.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut data_only = AttachedServer::start(
        &database,
        ListenerConfig {
            http_listen: "127.0.0.1:0".parse().unwrap(),
            admin_listen: None,
            postgres_listen: None,
        },
    )
    .await
    .unwrap();
    assert!(data_only.addresses().admin().is_none());
    assert_eq!(get(data_only.addresses().http(), "/v1").await.status, 200);
    assert_versioned_not_found(&get(data_only.addresses().http(), "/v1/ready").await);
    assert_eq!(
        get(data_only.addresses().http(), "/admin").await.status,
        404
    );
    data_only.close().await.unwrap();

    let mut restarted = AttachedServer::start(
        &database,
        ListenerConfig {
            http_listen: "127.0.0.1:0".parse().unwrap(),
            admin_listen: Some("127.0.0.1:0".parse().unwrap()),
            postgres_listen: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        get(restarted.addresses().admin().unwrap(), "/health")
            .await
            .status,
        200
    );
    restarted.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn unsafe_or_conflicting_admin_bindings_fail_without_leaking_a_data_listener() {
    let temp = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(temp.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();

    let error = AttachedServer::start(
        &database,
        ListenerConfig {
            http_listen: "127.0.0.1:0".parse().unwrap(),
            admin_listen: Some("0.0.0.0:0".parse().unwrap()),
            postgres_listen: None,
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("admin"));
    assert!(error.to_string().contains("loopback"));

    let data_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let data_address = data_reservation.local_addr().unwrap();
    let admin_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let admin_address = admin_reservation.local_addr().unwrap();
    drop(data_reservation);
    let error = AttachedServer::start(
        &database,
        ListenerConfig {
            http_listen: data_address,
            admin_listen: Some(admin_address),
            postgres_listen: None,
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("admin"));
    let rebound = std::net::TcpListener::bind(data_address)
        .expect("an admin bind failure must release the already-bound data listener");
    drop(rebound);
    drop(admin_reservation);

    assert_eq!(database.state(), briskdb::core::EngineState::Running);
    let session = database.owned_session();
    session
        .set_routing_key("after-listener-error")
        .await
        .unwrap();
    assert_eq!(
        session
            .query(briskdb::core::Statement::new("SELECT 1", vec![]))
            .await
            .unwrap()
            .value
            .rows()
            .len(),
        1
    );
    session.close().await.unwrap();
    database.close().await.unwrap();
}
