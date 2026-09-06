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
    let headers = lines
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
    assert!(discovery_head.body.is_empty());

    let data_query = post_json(
        data,
        "/v1/query",
        json!({"shard_key":"plane-owner", "sql":"SELECT 7 AS value"}),
    )
    .await;
    assert_eq!(data_query.status, 200);
    assert_eq!(data_query.json()["rows"], json!([[7]]));

    for path in ["/health", "/metrics", "/admin"] {
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
    assert_versioned_not_found(&get(data, "/v1/admin/global-indexes").await);
    assert_versioned_not_found(
        &post_json(
            data,
            "/v1/admin/broadcast",
            json!({"sql":"CREATE TABLE leaked_from_data (id INTEGER)"}),
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
    assert!(health_head.body.is_empty());
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
