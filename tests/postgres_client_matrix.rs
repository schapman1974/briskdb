use std::{env, error::Error, time::Duration};

use briskdb::core::{
    Database, GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType,
    GlobalIndexStorageTopology, UniqueNullSemantics,
};
use tokio_postgres::{NoTls, SimpleQueryMessage, error::SqlState};

const MATRIX_DSN_ENV: &str = "BRISKDB_POSTGRES_MATRIX_DSN";
const MATRIX_ROOT_ENV: &str = "BRISKDB_POSTGRES_MATRIX_ROOT";

#[test]
#[ignore = "requires the imported matrix root prepared by tests/postgres_client_matrix.sh"]
fn prepare_postgres_client_global_index() {
    let root = env::var(MATRIX_ROOT_ENV).expect("matrix root");
    let mut database = Database::open(root, 2).unwrap();
    let table_id = database
        .catalog()
        .table("default", "indexed_records")
        .unwrap()
        .unwrap()
        .id();
    let declaration = GlobalIndexDeclaration::new(
        table_id,
        "indexed_records_payload_unique",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("payload").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unwrap()
    .unique(UniqueNullSemantics::Distinct)
    .with_topology(GlobalIndexStorageTopology::selected_v1());
    let index_id = database.create_global_index(declaration).unwrap();
    database.build_global_index(index_id).unwrap();
}

async fn connect(
    dsn: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    ),
    tokio_postgres::Error,
> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    let connection = tokio::spawn(connection);
    Ok((client, connection))
}

async fn close(
    client: tokio_postgres::Client,
    connection: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) -> Result<(), Box<dyn Error>> {
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), connection)
        .await??
        .map_err(Into::into)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires the live listener prepared by tests/postgres_client_matrix.sh"]
async fn tokio_postgres_client_matrix() -> Result<(), Box<dyn Error>> {
    let dsn = env::var(MATRIX_DSN_ENV)
        .map_err(|_| format!("{MATRIX_DSN_ENV} must name the live BriskDB listener"))?;
    let (mut client, connection) = connect(&dsn).await?;

    eprintln!("tokio-postgres: indexed autocommit write");
    assert_eq!(
        client
            .execute(
                "INSERT INTO indexed_records (tenant_id, payload) VALUES ($1, $2)",
                &[&"tokio-index-a", &"tokio-global-key"],
            )
            .await?,
        1
    );
    let duplicate = client
        .execute(
            "INSERT INTO indexed_records (tenant_id, payload) VALUES ($1, $2)",
            &[&"tokio-index-b", &"tokio-global-key"],
        )
        .await
        .unwrap_err();
    assert_eq!(duplicate.code(), Some(&SqlState::UNIQUE_VIOLATION));
    assert_eq!(
        client.query_one("SELECT 1", &[]).await?.get::<_, String>(0),
        "1"
    );

    eprintln!("tokio-postgres: begin CRUD transaction");
    let transaction = client.transaction().await?;
    eprintln!("tokio-postgres: insert");
    assert_eq!(
        transaction
            .execute(
                "INSERT INTO records (tenant_id, payload) VALUES ($1, $2)",
                &[&"tokio-client", &"created"],
            )
            .await?,
        1
    );
    eprintln!("tokio-postgres: select created");
    let row = transaction
        .query_one(
            "SELECT payload FROM records WHERE tenant_id = $1",
            &[&"tokio-client"],
        )
        .await?;
    assert_eq!(row.get::<_, String>(0), "created");
    eprintln!("tokio-postgres: update");
    assert_eq!(
        transaction
            .execute(
                "UPDATE records SET payload = $1 WHERE tenant_id = $2",
                &[&"updated", &"tokio-client"],
            )
            .await?,
        1
    );
    eprintln!("tokio-postgres: select updated");
    let row = transaction
        .query_one(
            "SELECT payload FROM records WHERE tenant_id = $1",
            &[&"tokio-client"],
        )
        .await?;
    assert_eq!(row.get::<_, String>(0), "updated");
    eprintln!("tokio-postgres: delete");
    assert_eq!(
        transaction
            .execute(
                "DELETE FROM records WHERE tenant_id = $1",
                &[&"tokio-client"],
            )
            .await?,
        1
    );
    eprintln!("tokio-postgres: commit CRUD transaction");
    transaction.commit().await?;

    eprintln!("tokio-postgres: begin failure transaction");
    let transaction = client.transaction().await?;
    eprintln!("tokio-postgres: expected unsupported probe");
    let error = transaction.simple_query("SHOW work_mem").await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED));
    eprintln!("tokio-postgres: rollback failed transaction");
    transaction.rollback().await?;
    eprintln!("tokio-postgres: recover on same connection");
    let messages = client.simple_query("SELECT 1").await?;
    assert!(messages.iter().any(|message| {
        matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some("1"))
    }));
    eprintln!("tokio-postgres: close first connection");
    close(client, connection).await?;

    eprintln!("tokio-postgres: reconnect");
    let (client, connection) = connect(&dsn).await?;
    assert_eq!(
        client.query_one("SELECT 1", &[]).await?.get::<_, String>(0),
        "1"
    );
    close(client, connection).await
}
