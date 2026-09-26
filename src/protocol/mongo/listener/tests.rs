use super::*;
use crate::{
    DocumentSupport,
    protocol::mongo::{MongoListenerState, MongoReadinessReason},
};

#[tokio::test]
async fn aborted_listener_fails_readiness_even_before_first_poll_and_close_is_repeatable() {
    for poll in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let database = BriskDb::builder(root.path())
            .with_shard_count(2)
            .with_document_support(DocumentSupport::Enabled)
            .open()
            .await
            .unwrap();
        let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        if poll {
            let _stream = TcpStream::connect(server.address()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while server.metrics().accepted_connections == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        server.task.as_ref().unwrap().abort();
        let error = server.close().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(
            server.readiness().reason(),
            Some(MongoReadinessReason::ListenerFailed)
        );
        assert!(!server.readiness().ready());
        assert!(database.engine().readiness().ready());
        server.close().await.unwrap();
        assert_eq!(server.readiness().listener, MongoListenerState::Failed);
        assert_eq!(
            server.readiness().reason(),
            Some(MongoReadinessReason::ListenerFailed)
        );
        database.close().await.unwrap();
    }
}
