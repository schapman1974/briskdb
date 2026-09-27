use super::*;
use crate::document::*;

#[tokio::test]
async fn every_non_cursor_command_derives_complete_exact_document_requirements() {
    use Action::*;
    use DocumentCommand as C;
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    let ns = || DocumentNamespace::new("App.A", "items.x").unwrap();
    let read = DocumentReadOptions::new;
    let write = DocumentWriteOptions::new;
    let filter = DocumentFilter::empty;
    let row = || BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap();
    let index = || {
        DocumentIndexRequest::new(BsonDocument::from_entries([("x", BsonValue::Int32(1))]).unwrap())
            .unwrap()
    };
    let update = || {
        DocumentUpdateRequest::new(
            ns(),
            filter(),
            DocumentUpdate::new(
                BsonDocument::from_entries([("$set", BsonValue::Document(row()))]).unwrap(),
            )
            .unwrap(),
            DocumentMutationScope::One,
            write().with_upsert(true),
        )
    };
    let replace =
        || DocumentReplaceRequest::new(ns(), filter(), row(), write().with_upsert(true)).unwrap();
    let cases = [
        (
            C::CreateCollection(DocumentCreateCollectionRequest::new(
                ns(),
                DocumentCollectionOptions::empty(),
                write(),
            )),
            vec![ConnectDatabase, CreateObject, CreateDatabase],
        ),
        (
            C::CollectionExists(DocumentCollectionExistsRequest::new(ns())),
            vec![ConnectDatabase, ListObjects],
        ),
        (
            C::ListCollections(DocumentListCollectionsRequest::new("App.A", read()).unwrap()),
            vec![ConnectDatabase, ListObjects],
        ),
        (
            C::ListCollectionMetadata(
                DocumentListCollectionMetadataRequest::new("App.A", filter(), true, read())
                    .unwrap(),
            ),
            vec![ConnectDatabase, ListObjects],
        ),
        (
            C::ListDatabaseNames(DocumentListDatabaseNamesRequest::new(filter())),
            vec![ListDatabases],
        ),
        (
            C::DropCollection(DocumentDropCollectionRequest::new(ns(), write())),
            vec![ConnectDatabase, DropObject],
        ),
        (
            C::DropDatabase(DocumentDropDatabaseRequest::new("App.A", write()).unwrap()),
            vec![ConnectDatabase, DropDatabase],
        ),
        (
            C::Find(DocumentFindRequest::new(ns(), filter(), read())),
            vec![ConnectDatabase, ReadData],
        ),
        (
            C::FindOneAndDelete(DocumentFindOneAndDeleteRequest::new(ns(), filter(), read())),
            vec![ConnectDatabase, ReadData, DeleteData],
        ),
        (
            C::FindOneAndReplace(DocumentFindOneAndReplaceRequest::new(replace(), read())),
            vec![ConnectDatabase, UpdateData, InsertData, ReadData],
        ),
        (
            C::FindOneAndUpdate(DocumentFindOneAndUpdateRequest::new(update(), read())),
            vec![ConnectDatabase, UpdateData, InsertData, ReadData],
        ),
        (
            C::Aggregate(
                DocumentAggregateRequest::new(ns(), DocumentPipeline::new(vec![]).unwrap(), read())
                    .unwrap(),
            ),
            vec![ConnectDatabase, ReadData],
        ),
        (
            C::Count(DocumentCountRequest::new(ns(), filter(), read())),
            vec![ConnectDatabase, ReadData],
        ),
        (
            C::Distinct(DocumentDistinctRequest::new(ns(), "x", filter(), read()).unwrap()),
            vec![ConnectDatabase, ReadData],
        ),
        (
            C::Insert(DocumentInsertRequest::new(ns(), vec![row()], write()).unwrap()),
            vec![ConnectDatabase, InsertData],
        ),
        (
            C::Update(update()),
            vec![ConnectDatabase, UpdateData, InsertData],
        ),
        (
            C::Replace(replace()),
            vec![ConnectDatabase, UpdateData, InsertData],
        ),
        (
            C::Delete(DocumentDeleteRequest::new(
                ns(),
                filter(),
                DocumentMutationScope::Many,
                write(),
            )),
            vec![ConnectDatabase, DeleteData],
        ),
        (
            C::CreateIndex(DocumentCreateIndexRequest::new(ns(), index(), write())),
            vec![ConnectDatabase, CreateIndex],
        ),
        (
            C::CreateBuiltIndex(DocumentCreateIndexRequest::new(ns(), index(), write())),
            vec![ConnectDatabase, CreateIndex],
        ),
        (
            C::CreateIndexes(
                DocumentCreateIndexesRequest::new(ns(), vec![index()], write()).unwrap(),
            ),
            vec![ConnectDatabase, CreateIndex],
        ),
        (
            C::BuildIndex(DocumentBuildIndexRequest::new(ns(), "x", write()).unwrap()),
            vec![ConnectDatabase, CreateIndex],
        ),
        (
            C::DropIndex(DocumentDropIndexRequest::new(ns(), "x", write()).unwrap()),
            vec![ConnectDatabase, DropIndex],
        ),
        (
            C::DropIndexes(DocumentDropIndexesRequest::all(ns(), write())),
            vec![ConnectDatabase, DropIndex],
        ),
        (
            C::ListIndexes(DocumentListIndexesRequest::new(ns(), read())),
            vec![ConnectDatabase, ListIndexes],
        ),
        (
            C::ListIndexMetadata(DocumentListIndexMetadataRequest::new(ns(), read())),
            vec![ConnectDatabase, ListIndexes],
        ),
    ];
    assert_eq!(cases.len(), 26);
    for (command, expected_actions) in cases {
        let requirements = engine.document_requirements(&session, &command).unwrap();
        assert_eq!(
            requirements
                .iter()
                .map(|(action, _)| *action)
                .collect::<Vec<_>>(),
            expected_actions,
            "{:?}",
            command.kind()
        );
        for (action, resource) in requirements {
            let expected = match action.resource_kind() {
                crate::core::authorization::ResourceKind::DataDomain => {
                    Resource::data_domain(DataDomain::Document)
                }
                crate::core::authorization::ResourceKind::Database => {
                    Resource::database(DataDomain::Document, "App.A").unwrap()
                }
                crate::core::authorization::ResourceKind::Object => {
                    Resource::object(DataDomain::Document, "App.A", "items.x").unwrap()
                }
                _ => panic!("unexpected document resource kind"),
            };
            assert_eq!(resource, expected);
        }
    }
}

#[test]
fn non_upserting_mutations_do_not_require_unrelated_insert_or_read_privileges() {
    let ns = DocumentNamespace::new("app", "items").unwrap();
    assert_eq!(
        mutation(&ns, false, false)
            .unwrap()
            .iter()
            .map(|(action, _)| *action)
            .collect::<Vec<_>>(),
        [Action::ConnectDatabase, Action::UpdateData]
    );
}
