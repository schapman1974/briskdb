use briskdb::document::{
    BsonValue, DocumentCollectionMetadata, DocumentExecution, DocumentIndexLifecycle,
    DocumentIndexMetadata, DocumentPlan, DocumentRequestId, DocumentResult,
};
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};

use crate::bson::{
    PythonBsonOutputTypes, PythonUuidRepresentation, bson_document_to_python, bson_value_to_python,
    uuid_bytes_to_python,
};

pub(crate) fn execution_to_python(
    py: Python<'_>,
    execution: DocumentExecution,
    uuid_representation: PythonUuidRepresentation,
    bson_types: &PythonBsonOutputTypes,
) -> PyResult<Py<PyAny>> {
    let (request_id, plan, result) = execution.into_parts();
    let output = PyDict::new(py);
    output.set_item(
        "request_id",
        request_id_to_python(py, request_id, bson_types)?,
    )?;
    match plan {
        Some(plan) => output.set_item("plan", plan_to_python(py, &plan)?)?,
        None => output.set_item("plan", py.None())?,
    }

    match result {
        DocumentResult::Document(document) => {
            output.set_item("kind", "document")?;
            output.set_item("did_upsert", false)?;
            output.set_item("upserted_id", py.None())?;
            match document {
                Some(document) => output.set_item(
                    "document",
                    bson_document_to_python(py, &document, uuid_representation, bson_types)?,
                )?,
                None => output.set_item("document", py.None())?,
            }
        }
        DocumentResult::UpsertedDocument(result) => {
            let (id, document) = result.into_parts();
            output.set_item("kind", "document")?;
            output.set_item("did_upsert", true)?;
            output.set_item(
                "upserted_id",
                bson_value_to_python(py, &id, uuid_representation, bson_types)?,
            )?;
            match document {
                Some(document) => output.set_item(
                    "document",
                    bson_document_to_python(py, &document, uuid_representation, bson_types)?,
                )?,
                None => output.set_item("document", py.None())?,
            }
        }
        DocumentResult::DatabaseNames(names) => {
            output.set_item("kind", "database_names")?;
            output.set_item("names", names.into_vec())?;
        }
        DocumentResult::NamespaceDropped(existed) => {
            output.set_item("kind", "namespace_dropped")?;
            output.set_item("existed", existed)?;
        }
        DocumentResult::CollectionExists(exists) => {
            output.set_item("kind", "collection_exists")?;
            output.set_item("exists", exists)?;
        }
        DocumentResult::Collection(collection) => {
            output.set_item("kind", "collection")?;
            output.set_item(
                "collection",
                collection_to_python(py, &collection, uuid_representation, bson_types)?,
            )?;
        }
        DocumentResult::Collections(collections) => {
            output.set_item("kind", "collections")?;
            let values = PyList::empty(py);
            for collection in &collections {
                values.append(collection_to_python(
                    py,
                    collection,
                    uuid_representation,
                    bson_types,
                )?)?;
            }
            output.set_item("collections", values)?;
        }
        DocumentResult::Cursor(batch) => {
            output.set_item("kind", "cursor")?;
            output.set_item("namespace", namespace_to_python(py, batch.namespace())?)?;
            match batch.cursor_id() {
                Some(cursor_id) => output.set_item("cursor_id", cursor_id.get())?,
                None => output.set_item("cursor_id", py.None())?,
            }
            output.set_item("exhausted", batch.is_exhausted())?;
            let documents = PyList::empty(py);
            for document in batch.documents() {
                documents.append(bson_document_to_python(
                    py,
                    document,
                    uuid_representation,
                    bson_types,
                )?)?;
            }
            output.set_item("documents", documents)?;
        }
        DocumentResult::Count(count) => {
            output.set_item("kind", "count")?;
            output.set_item("count", count)?;
        }
        DocumentResult::Distinct(values) => {
            output.set_item("kind", "distinct")?;
            let output_values = PyList::empty(py);
            for value in &values {
                output_values.append(bson_value_to_python(
                    py,
                    value,
                    uuid_representation,
                    bson_types,
                )?)?;
            }
            output.set_item("values", output_values)?;
        }
        DocumentResult::Insert(result) => {
            output.set_item("kind", "insert")?;
            output.set_item("acknowledged", result.acknowledged())?;
            output.set_item("inserted_count", result.inserted_ids().len())?;
            let ids = PyList::empty(py);
            for value in result.inserted_ids() {
                ids.append(bson_value_to_python(
                    py,
                    value,
                    uuid_representation,
                    bson_types,
                )?)?;
            }
            output.set_item("inserted_ids", ids)?;
        }
        DocumentResult::Update(result) => {
            output.set_item("kind", "update")?;
            output.set_item("acknowledged", result.acknowledged())?;
            output.set_item("matched_count", result.matched_count())?;
            output.set_item("modified_count", result.modified_count())?;
            output.set_item("did_upsert", result.did_upsert())?;
            match result.upserted_id() {
                Some(id) => output.set_item(
                    "upserted_id",
                    bson_value_to_python(py, id, uuid_representation, bson_types)?,
                )?,
                None => output.set_item("upserted_id", py.None())?,
            }
        }
        DocumentResult::Delete(result) => {
            output.set_item("kind", "delete")?;
            output.set_item("acknowledged", result.acknowledged())?;
            output.set_item("deleted_count", result.deleted_count())?;
        }
        DocumentResult::IndexName(name) => {
            output.set_item("kind", "index_name")?;
            output.set_item("index_name", name)?;
            output.set_item("lifecycle", "pending_build")?;
        }
        DocumentResult::IndexReady(name) => {
            output.set_item("kind", "index_name")?;
            output.set_item("index_name", name)?;
            output.set_item("lifecycle", "ready")?;
        }
        DocumentResult::Indexes(indexes) => {
            output.set_item("kind", "indexes")?;
            let values = PyList::empty(py);
            for index in &indexes {
                values.append(index_to_python(py, index, uuid_representation, bson_types)?)?;
            }
            output.set_item("indexes", values)?;
        }
        DocumentResult::CursorKilled(killed) => {
            output.set_item("kind", "cursor_killed")?;
            output.set_item("killed", killed)?;
        }
        DocumentResult::Acknowledged(acknowledged) => {
            output.set_item("kind", "acknowledged")?;
            output.set_item("acknowledged", acknowledged)?;
        }
        _ => {
            return Err(crate::error::unsupported(
                "this document result is unknown to the current Python API",
            ));
        }
    }
    Ok(output.into_any().unbind())
}

fn request_id_to_python(
    py: Python<'_>,
    request_id: DocumentRequestId,
    bson_types: &PythonBsonOutputTypes,
) -> PyResult<Py<PyAny>> {
    uuid_bytes_to_python(py, request_id.as_bytes(), bson_types)
}

fn plan_to_python(py: Python<'_>, plan: &DocumentPlan) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    match plan {
        DocumentPlan::Point(point) => {
            output.set_item("kind", "point")?;
            output.set_item("collection_id", point.collection_id().get())?;
            output.set_item("shards", vec![point.shard()])?;
        }
        DocumentPlan::Scatter(scatter) => {
            output.set_item("kind", "scatter")?;
            output.set_item("collection_id", scatter.collection_id().get())?;
            output.set_item("shards", scatter.shards().to_vec())?;
        }
        _ => {
            return Err(crate::error::unsupported(
                "this document plan is unknown to the current Python API",
            ));
        }
    }
    Ok(output.into_any().unbind())
}

fn namespace_to_python(
    py: Python<'_>,
    namespace: &briskdb::document::DocumentNamespace,
) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    output.set_item("database", namespace.database())?;
    output.set_item("collection", namespace.collection())?;
    Ok(output.into_any().unbind())
}

fn collection_to_python(
    py: Python<'_>,
    collection: &DocumentCollectionMetadata,
    uuid_representation: PythonUuidRepresentation,
    bson_types: &PythonBsonOutputTypes,
) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    output.set_item("id", collection.id().get())?;
    output.set_item("database_id", collection.database_id().get())?;
    output.set_item("database", collection.database_name())?;
    output.set_item("name", collection.name())?;
    output.set_item("namespace", collection.namespace())?;
    output.set_item(
        "options",
        bson_document_to_python(
            py,
            collection.options().document(),
            uuid_representation,
            bson_types,
        )?,
    )?;
    let placement = PyDict::new(py);
    placement.set_item("code", collection.placement().code())?;
    placement.set_item("version", collection.placement().version())?;
    output.set_item("placement", placement)?;
    let indexes = PyList::empty(py);
    for index in collection.indexes() {
        indexes.append(index_to_python(py, index, uuid_representation, bson_types)?)?;
    }
    output.set_item("indexes", indexes)?;
    Ok(output.into_any().unbind())
}

fn index_to_python(
    py: Python<'_>,
    index: &DocumentIndexMetadata,
    uuid_representation: PythonUuidRepresentation,
    bson_types: &PythonBsonOutputTypes,
) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    output.set_item("name", index.name())?;
    let definition = index.definition();
    let keys = if let Some(definition) = definition {
        definition.keys()
    } else if index.is_built_in() {
        match index.specification().get_first("key") {
            Some(BsonValue::Document(keys)) => keys,
            _ => index.specification(),
        }
    } else {
        index.specification()
    };
    output.set_item(
        "keys",
        bson_document_to_python(py, keys, uuid_representation, bson_types)?,
    )?;
    output.set_item("unique", index.is_unique())?;
    if let Some(definition) = definition {
        if definition.sparse() {
            output.set_item("sparse", true)?;
        }
        if let Some(filter) = definition.partial_filter() {
            output.set_item(
                "partial_filter",
                bson_document_to_python(py, filter, uuid_representation, bson_types)?,
            )?;
        }
    }
    output.set_item("built_in", index.is_built_in())?;
    let lifecycle = match index.lifecycle() {
        DocumentIndexLifecycle::Ready => "ready",
        DocumentIndexLifecycle::PendingBuild => "pending_build",
        _ => {
            return Err(crate::error::unsupported(
                "this document index lifecycle is unknown to the current Python API",
            ));
        }
    };
    output.set_item("lifecycle", lifecycle)?;
    Ok(output.into_any().unbind())
}
