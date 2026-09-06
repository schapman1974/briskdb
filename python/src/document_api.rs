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
        DocumentResult::Indexes(indexes) => {
            output.set_item("kind", "indexes")?;
            let values = PyList::empty(py);
            for index in &indexes {
                values.append(index_to_python(py, index, uuid_representation, bson_types)?)?;
            }
            output.set_item("indexes", values)?;
        }
        // The Python surface only constructs commands from the currently
        // executable engine slice. Keep future result variants explicit if a
        // core change accidentally routes one through these methods.
        DocumentResult::Acknowledged(_)
        | DocumentResult::Document(_)
        | DocumentResult::Distinct(_)
        | DocumentResult::Update(_)
        | DocumentResult::CursorKilled(_) => {
            return Err(crate::error::unsupported(
                "this document result is not exposed by the current Python API",
            ));
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
    let keys = if index.is_built_in() {
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
