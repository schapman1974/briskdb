//! Bounded Ready-index discovery over validated manifest snapshots.

use super::*;

impl Storage {
    pub(crate) fn document_index_metadata_identity_controlled(
        &self,
        namespace: &DocumentNamespace,
        control: Arc<OperationControl>,
    ) -> EngineResult<Option<(DocumentCollectionId, u64)>> {
        let result = (|| {
            let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
            run_manifest_controlled(&mut connection, control, |connection| {
                read_ready_manifest_snapshot(connection, self.shard_count(), |connection| {
                    connection
                        .query_row(
                            "SELECT c.collection_id, a.index_high_water
                         FROM briskdb_document_collections AS c
                         JOIN briskdb_document_databases AS d USING (database_id)
                         CROSS JOIN briskdb_document_index_allocator AS a
                         WHERE d.database_name = ?1 AND c.collection_name = ?2
                           AND c.lifecycle_state = ?3 AND a.singleton = 1",
                            params![
                                namespace.database(),
                                namespace.collection(),
                                COLLECTION_ACTIVE
                            ],
                            |row| {
                                Ok((
                                    DocumentCollectionId::from_validated(
                                        row.get::<_, i64>(0)? as u64
                                    ),
                                    row.get::<_, i64>(1)? as u64,
                                ))
                            },
                        )
                        .optional()
                        .map_err(sqlite_error::storage)
                })
            })
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn scan_document_index_metadata_controlled(
        &self,
        collection: DocumentCollectionId,
        upper_id: u64,
        after_name: Option<&str>,
        control: Arc<OperationControl>,
        mut visit: impl FnMut(DocumentIndexMetadata) -> EngineResult<bool>,
    ) -> EngineResult<()> {
        let result = (|| {
            let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
            let read_control = Arc::clone(&control);
            run_manifest_controlled(&mut connection, control, |connection| {
                read_ready_manifest_snapshot(connection, self.shard_count(), |connection| {
                    let exists: bool = connection
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM briskdb_document_collections
                         WHERE collection_id = ?1 AND lifecycle_state = ?2)",
                            params![collection.get() as i64, COLLECTION_ACTIVE],
                            |row| row.get(0),
                        )
                        .map_err(sqlite_error::storage)?;
                    if !exists {
                        return Err(
                            crate::document::DocumentCursorError::NotFound.into_engine_error()
                        );
                    }
                    // Separate built-in and secondary scans preserve TinyMongo's
                    // built-in-first/name order without sorting large BSON blobs.
                    for built_in in [true, false] {
                        if built_in && after_name.is_some() {
                            continue;
                        }
                        let mut statement = connection
                            .prepare(
                                "SELECT ids.index_id, i.index_name, i.spec_bson, i.is_unique
                             FROM briskdb_document_indexes AS i
                             JOIN briskdb_document_index_identities AS ids
                               USING (collection_id, index_name)
                             WHERE i.collection_id = ?1 AND i.lifecycle_state = ?2
                               AND i.is_builtin = ?3 AND ids.index_id <= ?4
                               AND i.index_name > ?5 ORDER BY i.index_name",
                            )
                            .map_err(sqlite_error::storage)?;
                        let mut rows = statement
                            .query(params![
                                collection.get() as i64,
                                INDEX_READY,
                                built_in,
                                upper_id as i64,
                                if built_in {
                                    ""
                                } else {
                                    after_name.unwrap_or("")
                                },
                            ])
                            .map_err(sqlite_error::storage)?;
                        while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
                            ensure_control_active(&read_control, "while reading index metadata")?;
                            let bytes: Vec<u8> = row.get(2).map_err(sqlite_error::storage)?;
                            let index = DocumentIndexMetadata::from_validated_parts(
                                DocumentIndexId::from_validated(
                                    row.get::<_, i64>(0).map_err(sqlite_error::storage)? as u64,
                                ),
                                row.get(1).map_err(sqlite_error::storage)?,
                                decode_metadata_document(
                                    &bytes,
                                    "stored index metadata is invalid",
                                )?,
                                row.get(3).map_err(sqlite_error::storage)?,
                                built_in,
                                DocumentIndexLifecycle::Ready,
                            );
                            if !visit(index)? {
                                return Ok(());
                            }
                        }
                    }
                    ensure_control_active(&read_control, "after reading index metadata")
                })
            })
        })();
        self.fail_closed_on_corruption(result)
    }
}
