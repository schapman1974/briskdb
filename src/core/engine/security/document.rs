use super::*;
use crate::{
    core::authorization::{Action, DataDomain, Resource},
    document::{DocumentCommand, DocumentNamespace},
};

type Requirements = Vec<(Action, Resource)>;

impl Engine {
    /// Wire adapters sometimes return an empty result without executing the
    /// document command, or create a namespace before executing a write. Check
    /// the real command's requirements before either optimization. Execution
    /// still rechecks current authority; this is not a reusable admission token.
    #[cfg(feature = "mongo")]
    pub(crate) async fn preflight_document_authorization(
        &self,
        session: &Session,
        command: &DocumentCommand,
        context: RequestContext,
    ) -> EngineResult<()> {
        let mut operation = self.operation_lifecycle(context)?;
        if session.owner != self.inner.id {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "the session belongs to a different engine",
            )));
        }
        let result = operation
            .wait_preflight(self.authorize_document(session, command))
            .await;
        operation.check_before_start()?;
        operation.finish(result)
    }

    pub(in crate::core::engine) async fn authorize_document(
        &self,
        session: &Session,
        command: &DocumentCommand,
    ) -> EngineResult<()> {
        if !self.security_enabled() {
            return Ok(());
        }
        session.inner.lock().await.ensure_open()?;
        let principal = session.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        let requirements = self.document_requirements(session, command)?;
        self.security_call(move |authority| {
            authority.authorize_all(
                &principal,
                requirements
                    .iter()
                    .map(|(action, resource)| (*action, resource)),
            )
        })
        .await
    }

    fn document_requirements(
        &self,
        session: &Session,
        command: &DocumentCommand,
    ) -> EngineResult<Requirements> {
        use DocumentCommand as C;
        match command {
            C::ListDatabaseNames(_) => Ok(vec![(
                Action::ListDatabases,
                Resource::data_domain(DataDomain::Document),
            )]),
            C::ListCollections(r) => database(r.database(), Action::ListObjects),
            C::ListCollectionMetadata(r) => database(r.namespace().database(), Action::ListObjects),
            C::CollectionExists(r) => database(r.namespace().database(), Action::ListObjects),
            C::DropDatabase(r) => database(r.database(), Action::DropDatabase),
            C::CreateCollection(r) => {
                let mut requirements = object(r.namespace(), &[Action::CreateObject])?;
                // This command can implicitly create the logical database. Require
                // that privilege even if it existed before a concurrent drop.
                requirements.push((
                    Action::CreateDatabase,
                    Resource::database(DataDomain::Document, r.namespace().database())?,
                ));
                Ok(requirements)
            }
            C::DropCollection(r) => object(r.namespace(), &[Action::DropObject]),
            C::Find(r) => object(r.namespace(), &[Action::ReadData]),
            C::Aggregate(r) => object(r.namespace(), &[Action::ReadData]),
            C::Count(r) => object(r.namespace(), &[Action::ReadData]),
            C::Distinct(r) => object(r.namespace(), &[Action::ReadData]),
            C::Insert(r) => object(r.namespace(), &[Action::InsertData]),
            C::Delete(r) => object(r.namespace(), &[Action::DeleteData]),
            C::Update(r) => mutation(r.namespace(), r.write_options().upsert(), false),
            C::Replace(r) => mutation(r.namespace(), r.write_options().upsert(), false),
            C::FindOneAndUpdate(r) => mutation(
                r.namespace(),
                r.update_request().write_options().upsert(),
                true,
            ),
            C::FindOneAndReplace(r) => mutation(
                r.namespace(),
                r.replacement_request().write_options().upsert(),
                true,
            ),
            C::FindOneAndDelete(r) => {
                object(r.namespace(), &[Action::ReadData, Action::DeleteData])
            }
            C::CreateIndex(r) | C::CreateBuiltIndex(r) => {
                object(r.namespace(), &[Action::CreateIndex])
            }
            C::CreateIndexes(r) => object(r.namespace(), &[Action::CreateIndex]),
            C::BuildIndex(r) => object(r.namespace(), &[Action::CreateIndex]),
            C::DropIndex(r) => object(r.namespace(), &[Action::DropIndex]),
            C::DropIndexes(r) => object(r.namespace(), &[Action::DropIndex]),
            C::ListIndexes(r) => object(r.namespace(), &[Action::ListIndexes]),
            C::ListIndexMetadata(r) => object(r.namespace(), &[Action::ListIndexes]),
            C::ContinueCursor(r) => self.inner.document_cursors.security_requirements(
                ConnectionOwner::new(session.id().get()),
                r.namespace(),
                r.cursor_id(),
            ),
            C::KillCursor(r) => self.inner.document_cursors.security_requirements(
                ConnectionOwner::new(session.id().get()),
                r.namespace(),
                r.cursor_id(),
            ),
        }
    }
}

pub(in crate::core::engine) fn database(name: &str, action: Action) -> EngineResult<Requirements> {
    let resource = Resource::database(DataDomain::Document, name)?;
    Ok(vec![
        (Action::ConnectDatabase, resource.clone()),
        (action, resource),
    ])
}

pub(in crate::core::engine) fn object(
    namespace: &DocumentNamespace,
    actions: &[Action],
) -> EngineResult<Requirements> {
    let mut requirements = vec![(
        Action::ConnectDatabase,
        Resource::database(DataDomain::Document, namespace.database())?,
    )];
    let resource = Resource::object(
        DataDomain::Document,
        namespace.database(),
        namespace.collection(),
    )?;
    requirements.extend(actions.iter().map(|action| (*action, resource.clone())));
    Ok(requirements)
}

fn mutation(
    namespace: &DocumentNamespace,
    upsert: bool,
    returns_document: bool,
) -> EngineResult<Requirements> {
    let mut actions = vec![Action::UpdateData];
    if upsert {
        actions.push(Action::InsertData);
    }
    if returns_document {
        actions.push(Action::ReadData);
    }
    object(namespace, &actions)
}

#[cfg(all(test, unix))]
mod tests;
