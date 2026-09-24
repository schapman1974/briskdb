//! Private Mongo-only listener for the wheel's managed PyMongo clients.
//! It shares database-close ownership, but opens no HTTP/admin/SQL ports.

use super::*;
use briskdb::protocol::mongo::MongoServer;

pub(super) struct MongoShared {
    server: Mutex<Option<MongoServer>>,
    runtime: Arc<RuntimeOwner>,
    address: SocketAddr,
}

impl MongoShared {
    pub(super) fn begin_close(&self) -> NativeResult<()> {
        if let Some(server) = self.server.lock()?.as_ref() {
            server.begin_close();
        }
        Ok(())
    }

    pub(super) fn close_native(&self) -> NativeResult<()> {
        let mut slot = self.server.lock()?;
        if let Some(server) = slot.as_mut() {
            self.runtime
                .runtime
                .block_on(server.close())
                .map_err(listener_error)?;
            slot.take();
        }
        Ok(())
    }
}

impl Drop for MongoShared {
    fn drop(&mut self) {
        if let Ok(slot) = self.server.get_mut() {
            if let Some(server) = slot.as_ref() {
                server.begin_close();
            }
        }
    }
}

#[pyclass(name = "_MongoListener", module = "briskdb._briskdb", frozen)]
pub(super) struct MongoListener {
    shared: Arc<MongoShared>,
}

#[pymethods]
impl MongoListener {
    #[getter]
    fn address(&self) -> String {
        self.shared.address.to_string()
    }

    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<bool> {
        run_native(py, || Ok(self.shared.server.lock()?.is_none()))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        run_native(py, || self.shared.close_native())
    }
}

pub(super) fn start(shared: &Arc<DatabaseShared>, py: Python<'_>) -> PyResult<MongoListener> {
    let shared = Arc::clone(shared);
    run_native(py, move || {
        // Same lock order as Database.close and ordinary listener registration.
        let database_slot = shared.database.lock()?;
        let database = database_slot
            .as_ref()
            .ok_or(NativeError::Closed("database"))?;
        let listener = shared
            .runtime
            .runtime
            .block_on(MongoServer::start(
                database,
                SocketAddr::from(([127, 0, 0, 1], 0)),
            ))
            .map_err(listener_error)?;
        let server = Arc::new(MongoShared {
            address: listener.address(),
            server: Mutex::new(Some(listener)),
            runtime: Arc::clone(&shared.runtime),
        });
        let mut registry = shared.mongo_servers.lock()?;
        registry.retain(|server| server.strong_count() > 0);
        registry.push(Arc::downgrade(&server));
        Ok(MongoListener { shared: server })
    })
}
