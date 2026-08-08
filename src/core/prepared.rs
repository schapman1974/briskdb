//! Protocol-neutral prepared-statement and bound-portal state.

use std::{collections::BTreeMap, fmt, num::NonZeroU64, sync::Arc};

use crate::sql::{SqlDialect, SqlTranslationMode, TranslatedSql};

use super::{
    Column, DataType, EngineError, EngineErrorKind, EngineResult, LogicalDatabaseId,
    PreparedStatementLimits, ResultSet, SessionId, Value,
};

/// A request to prepare exactly one protocol-neutral SQL statement.
#[derive(Clone, PartialEq, Eq)]
pub struct PrepareRequest {
    database: LogicalDatabaseId,
    dialect: SqlDialect,
    translation_mode: SqlTranslationMode,
    sql: String,
}

impl PrepareRequest {
    /// Construct a request with an explicit logical database, source dialect,
    /// and translation policy.
    pub fn new(
        database: LogicalDatabaseId,
        dialect: SqlDialect,
        translation_mode: SqlTranslationMode,
        sql: impl Into<String>,
    ) -> Self {
        Self {
            database,
            dialect,
            translation_mode,
            sql: sql.into(),
        }
    }

    /// Return the selected logical database.
    pub const fn database(&self) -> LogicalDatabaseId {
        self.database
    }

    /// Return the explicitly selected source dialect.
    pub const fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    /// Return the explicitly selected translation policy.
    pub const fn translation_mode(&self) -> SqlTranslationMode {
        self.translation_mode
    }

    /// Return the original SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn into_parts(self) -> (LogicalDatabaseId, SqlDialect, SqlTranslationMode, String) {
        (self.database, self.dialect, self.translation_mode, self.sql)
    }
}

impl fmt::Debug for PrepareRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrepareRequest")
            .field("database", &self.database)
            .field("dialect", &self.dialect)
            .field("translation_mode", &self.translation_mode)
            .field("sql_bytes", &self.sql.len())
            .finish()
    }
}

/// An opaque handle for one prepared statement in one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PreparedStatementId {
    session: SessionId,
    sequence: NonZeroU64,
}

impl PreparedStatementId {
    const fn new(session: SessionId, sequence: NonZeroU64) -> Self {
        Self { session, sequence }
    }

    pub(crate) const fn session(self) -> SessionId {
        self.session
    }

    const fn sequence(self) -> NonZeroU64 {
        self.sequence
    }
}

/// An opaque handle for one immutable bound portal in one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PortalId {
    session: SessionId,
    sequence: NonZeroU64,
}

impl PortalId {
    const fn new(session: SessionId, sequence: NonZeroU64) -> Self {
        Self { session, sequence }
    }

    pub(crate) const fn session(self) -> SessionId {
        self.session
    }

    const fn sequence(self) -> NonZeroU64 {
        self.sequence
    }
}

/// Select the prepared statement or bound portal whose metadata is requested.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DescribeTarget {
    /// Describe a prepared statement before or after binding.
    Statement(PreparedStatementId),
    /// Describe the statement underlying one bound portal.
    Portal(PortalId),
}

/// Owned parameter and result metadata for a prepared statement.
#[derive(Clone, PartialEq, Eq)]
pub struct PreparedStatementDescription {
    parameter_types: Box<[DataType]>,
    columns: Box<[Column]>,
    schema_generation: u64,
}

impl PreparedStatementDescription {
    pub(crate) fn new(
        parameter_count: usize,
        columns: Vec<Column>,
        schema_generation: u64,
    ) -> Self {
        Self {
            parameter_types: vec![DataType::Unknown; parameter_count].into_boxed_slice(),
            columns: columns.into_boxed_slice(),
            schema_generation,
        }
    }

    /// Return one protocol-neutral type for every normalized parameter index.
    ///
    /// Types are currently `Unknown`; wire adapters decode their own parameter
    /// representation into a concrete [`Value`] before binding.
    pub fn parameter_types(&self) -> &[DataType] {
        &self.parameter_types
    }

    /// Return ordered result-column metadata. Commands have no columns.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Return the application-schema generation used to compile the metadata.
    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    /// Return whether SQLite reported result columns for the statement.
    pub fn returns_rows(&self) -> bool {
        !self.columns.is_empty()
    }
}

impl fmt::Debug for PreparedStatementDescription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedStatementDescription")
            .field("parameter_count", &self.parameter_types.len())
            .field("column_count", &self.columns.len())
            .field("schema_generation", &self.schema_generation)
            .finish()
    }
}

/// The result of executing one bound portal.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum PreparedExecution {
    /// A command completed and changed this many rows.
    AffectedRows(usize),
    /// A read-only statement returned a bounded materialized result.
    Rows(ResultSet),
}

#[derive(Clone)]
pub(crate) struct PreparedTemplate {
    database: LogicalDatabaseId,
    translated: TranslatedSql,
    description: PreparedStatementDescription,
}

impl PreparedTemplate {
    pub(crate) const fn database(&self) -> LogicalDatabaseId {
        self.database
    }

    pub(crate) const fn translated(&self) -> &TranslatedSql {
        &self.translated
    }

    pub(crate) const fn description(&self) -> &PreparedStatementDescription {
        &self.description
    }

    pub(crate) fn replace_description(&mut self, description: PreparedStatementDescription) {
        self.description = description;
    }
}

impl fmt::Debug for PreparedTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedTemplate")
            .field("database", &self.database)
            .field("dialect", &self.translated.dialect())
            .field("translation_mode", &self.translated.mode())
            .field("source_bytes", &self.translated.source().len())
            .field("sqlite_sql_bytes", &self.translated.sqlite_sql().len())
            .field("parameter_count", &self.description.parameter_types.len())
            .field("column_count", &self.description.columns.len())
            .field("schema_generation", &self.description.schema_generation)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct PreparedPortal {
    statement: PreparedStatementId,
    parameters: Arc<[Value]>,
    routing_key: Option<Arc<[u8]>>,
    retained_bytes: u64,
}

impl PreparedPortal {
    pub(crate) const fn statement(&self) -> PreparedStatementId {
        self.statement
    }

    pub(crate) fn parameters(&self) -> &[Value] {
        &self.parameters
    }

    pub(crate) fn routing_key(&self) -> Option<&[u8]> {
        self.routing_key.as_deref()
    }
}

impl fmt::Debug for PreparedPortal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedPortal")
            .field("statement", &self.statement)
            .field("parameter_count", &self.parameters.len())
            .field("has_routing_key", &self.routing_key.is_some())
            .field("retained_bytes", &self.retained_bytes)
            .finish()
    }
}

pub(crate) struct PreparedState {
    session: SessionId,
    limits: PreparedStatementLimits,
    next_statement_sequence: u64,
    next_portal_sequence: u64,
    statements: BTreeMap<NonZeroU64, PreparedTemplate>,
    portals: BTreeMap<NonZeroU64, PreparedPortal>,
    retained_bound_value_bytes: u64,
}

impl PreparedState {
    pub(crate) fn new(session: SessionId, limits: PreparedStatementLimits) -> Self {
        Self {
            session,
            limits,
            next_statement_sequence: 1,
            next_portal_sequence: 1,
            statements: BTreeMap::new(),
            portals: BTreeMap::new(),
            retained_bound_value_bytes: 0,
        }
    }

    pub(crate) fn ensure_statement_capacity(&self) -> EngineResult<()> {
        if self.statements.len() >= self.limits.max_statements_per_session() {
            Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                "the session prepared-statement cache is full",
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn insert_statement(
        &mut self,
        database: LogicalDatabaseId,
        translated: TranslatedSql,
        description: PreparedStatementDescription,
    ) -> EngineResult<PreparedStatementId> {
        self.ensure_statement_capacity()?;
        let sequence = next_sequence(
            &mut self.next_statement_sequence,
            "the session exhausted prepared-statement identifiers",
        )?;
        let id = PreparedStatementId::new(self.session, sequence);
        let previous = self.statements.insert(
            sequence,
            PreparedTemplate {
                database,
                translated,
                description,
            },
        );
        debug_assert!(
            previous.is_none(),
            "prepared-statement IDs are never reused"
        );
        Ok(id)
    }

    pub(crate) fn statement(&self, id: PreparedStatementId) -> EngineResult<&PreparedTemplate> {
        self.ensure_statement_owner(id)?;
        self.statements
            .get(&id.sequence())
            .ok_or_else(missing_statement)
    }

    pub(crate) fn statement_mut(
        &mut self,
        id: PreparedStatementId,
    ) -> EngineResult<&mut PreparedTemplate> {
        self.ensure_statement_owner(id)?;
        self.statements
            .get_mut(&id.sequence())
            .ok_or_else(missing_statement)
    }

    pub(crate) fn close_statement(&mut self, id: PreparedStatementId) -> EngineResult<bool> {
        self.ensure_statement_owner(id)?;
        if self.statements.remove(&id.sequence()).is_none() {
            return Ok(false);
        }

        let released = self
            .portals
            .values()
            .filter(|portal| portal.statement == id)
            .try_fold(0_u64, |total, portal| {
                total.checked_add(portal.retained_bytes).ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "bound-portal byte accounting overflowed while closing a statement",
                    )
                })
            })?;
        self.portals.retain(|_, portal| portal.statement != id);
        self.retained_bound_value_bytes = self
            .retained_bound_value_bytes
            .checked_sub(released)
            .expect("retained portal bytes include every dependent portal");
        Ok(true)
    }

    pub(crate) fn insert_portal(
        &mut self,
        statement: PreparedStatementId,
        parameters: Vec<Value>,
        routing_key: Option<Vec<u8>>,
    ) -> EngineResult<PortalId> {
        self.statement(statement)?;
        let (retained_bytes, next_retained) =
            self.ensure_portal_capacity(&parameters, routing_key.as_deref())?;
        let sequence = next_sequence(
            &mut self.next_portal_sequence,
            "the session exhausted bound-portal identifiers",
        )?;
        let id = PortalId::new(self.session, sequence);
        let previous = self.portals.insert(
            sequence,
            PreparedPortal {
                statement,
                parameters: Arc::from(parameters),
                routing_key: routing_key.map(Arc::from),
                retained_bytes,
            },
        );
        debug_assert!(previous.is_none(), "bound-portal IDs are never reused");
        self.retained_bound_value_bytes = next_retained;
        Ok(id)
    }

    pub(crate) fn ensure_portal_capacity(
        &self,
        parameters: &[Value],
        routing_key: Option<&[u8]>,
    ) -> EngineResult<(u64, u64)> {
        if self.portals.len() >= self.limits.max_portals_per_session() {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                "the session bound-portal cache is full",
            ));
        }
        let retained_bytes = retained_bound_value_bytes(parameters, routing_key)?;
        let next_retained = self
            .retained_bound_value_bytes
            .checked_add(retained_bytes)
            .filter(|bytes| *bytes <= self.limits.max_retained_bound_value_bytes())
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "bound portals exceed the session retained-value byte limit",
                )
            })?;
        Ok((retained_bytes, next_retained))
    }

    pub(crate) fn ensure_planning_capacity(
        &self,
        parameters: &[Value],
        parameter_indices: &[usize],
        routing_key: Option<&[u8]>,
    ) -> EngineResult<()> {
        // The planner owns one canonical copy of an explicit route while it
        // validates the bind. Account for that allocation as part of the same
        // per-bind ceiling as expanded parameter occurrences.
        let mut planning_bytes = match routing_key {
            Some(key) => usize_to_u64(key.len())?,
            None => 0,
        };
        if planning_bytes > self.limits.max_retained_bound_value_bytes() {
            return Err(planning_bytes_exceeded());
        }
        for index in parameter_indices {
            let parameter = index
                .checked_sub(1)
                .and_then(|index| parameters.get(index))
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "normalized parameter layout is outside the validated bind",
                    )
                })?;
            // Inference may retain one typed payload and routing may retain one
            // canonical payload for the same occurrence. Conservatively budget
            // both before the planner can allocate either copy.
            let occurrence_bytes = retained_value_bytes(parameter)?
                .checked_mul(2)
                .ok_or_else(retained_bytes_overflow)?;
            planning_bytes = planning_bytes
                .checked_add(occurrence_bytes)
                .filter(|bytes| *bytes <= self.limits.max_retained_bound_value_bytes())
                .ok_or_else(planning_bytes_exceeded)?;
        }
        Ok(())
    }

    pub(crate) fn portal(&self, id: PortalId) -> EngineResult<&PreparedPortal> {
        self.ensure_portal_owner(id)?;
        self.portals.get(&id.sequence()).ok_or_else(missing_portal)
    }

    pub(crate) fn close_portal(&mut self, id: PortalId) -> EngineResult<bool> {
        self.ensure_portal_owner(id)?;
        let Some(portal) = self.portals.remove(&id.sequence()) else {
            return Ok(false);
        };
        self.retained_bound_value_bytes = self
            .retained_bound_value_bytes
            .checked_sub(portal.retained_bytes)
            .expect("retained portal bytes include the removed portal");
        Ok(true)
    }

    pub(crate) fn clear(&mut self) {
        self.statements.clear();
        self.portals.clear();
        self.retained_bound_value_bytes = 0;
    }

    fn ensure_statement_owner(&self, id: PreparedStatementId) -> EngineResult<()> {
        if id.session() == self.session {
            Ok(())
        } else {
            Err(foreign_handle("prepared statement"))
        }
    }

    fn ensure_portal_owner(&self, id: PortalId) -> EngineResult<()> {
        if id.session() == self.session {
            Ok(())
        } else {
            Err(foreign_handle("bound portal"))
        }
    }

    #[cfg(test)]
    pub(crate) fn statement_count(&self) -> usize {
        self.statements.len()
    }

    #[cfg(test)]
    pub(crate) fn portal_count(&self) -> usize {
        self.portals.len()
    }

    #[cfg(test)]
    pub(crate) const fn retained_bound_value_bytes_for_test(&self) -> u64 {
        self.retained_bound_value_bytes
    }
}

impl fmt::Debug for PreparedState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedState")
            .field("session", &self.session)
            .field("limits", &self.limits)
            .field("statement_count", &self.statements.len())
            .field("portal_count", &self.portals.len())
            .field(
                "retained_bound_value_bytes",
                &self.retained_bound_value_bytes,
            )
            .finish()
    }
}

fn next_sequence(counter: &mut u64, diagnostic: &'static str) -> EngineResult<NonZeroU64> {
    let sequence = NonZeroU64::new(*counter)
        .ok_or_else(|| EngineError::new(EngineErrorKind::LimitExceeded, diagnostic))?;
    *counter = counter
        .checked_add(1)
        .ok_or_else(|| EngineError::new(EngineErrorKind::LimitExceeded, diagnostic))?;
    Ok(sequence)
}

fn retained_bound_value_bytes(
    parameters: &[Value],
    routing_key: Option<&[u8]>,
) -> EngineResult<u64> {
    let mut total = match routing_key {
        Some(key) => usize_to_u64(key.len())?,
        None => 0,
    };
    for parameter in parameters {
        total = total
            .checked_add(retained_value_bytes(parameter)?)
            .ok_or_else(retained_bytes_overflow)?;
    }
    Ok(total)
}

fn retained_value_bytes(parameter: &Value) -> EngineResult<u64> {
    const TYPE_BYTES: u64 = 1;
    const LENGTH_BYTES: u64 = 8;
    const FIXED_VALUE_BYTES: u64 = 8;

    let payload = match parameter {
        Value::Null => 0,
        Value::Boolean(_) | Value::Int64(_) | Value::UInt64(_) | Value::Float64(_) => {
            FIXED_VALUE_BYTES
        }
        Value::Decimal(value) => usize_to_u64(value.as_str().len())?,
        Value::Text(value) => usize_to_u64(value.len())?,
        Value::InvalidText(value) | Value::Binary(value) => usize_to_u64(value.len())?,
    };
    TYPE_BYTES
        .checked_add(LENGTH_BYTES)
        .and_then(|bytes| bytes.checked_add(payload))
        .ok_or_else(retained_bytes_overflow)
}

fn usize_to_u64(value: usize) -> EngineResult<u64> {
    u64::try_from(value).map_err(|_| retained_bytes_overflow())
}

fn retained_bytes_overflow() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "bound portal retained-value byte accounting overflowed",
    )
}

fn planning_bytes_exceeded() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "bound parameters exceed the session planning byte limit",
    )
}

fn missing_statement() -> EngineError {
    EngineError::new(
        EngineErrorKind::FailedPrecondition,
        "the prepared statement is not open in this session",
    )
}

fn missing_portal() -> EngineError {
    EngineError::new(
        EngineErrorKind::FailedPrecondition,
        "the bound portal is not open in this session",
    )
}

fn foreign_handle(entity: &str) -> EngineError {
    EngineError::new(
        EngineErrorKind::FailedPrecondition,
        format!("the {entity} belongs to a different session"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{normalize_placeholders, parse, translate_sql, validate_common_subset};

    fn translated(sql: &str) -> TranslatedSql {
        let parsed = parse(SqlDialect::Sqlite, sql).unwrap();
        let common = validate_common_subset(parsed).unwrap();
        let normalized = normalize_placeholders(common).unwrap();
        translate_sql(normalized, SqlTranslationMode::StrictSqlite).unwrap()
    }

    fn description(parameters: usize) -> PreparedStatementDescription {
        PreparedStatementDescription::new(parameters, vec![Column::new("v", DataType::Unknown)], 0)
    }

    fn state(max_statements: usize, max_portals: usize, max_bytes: u64) -> PreparedState {
        let limits = PreparedStatementLimits::new(max_statements, max_portals, max_bytes).unwrap();
        PreparedState::new(SessionId::for_test(99), limits)
    }

    #[test]
    fn prepare_request_accessors_and_debug_are_owned_and_redacted() {
        let database = LogicalDatabaseId::new(1).unwrap();
        let request = PrepareRequest::new(
            database,
            SqlDialect::PostgreSql,
            SqlTranslationMode::Compatibility,
            "SELECT 'private' WHERE id = $1",
        );
        assert_eq!(request.database(), database);
        assert_eq!(request.dialect(), SqlDialect::PostgreSql);
        assert_eq!(
            request.translation_mode(),
            SqlTranslationMode::Compatibility
        );
        assert_eq!(request.sql(), "SELECT 'private' WHERE id = $1");
        let debug = format!("{request:?}");
        assert!(debug.contains("sql_bytes"));
        assert!(!debug.contains("private"));
    }

    #[test]
    fn statement_cache_is_bounded_distinct_monotonic_and_has_no_eviction() {
        let database = LogicalDatabaseId::new(1).unwrap();
        let mut state = state(2, 2, 1_024);
        let first = state
            .insert_statement(database, translated("SELECT 1"), description(0))
            .unwrap();
        let second = state
            .insert_statement(database, translated("SELECT 1"), description(0))
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(state.statement_count(), 2);
        assert_eq!(
            state.ensure_statement_capacity().unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(state.statement(first).unwrap().database(), database);

        assert!(state.close_statement(first).unwrap());
        assert!(!state.close_statement(first).unwrap());
        let third = state
            .insert_statement(database, translated("SELECT 2"), description(0))
            .unwrap();
        assert!(third > second);
        assert_eq!(state.statement_count(), 2);
        assert!(state.statement(second).is_ok());
    }

    #[test]
    fn portals_are_bounded_accounted_and_statement_close_cascades() {
        let database = LogicalDatabaseId::new(1).unwrap();
        let mut state = state(2, 2, 128);
        let sql = translated("SELECT 1");
        let statement = state
            .insert_statement(database, sql, description(0))
            .unwrap();
        let first = state
            .insert_portal(statement, vec![Value::from("abc")], Some(b"route".to_vec()))
            .unwrap();
        let second = state
            .insert_portal(statement, vec![Value::from(7_i64)], None)
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(state.portal_count(), 2);
        assert_eq!(state.retained_bound_value_bytes_for_test(), 34);
        assert_eq!(
            state
                .insert_portal(statement, vec![], None)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );

        assert!(state.close_statement(statement).unwrap());
        assert_eq!(state.portal_count(), 0);
        assert_eq!(state.retained_bound_value_bytes_for_test(), 0);
        assert_eq!(
            state.portal(first).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn portal_byte_failure_is_atomic_and_close_releases_exact_capacity() {
        let database = LogicalDatabaseId::new(1).unwrap();
        let mut state = state(1, 2, 20);
        let sql = translated("SELECT 1");
        let statement = state
            .insert_statement(database, sql, description(0))
            .unwrap();
        let too_large = state
            .insert_portal(statement, vec![Value::from("twelve-bytes")], None)
            .unwrap_err();
        assert_eq!(too_large.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(state.portal_count(), 0);
        assert_eq!(state.retained_bound_value_bytes_for_test(), 0);

        let portal = state
            .insert_portal(statement, vec![Value::from("abc")], None)
            .unwrap();
        assert_eq!(state.retained_bound_value_bytes_for_test(), 12);
        assert!(state.close_portal(portal).unwrap());
        assert!(!state.close_portal(portal).unwrap());
        assert_eq!(state.retained_bound_value_bytes_for_test(), 0);
    }

    #[test]
    fn repeated_parameter_occurrences_are_bounded_before_planning_allocates() {
        let state = state(1, 1, 30);
        let parameters = vec![Value::from("abc")];
        assert!(state.ensure_portal_capacity(&parameters, None).is_ok());
        assert!(
            state
                .ensure_planning_capacity(&parameters, &[1], Some(b"123456"))
                .is_ok()
        );
        assert_eq!(
            state
                .ensure_planning_capacity(&parameters, &[1, 1], None)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(
            state
                .ensure_planning_capacity(&parameters, &[1], Some(b"1234567"))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(state.portal_count(), 0);
        assert_eq!(state.retained_bound_value_bytes_for_test(), 0);
    }

    #[test]
    fn retained_value_accounting_covers_every_type_and_exact_boundaries() {
        let cases = [
            (Value::Null, 9),
            (Value::from(false), 17),
            (Value::from(i64::MIN), 17),
            (Value::from(u64::MAX), 17),
            (Value::from(1.5_f64), 17),
            (Value::decimal("12.3").unwrap(), 13),
            (Value::from("é"), 11),
            (Value::from(""), 9),
            (Value::InvalidText(vec![0x80]), 10),
            (Value::from(vec![0_u8, 255]), 11),
            (Value::from(Vec::<u8>::new()), 9),
        ];
        for (value, expected) in cases {
            assert_eq!(
                retained_bound_value_bytes(&[value], None).unwrap(),
                expected
            );
        }
        assert_eq!(retained_bound_value_bytes(&[], Some(b"route")).unwrap(), 5);

        let state = state(1, 1, 12);
        let parameters = vec![Value::from("abc")];
        assert_eq!(
            state.ensure_portal_capacity(&parameters, None).unwrap(),
            (12, 12)
        );
        assert_eq!(
            state
                .ensure_portal_capacity(&parameters, Some(b"x"))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(state.portal_count(), 0);
        assert_eq!(state.retained_bound_value_bytes_for_test(), 0);
    }

    #[test]
    fn handles_are_session_scoped_and_debug_omits_cached_contents() {
        let database = LogicalDatabaseId::new(1).unwrap();
        let mut first = state(1, 1, 128);
        let sql = translated("SELECT 'private'");
        let statement = first
            .insert_statement(database, sql.clone(), description(0))
            .unwrap();
        let portal = first
            .insert_portal(
                statement,
                vec![Value::from("private")],
                Some(b"private-route".to_vec()),
            )
            .unwrap();
        let second = PreparedState::new(
            SessionId::for_test(100),
            PreparedStatementLimits::new(1, 1, 128).unwrap(),
        );

        assert_eq!(
            second.statement(statement).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            second.portal(portal).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        let debug = format!("{first:?} {:?}", first.portal(portal).unwrap());
        assert!(!debug.contains("private"));
        assert!(!debug.contains("private-route"));
    }

    #[test]
    fn description_accessors_preserve_counts_and_redact_column_names() {
        let description = PreparedStatementDescription::new(
            2,
            vec![
                Column::new("private_a", DataType::Unknown),
                Column::new("private_b", DataType::Unknown),
            ],
            7,
        );
        assert_eq!(
            description.parameter_types(),
            [DataType::Unknown, DataType::Unknown]
        );
        assert_eq!(description.columns().len(), 2);
        assert_eq!(description.schema_generation(), 7);
        assert!(description.returns_rows());
        let debug = format!("{description:?}");
        assert!(!debug.contains("private_a"));
        assert!(!debug.contains("private_b"));
    }
}
