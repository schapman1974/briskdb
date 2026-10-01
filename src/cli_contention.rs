//! Explicit daemon configuration for the engine's shared contention budget.

use std::time::Duration;

use briskdb::core::{
    ContentionJitter, ContentionPolicy, EngineError, EngineErrorKind, EngineResult,
};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Mode {
    Legacy,
    FailFast,
    Backoff,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Jitter {
    None,
    Full,
}

#[derive(Debug, clap::Args)]
pub(super) struct ContentionArgs {
    /// Lock/admission waiting policy; never replays application writes.
    #[arg(
        long,
        env = "BRISKDB_CONTENTION_MODE",
        value_enum,
        default_value = "legacy"
    )]
    contention_mode: Mode,
    /// First retry delay in whole milliseconds; backoff mode requires all six settings.
    #[arg(long, env = "BRISKDB_CONTENTION_INITIAL_DELAY_MS")]
    contention_initial_delay_ms: Option<u64>,
    /// Maximum retry delay in whole milliseconds.
    #[arg(long, env = "BRISKDB_CONTENTION_MAX_DELAY_MS")]
    contention_max_delay_ms: Option<u64>,
    /// Integer exponential multiplier (1-1024).
    #[arg(long, env = "BRISKDB_CONTENTION_MULTIPLIER")]
    contention_multiplier: Option<u32>,
    /// Delay randomization: none or full.
    #[arg(long, env = "BRISKDB_CONTENTION_JITTER", value_enum)]
    contention_jitter: Option<Jitter>,
    /// Maximum retries, excluding the initial acquisition attempt (1-1000000).
    #[arg(long, env = "BRISKDB_CONTENTION_MAX_RETRIES")]
    contention_max_retries: Option<u32>,
    /// Total contention budget in whole milliseconds; request deadlines still win.
    #[arg(long, env = "BRISKDB_CONTENTION_MAX_ELAPSED_MS")]
    contention_max_elapsed_ms: Option<u64>,
}

impl ContentionArgs {
    pub(super) fn policy(&self) -> EngineResult<Option<ContentionPolicy>> {
        let values = (
            self.contention_initial_delay_ms,
            self.contention_max_delay_ms,
            self.contention_multiplier,
            self.contention_jitter,
            self.contention_max_retries,
            self.contention_max_elapsed_ms,
        );
        match (self.contention_mode, values) {
            (Mode::Legacy, (None, None, None, None, None, None)) => Ok(None),
            (Mode::FailFast, (None, None, None, None, None, None)) => {
                Ok(Some(ContentionPolicy::fail_fast()))
            }
            (
                Mode::Backoff,
                (
                    Some(initial),
                    Some(maximum),
                    Some(multiplier),
                    Some(jitter),
                    Some(retries),
                    Some(elapsed),
                ),
            ) => ContentionPolicy::new(
                Duration::from_millis(initial),
                Duration::from_millis(maximum),
                multiplier,
                match jitter {
                    Jitter::None => ContentionJitter::None,
                    Jitter::Full => ContentionJitter::Full,
                },
                retries,
                Duration::from_millis(elapsed),
            )
            .map(Some),
            _ => Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "contention backoff requires mode=backoff and all six delay/multiplier/jitter/retry/elapsed settings; legacy and fail-fast accept no backoff settings",
            )),
        }
    }
}
