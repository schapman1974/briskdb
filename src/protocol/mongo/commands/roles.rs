//! Strict role deletion over the same current, durable authority as user edits.

use super::*;
use crate::core::security_catalog::SecurityName;

pub(super) fn prepare_drop(
    request: &Request,
    started: Instant,
    limits: super::super::MongoResourceLimits,
) -> Result<Prepared> {
    if request.more_to_come || request.legacy_handshake || !request.sequences.is_empty() {
        return Err(CommandError::options());
    }
    if request.database == "local" {
        return Err(CommandError::unsupported());
    }
    let allowed = ["dropRole", "writeConcern", "$db"];
    let mut seen = 0u8;
    for (field, value) in request.body.iter() {
        let index = allowed
            .iter()
            .position(|allowed| *allowed == field)
            .ok_or_else(CommandError::options)?;
        if seen & (1 << index) != 0 {
            return Err(CommandError::invalid());
        }
        seen |= 1 << index;
        if field == "writeConcern" {
            users::write_concern(value)?;
        }
    }
    let Some(BsonValue::String(name)) = request.body.get_first("dropRole") else {
        return Err(CommandError::invalid());
    };
    let name = SecurityName::new(&request.database, name)?;
    let deadline = started + limits.command_timeout();
    if Instant::now() >= deadline {
        return Err(CommandError::new(
            50,
            "MaxTimeMSExpired",
            "command deadline exceeded",
        ));
    }
    Ok(Prepared {
        command: Command::DropRole(name),
        deadline,
        advisory_hint: false,
    })
}

#[cfg(test)]
mod tests;
