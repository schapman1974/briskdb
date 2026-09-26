//! Explicit, bounded read-option compatibility. Never silently weaken sessions,
//! durability, collation, cursor expiry, or partial-result guarantees.

use super::*;

const HINT_WARNING: &str =
    "hint: accepted for TinyMongo compatibility; index selection remains automatic";

/// `None` leaves the option to the ordinary command validator. BSON input has
/// already passed the wire size/depth/allocation limits. Comments and advisory
/// hints are not cloned, retained in cursors, logged, or used as metric labels.
pub(super) fn accepts(command: &str, field: &str, value: &BsonValue) -> Option<bool> {
    let read = matches!(command, "find" | "count" | "distinct" | "aggregate");
    Some(match field {
        "hint" if read => matches!(value, BsonValue::String(_) | BsonValue::Document(_)),
        "comment" if read || command == "getMore" => true,
        "collation" if read => matches!(value, BsonValue::Document(doc)
            if doc.len() == 1 && matches!(doc.get_first("locale"), Some(BsonValue::String(locale)) if locale == "simple")),
        "readConcern" if read => matches!(value, BsonValue::Document(doc)
            if doc.is_empty() || (doc.len() == 1 && matches!(doc.get_first("level"), Some(BsonValue::String(level)) if level == "local"))),
        "allowDiskUse" if matches!(command, "find" | "aggregate") => {
            matches!(value, BsonValue::Boolean(false))
        }
        "tailable"
        | "awaitData"
        | "noCursorTimeout"
        | "allowPartialResults"
        | "returnKey"
        | "showRecordId"
            if command == "find" =>
        {
            matches!(value, BsonValue::Boolean(false))
        }
        // MongoDB itself ignores this legacy flag; it does not enable an oplog.
        "oplogReplay" if command == "find" => matches!(value, BsonValue::Boolean(_)),
        // Empty bindings cannot introduce any expression variables.
        "let" if matches!(command, "find" | "aggregate") => {
            matches!(value, BsonValue::Document(doc) if doc.is_empty())
        }
        _ => return None,
    })
}

pub(super) fn reply(mut reply: BsonDocument, advisory_hint: bool) -> BsonDocument {
    if advisory_hint {
        // Fixed diagnostic text: never echo the supplied index name/pattern.
        // It fits in the command's reserved 4096-byte protocol envelope.
        reply
            .push(
                "briskdbReadWarnings",
                BsonValue::Array(vec![BsonValue::from(HINT_WARNING)]),
            )
            .expect("static diagnostic field");
    }
    reply
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonValue {
        BsonValue::Document(BsonDocument::from_entries(entries).unwrap())
    }

    fn request(command: &str, option: &str, value: BsonValue) -> Request {
        let mut body = fields([(command, BsonValue::from("items"))]);
        if command == "aggregate" {
            body.push("pipeline", BsonValue::Array(vec![])).unwrap();
            body.push("cursor", object([])).unwrap();
        } else if command == "distinct" {
            body.push("key", BsonValue::from("v")).unwrap();
        }
        body.push(option, value).unwrap();
        body.push("$db", BsonValue::from("absent")).unwrap();
        Request {
            request_id: 1,
            database: "absent".into(),
            body,
            sequences: vec![],
            more_to_come: false,
            legacy_handshake: false,
        }
    }

    #[test]
    fn safe_read_options_validate_before_admission_and_do_not_mutate_inputs() {
        for command in ["find", "count", "distinct", "aggregate"] {
            for (option, value) in [
                ("hint", BsonValue::from("private-index-name")),
                ("hint", object([("private-field", BsonValue::Int32(-1))])),
                ("hint", object([])),
                (
                    "comment",
                    object([("private", BsonValue::Array(vec![BsonValue::Null]))]),
                ),
                ("comment", BsonValue::Int32(9)),
                ("readConcern", object([])),
                ("readConcern", object([("level", BsonValue::from("local"))])),
                ("collation", object([("locale", BsonValue::from("simple"))])),
            ] {
                let request = request(command, option, value);
                let before = request.body.clone();
                let plan = prepare(&request, false).unwrap().unwrap();
                assert_eq!(plan.advisory_hint, option == "hint");
                assert_eq!(request.body, before);
            }
        }
    }

    #[test]
    fn semantic_options_and_invalid_hint_types_are_still_rejected() {
        for command in ["find", "count", "distinct", "aggregate"] {
            for (option, value) in [
                ("hint", BsonValue::Int32(1)),
                ("hint", BsonValue::Array(vec![])),
                ("hint", BsonValue::Null),
                (
                    "readConcern",
                    object([("level", BsonValue::from("majority"))]),
                ),
                (
                    "readConcern",
                    object([("level", BsonValue::from("snapshot"))]),
                ),
                (
                    "readConcern",
                    object([
                        ("level", BsonValue::from("local")),
                        ("afterClusterTime", BsonValue::Int32(1)),
                    ]),
                ),
                ("collation", object([])),
                ("collation", object([("locale", BsonValue::from("en"))])),
                (
                    "collation",
                    object([
                        ("locale", BsonValue::from("simple")),
                        ("strength", BsonValue::Int32(1)),
                    ]),
                ),
                ("lsid", object([])),
                ("arbitraryOption", BsonValue::Null),
            ] {
                assert_eq!(
                    prepare(&request(command, option, value), false)
                        .unwrap()
                        .err()
                        .unwrap()
                        .code,
                    72
                );
            }
        }
    }

    #[test]
    fn cursor_defaults_cannot_disable_limits_or_enable_unimplemented_features() {
        for option in [
            "allowDiskUse",
            "tailable",
            "awaitData",
            "noCursorTimeout",
            "allowPartialResults",
            "returnKey",
            "showRecordId",
        ] {
            assert!(
                prepare(&request("find", option, BsonValue::Boolean(false)), false)
                    .unwrap()
                    .is_ok()
            );
            for value in [
                BsonValue::Boolean(true),
                BsonValue::Int32(0),
                BsonValue::Null,
            ] {
                assert_eq!(
                    prepare(&request("find", option, value), false)
                        .unwrap()
                        .err()
                        .unwrap()
                        .code,
                    72
                );
            }
        }
        for command in ["find", "aggregate"] {
            assert!(
                prepare(&request(command, "let", object([])), false)
                    .unwrap()
                    .is_ok()
            );
            assert_eq!(
                prepare(
                    &request(command, "let", object([("private", BsonValue::Int32(1))])),
                    false
                )
                .unwrap()
                .err()
                .unwrap()
                .code,
                72
            );
        }
        for value in [false, true] {
            assert!(
                prepare(
                    &request("find", "oplogReplay", BsonValue::Boolean(value)),
                    false
                )
                .unwrap()
                .is_ok()
            );
        }
    }

    #[test]
    fn read_compatibility_never_spills_into_write_or_metadata_options() {
        for command in [
            "create",
            "drop",
            "delete",
            "update",
            "findAndModify",
            "listIndexes",
            "createIndexes",
        ] {
            for option in ["hint", "readConcern", "collation", "noCursorTimeout", "let"] {
                assert!(accepts(command, option, &object([])).is_none());
            }
        }
        let body = fields([("ok", BsonValue::Double(1.0))]);
        assert_eq!(reply(body.clone(), false), body);
        assert_eq!(
            reply(body, true).get_first("briskdbReadWarnings"),
            Some(&BsonValue::Array(vec![BsonValue::from(HINT_WARNING)]))
        );
        assert_eq!(accepts("getMore", "comment", &object([])), Some(true));
        assert!(accepts("getMore", "hint", &object([])).is_none());
    }
}
