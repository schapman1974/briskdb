use super::*;

#[test]
fn entry_checksum_has_frozen_bytes_and_binds_every_identity() {
    let collection = DocumentCollectionId::from_validated(7);
    let index = DocumentIndexId::from_validated(13);
    let record = [0x42; 32];
    let digest = checksum(
        collection,
        index,
        3,
        b"canonical-id",
        b"BDIK-tuple",
        &record,
    );
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        hex,
        "67e8e7a61c0605511b9e71389967e2243cafc0821c9706a060f6ffcaa1da2dd4"
    );
    for other in [
        checksum(
            DocumentCollectionId::from_validated(8),
            index,
            3,
            b"canonical-id",
            b"BDIK-tuple",
            &record,
        ),
        checksum(
            collection,
            DocumentIndexId::from_validated(14),
            3,
            b"canonical-id",
            b"BDIK-tuple",
            &record,
        ),
        checksum(
            collection,
            index,
            4,
            b"canonical-id",
            b"BDIK-tuple",
            &record,
        ),
        checksum(
            collection,
            index,
            3,
            b"canonical-ie",
            b"BDIK-tuple",
            &record,
        ),
        checksum(
            collection,
            index,
            3,
            b"canonical-id",
            b"BDIK-tuplf",
            &record,
        ),
        checksum(
            collection,
            index,
            3,
            b"canonical-id",
            b"BDIK-tuple",
            &[0x43; 32],
        ),
    ] {
        assert_ne!(other, digest);
    }
}
