#![cfg(feature = "mongo")]

use briskdb::{BriskDb, DocumentSupport, protocol::mongo::MongoServer};
use std::process::Command;

/// Executes unchanged frozen cases through the ordinary BriskDB PyMongo adapter.
/// Kept separate from the reference-only report until the full corpus passes.
#[tokio::test]
#[ignore = "requires the frozen Mongo contract Python dependencies"]
async fn frozen_supported_contracts_against_real_briskdb_endpoint() {
    let root = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(4)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let uri = format!("mongodb://{}/?directConnection=true", server.address());
    let report_root = tempfile::tempdir().unwrap();
    let report = std::env::var_os("BRISKDB_MONGO_CONTRACT_REPORT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| report_root.path().join("candidate-aggregation.xml"));
    let output = tokio::task::spawn_blocking(move || run_contract(&uri, &report))
        .await
        .unwrap();
    server.close().await.unwrap();
    database.close().await.unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

fn run_contract(uri: &str, report: &std::path::Path) -> std::process::Output {
    let python =
        std::env::var("BRISKDB_MONGO_CONTRACT_PYTHON").unwrap_or_else(|_| "python3".to_owned());
    let output = Command::new(&python)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-c",
            "/dev/null",
        ])
        .arg("--rootdir")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .args([
            "compat/mongo/v1/runner/contracts/test_aggregation_basic_stages_contract.py",
            "compat/mongo/v1/runner/contracts/test_aggregation_projection_stages_contract.py",
            "compat/mongo/v1/runner/contracts/test_aggregation_contract.py",
            "compat/mongo/v1/runner/contracts/test_group_accumulators_contract.py",
            "compat/mongo/v1/runner/contracts/test_client_read_fidelity_contract.py",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_min_and_max_follow_bson_order_and_report_noops",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_min_and_max_include_null_in_whole_bson_value_order",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_rename_moves_nested_values_overwrites_and_ignores_missing_source",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_pop_handles_front_back_nested_empty_and_missing_arrays",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_new_update_operators_follow_numeric_array_paths",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_malformed_new_update_operands_report_mongodb_codes",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_update_path_conflicts_report_code_40_before_writing",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_new_update_operators_preserve_immutable_id_semantics",
            "compat/mongo/v1/runner/contracts/test_update_operator_contract.py::test_new_update_operators_report_target_and_path_errors_atomically",
            "compat/mongo/v1/runner/contracts/test_query_operator_contract.py::test_comment_remains_invalid_as_a_field_operator",
            "compat/mongo/v1/runner/contracts/test_query_operator_contract.py::test_add_to_set_non_array_errors_report_code_2_and_leave_document_atomic",
            "compat/mongo/v1/runner/contracts/test_array_update_contract.py",
            "compat/mongo/v1/runner/contracts/test_bson_comparison_contract.py::test_tm036_pull_reuses_unbounded_min_max_key_ranges",
            "compat/mongo/v1/runner/contracts/test_bson_comparison_contract.py::test_pull_reuses_recursive_bson_range_comparison",
            "compat/mongo/v1/runner/contracts/test_bson_comparison_contract.py::test_pull_document_ranges_share_missing_and_array_path_semantics",
            "compat/mongo/v1/runner/contracts/test_query_operator_contract.py::test_every_filtering_crud_entrypoint_rejects_invalid_not_operands",
            "compat/mongo/v1/runner/contracts/test_query_operator_contract.py::test_every_filtering_crud_entrypoint_rejects_operator_typos",
            "compat/mongo/v1/runner/contracts/test_talkpython_contract.py::test_replace_one_preserves_id_and_replaces_the_full_document",
            "compat/mongo/v1/runner/contracts/test_talkpython_contract.py::test_write_result_metadata_used_by_the_application",
            "compat/mongo/v1/runner/contracts/test_talkpython_contract.py::test_binary_ids_use_bson_equality_without_losing_subtype",
            "compat/mongo/v1/runner/contracts/test_talkpython_contract.py::test_boolean_and_numeric_ids_are_bson_distinct",
            "compat/mongo/v1/runner/contracts/test_crud_contract.py::test_unset_removes_top_level_and_nested_fields",
            "compat/mongo/v1/runner/contracts/test_crud_contract.py::test_update_and_result_metadata",
            "compat/mongo/v1/runner/contracts/test_talkpython_contract.py::test_inc_creates_a_missing_counter",
            "compat/mongo/v1/runner/contracts/test_decimal128_contract.py::test_decimal128_inc_promotes_the_result",
            "compat/mongo/v1/runner/contracts/test_decimal128_contract.py::test_decimal128_representation_changes_are_persisted",
            "compat/mongo/v1/runner/contracts/test_crud_contract.py::test_replace_upsert_preserves_equality_bound_id",
            "compat/mongo/v1/runner/contracts/test_crud_contract.py::test_replace_upsert_stores_id_first",
            "compat/mongo/v1/runner/contracts/test_crud_contract.py::test_replace_upsert_accepts_bson_equal_filter_and_replacement_ids",
            "compat/mongo/v1/runner/contracts/test_crud_contract.py::test_replace_upsert_rejects_conflicting_filter_and_replacement_ids",
        ])
        .args([
            "--mongo-contract-target=briskdb",
            "--mongo-contract-api=both",
            "--mongo-contract-require-target",
            "--mongo-contract-briskdb-uri",
            uri,
        ])
        .arg("--junitxml")
        .arg(report)
        .output()
        .expect("launch frozen contract runner");
    if output.status.success() {
        // Require the exact locked case/API set, not just pytest's exit code:
        // accidental filtering, skips, or duplicated cases must fail the gate.
        let validation = Command::new(python)
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args([
                "-c",
                r#"
import json
import sys
from pathlib import Path
from scripts.mongo_parity import ingest_junit

with open('compat/mongo/v1/corpus.json', encoding='utf-8') as source:
    corpus = json.load(source)
modules = {
    'tests.contracts.test_aggregation_basic_stages_contract',
    'tests.contracts.test_aggregation_projection_stages_contract',
    'tests.contracts.test_aggregation_contract',
    'tests.contracts.test_group_accumulators_contract',
    'tests.contracts.test_client_read_fidelity_contract',
    'tests.contracts.test_array_update_contract',
}
individual = {'tests.contracts.test_talkpython_contract::' + name for name in [
    'test_replace_one_preserves_id_and_replaces_the_full_document',
    'test_write_result_metadata_used_by_the_application',
    'test_binary_ids_use_bson_equality_without_losing_subtype',
    'test_boolean_and_numeric_ids_are_bson_distinct',
    'test_inc_creates_a_missing_counter',
]}
individual.update('tests.contracts.test_query_operator_contract::' + name for name in [
    'test_comment_remains_invalid_as_a_field_operator',
    'test_add_to_set_non_array_errors_report_code_2_and_leave_document_atomic',
    'test_every_filtering_crud_entrypoint_rejects_invalid_not_operands',
    'test_every_filtering_crud_entrypoint_rejects_operator_typos',
])
individual.update('tests.contracts.test_update_operator_contract::' + name for name in [
    'test_min_and_max_follow_bson_order_and_report_noops',
    'test_min_and_max_include_null_in_whole_bson_value_order',
    'test_rename_moves_nested_values_overwrites_and_ignores_missing_source',
    'test_pop_handles_front_back_nested_empty_and_missing_arrays',
    'test_new_update_operators_follow_numeric_array_paths',
    'test_malformed_new_update_operands_report_mongodb_codes',
    'test_update_path_conflicts_report_code_40_before_writing',
    'test_new_update_operators_preserve_immutable_id_semantics',
    'test_new_update_operators_report_target_and_path_errors_atomically',
])
individual.update('tests.contracts.test_bson_comparison_contract::' + name for name in [
    'test_tm036_pull_reuses_unbounded_min_max_key_ranges',
    'test_pull_reuses_recursive_bson_range_comparison',
    'test_pull_document_ranges_share_missing_and_array_path_semantics',
])
individual.update('tests.contracts.test_decimal128_contract::' + name for name in [
    'test_decimal128_inc_promotes_the_result',
    'test_decimal128_representation_changes_are_persisted',
])
individual.add('tests.contracts.test_crud_contract::test_update_and_result_metadata')
individual.update('tests.contracts.test_crud_contract::' + name for name in [
    'test_replace_upsert_preserves_equality_bound_id[query0-77]',
    'test_replace_upsert_preserves_equality_bound_id[query1-pinned-key]',
    'test_replace_upsert_preserves_equality_bound_id[query2-88]',
    'test_replace_upsert_preserves_equality_bound_id[query3-None]',
    'test_replace_upsert_stores_id_first',
    'test_replace_upsert_accepts_bson_equal_filter_and_replacement_ids',
    'test_replace_upsert_rejects_conflicting_filter_and_replacement_ids',
])
expected = {(case['id'], api) for case in corpus['cases']
            if (case['id'].split('::', 1)[0] in modules or case['id'] in individual or
                case['id'] == 'tests.contracts.test_crud_contract::test_unset_removes_top_level_and_nested_fields')
            for api in case['apis']}
executions = ingest_junit(Path(sys.argv[1]), 'briskdb', corpus)['executions']
actual = {(item['case_id'], item['api']) for item in executions}
assert len(expected) == len(executions) == 238, ('locked suite coverage changed', len(expected), len(executions))
assert actual == expected, 'candidate suite omitted or substituted locked cases'
assert all(item['outcome'] == 'passed' and item['target'] == 'briskdb-briskdb'
           for item in executions), 'candidate suite skipped or failed a case'
print('Verified all 238 exact frozen candidate executions, with no skips.')
"#,
            ])
            .arg(report)
            .output()
            .expect("validate candidate JUnit coverage");
        if !validation.status.success() {
            return validation;
        }
        println!("{}", String::from_utf8_lossy(&validation.stdout));
    }
    output
}
