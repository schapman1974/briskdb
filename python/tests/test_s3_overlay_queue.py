import json
from dataclasses import replace

import pytest

import briskdb
from briskdb.s3_overlay import Database, RetryOptions, UpdateRequest, _request_json
from briskdb.s3_overlay_queue import QueuedUpdates, SqsUpdateQueue, process_batch

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/updates.fifo"
QUEUE_ARN = "arn:aws:sqs:us-east-1:123456789012:updates.fifo"


class Native:
    def __init__(self):
        self.calls = []
        self.failure = None
    def update_target(self, raw):
        self.calls.append(("target", json.loads(raw)))
        return json.dumps({"database_id": "db", "message_group_id": "db:0:1", "table": "notes", "partition": 1})
    def update(self, raw, options):
        self.calls.append(("update", json.loads(raw), json.loads(options)))
        if self.failure:
            raise self.failure
        return json.dumps({"status": "committed", "operation_id": json.loads(raw)["operation_id"], "affected_rows": 1})
    def update_status(self, operation_id, timeout_ms):
        return "null"


class Client:
    def __init__(self):
        self.calls = []
        self.failure = None
    def send_message(self, **kwargs):
        self.calls.append(kwargs)
        if self.failure:
            raise self.failure
        return {"MessageId": "accepted"}


def fixture():
    db = Database.__new__(Database)
    db._db = Native()
    client = Client()
    queue = SqsUpdateQueue(QUEUE_URL, region="us-east-1", client=client)
    request = UpdateRequest("notes", {"id": "one"}, set={"text": "new"}, expected={"text": "old"})
    return db, client, QueuedUpdates(db, queue), request


def test_quick_commit_does_not_send_to_queue():
    db, client, writer, request = fixture()
    result = writer.submit(request)
    assert result["status"] == "committed"
    assert not client.calls
    options = db._db.calls[-1][2]
    assert 0 < options["timeout_ms"] <= 750
    assert options["max_retries"] == 2


@pytest.mark.parametrize("error", [briskdb.BusyError, briskdb.DeadlineExceededError, briskdb.StorageUnavailableError])
def test_retryable_or_unknown_outcome_hands_off_identical_operation(error):
    db, client, writer, request = fixture()
    db._db.failure = error("test failure")
    result = writer.submit(request)
    assert result["status"] == "queued" and not result["committed"]
    assert result["operation_id"] == request.operation_id
    sent = client.calls[0]
    assert sent["MessageGroupId"] == "db:0:1"
    assert json.loads(sent["MessageBody"])["request"] == json.loads(_request_json(request))


@pytest.mark.parametrize("error", [briskdb.PermissionDeniedError, briskdb.ReadOnlyError,
                                   briskdb.InvalidArgumentError, briskdb.IdempotencyConflictError])
def test_permanent_error_is_not_silently_queued(error):
    db, client, writer, request = fixture()
    db._db.failure = error("permanent")
    with pytest.raises(error):
        writer.submit(request)
    assert not client.calls


def test_failed_queue_handoff_is_not_acknowledged():
    db, client, writer, request = fixture()
    db._db.failure = briskdb.BusyError("collision")
    client.failure = TimeoutError("SQS response lost")
    with pytest.raises(TimeoutError):
        writer.submit(request)


def test_queued_mode_and_too_short_budget_do_not_attempt_foreground_write():
    db, client, writer, request = fixture()
    writer.submit(request, mode="queued")
    writer.submit(request, retry=RetryOptions(timeout_ms=100))
    assert len(client.calls) == 2
    assert not any(call[0] == "update" for call in db._db.calls)


def test_queued_replacements_require_old_values_or_version_guard():
    db, client, writer, request = fixture()
    with pytest.raises(ValueError, match="expected old values"):
        writer.submit(replace(request, expected={}), mode="queued")
    guarded = QueuedUpdates(db, writer.queue, version_column="revision")
    with pytest.raises(ValueError, match="expected version"):
        guarded.submit(request)
    guarded.submit(replace(request, expected={"revision": 2}, increment={"revision": 1}), mode="queued")
    assert len(client.calls) == 1


def batch_from(client, count=1):
    sent = client.calls[-1]
    return {"Records": [{"messageId": str(i), "body": sent["MessageBody"],
        "eventSource": "aws:sqs", "eventSourceARN": QUEUE_ARN,
        "attributes": {"MessageGroupId": sent["MessageGroupId"]}} for i in range(count)]}


def test_worker_validates_routing_and_processes_native_idempotent_request():
    db, client, writer, request = fixture()
    writer.submit(request, mode="queued")
    assert process_batch(db, batch_from(client), queue_arn=QUEUE_ARN) == {"batchItemFailures": []}
    call = db._db.calls[-1]
    assert call[1]["operation_id"] == request.operation_id
    assert call[2]["allow_compaction"]


@pytest.mark.parametrize("mutation", ["database", "group", "queue", "source", "unguarded"])
def test_worker_never_acknowledges_invalid_or_misrouted_messages(mutation):
    db, client, writer, request = fixture()
    writer.submit(request, mode="queued")
    event = batch_from(client, 2)
    first = event["Records"][0]
    if mutation in ("database", "unguarded"):
        body = json.loads(first["body"])
        if mutation == "database": body["database_id"] = "other"
        else: body["request"]["expected"] = {}
        first["body"] = json.dumps(body)
    elif mutation == "group": first["attributes"]["MessageGroupId"] = "other"
    elif mutation == "queue": first["eventSourceARN"] = QUEUE_ARN.replace("updates", "other")
    else: first["eventSource"] = "other"
    assert process_batch(db, event, queue_arn=QUEUE_ARN) == {"batchItemFailures": [{"itemIdentifier": "0"}, {"itemIdentifier": "1"}]}
    assert not any(call[0] == "update" for call in db._db.calls)


def test_fifo_stops_after_failure_and_honors_lambda_remaining_budget():
    db, client, writer, request = fixture()
    writer.submit(request, mode="queued")
    event = batch_from(client, 3)
    db._db.failure = briskdb.BusyError("retry later")
    result = process_batch(db, event, queue_arn=QUEUE_ARN)
    assert len(result["batchItemFailures"]) == 3
    assert sum(call[0] == "update" for call in db._db.calls) == 1
    db._db.calls.clear()
    process_batch(db, event, queue_arn=QUEUE_ARN, remaining_time_ms=lambda: 800)
    assert not any(call[0] == "update" for call in db._db.calls)


@pytest.mark.parametrize("options", [{"timeout_ms": True}, {"max_retries": -1},
    {"backoff_ms": 200}, {"allow_compaction": "yes"}, {"timeout_ms": 0}])
def test_invalid_retry_options(options):
    with pytest.raises((ValueError, TypeError)):
        RetryOptions(**options)


@pytest.mark.parametrize("url", ["http://sqs.us-east-1.amazonaws.com/123456789012/a.fifo",
    "https://evil.example/123456789012/a.fifo", QUEUE_URL + "?extra=1", QUEUE_URL.replace(".fifo", "")])
def test_queue_url_fails_closed(url):
    with pytest.raises(ValueError):
        SqsUpdateQueue(url, region="us-east-1", client=Client())


def test_sdk_client_has_bounded_timeouts_and_no_hidden_retries(monkeypatch):
    import boto3
    calls = []
    monkeypatch.setattr(boto3, "client", lambda *args, **kw: calls.append((args, kw)) or Client())
    SqsUpdateQueue(QUEUE_URL, region="us-east-1", send_timeout_ms=400)
    config = calls[0][1]["config"]
    assert config.connect_timeout == config.read_timeout == 0.2
    assert config.retries == {"mode": "standard", "total_max_attempts": 1}


def test_request_validation_and_public_update_methods():
    db, client, writer, request = fixture()
    assert db.update(request)["operation_id"] == request.operation_id
    assert db.update_target(request)["database_id"] == "db"
    assert db.update_status(request.operation_id) is None
    for invalid in (replace(request, operation_id="not-an-id"),
                    replace(request, key={1: "bad-key"}),
                    replace(request, set={"text": float("nan")})):
        with pytest.raises((ValueError, TypeError)):
            db.update(invalid)


def test_worker_acknowledges_terminal_condition_result_not_as_saved():
    db, client, writer, request = fixture()
    writer.submit(request, mode="queued")
    db._db.update = lambda raw, options: json.dumps({"status": "condition_not_met", "affected_rows": 0})
    assert process_batch(db, batch_from(client), queue_arn=QUEUE_ARN) == {"batchItemFailures": []}


def test_queue_deduplication_distinguishes_reused_id_with_different_intent():
    db, client, writer, request = fixture()
    writer.submit(request, mode="queued")
    writer.submit(request, mode="queued")
    writer.submit(replace(request, set={"text": "other"}), mode="queued")
    assert client.calls[0]["MessageDeduplicationId"] == client.calls[1]["MessageDeduplicationId"]
    assert client.calls[0]["MessageDeduplicationId"] != client.calls[2]["MessageDeduplicationId"]
