"""Optional durable queue adapter for S3 overlay point updates.

Nothing creates queues, changes IAM, or starts background threads implicitly.
Use a FIFO queue + DLQ, ReportBatchItemFailures, and alarm on DLQ depth/queue age.
Queue acknowledgement is NOT database commitment. The native operation ID is
the duplicate-safety authority; SQS's finite deduplication window is not enough.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import logging
import re
import time
from typing import Any
from urllib.parse import urlsplit

from .s3_overlay import Database, RetryOptions, UpdateRequest, _request_json

_log = logging.getLogger(__name__)
_MAX_BODY = 256 * 1024


def _guard_queued_replacement(request: dict[str, Any], version_column: str | None):
    changes = request.get("set", {})
    if not changes:
        return  # Atomic increments still use native operation-ID deduplication.
    expected = request.get("expected", {})
    if version_column:
        version = expected.get(version_column)
        if (isinstance(version, dict) and set(version) == {"Integer"}
                and type(version["Integer"]) is int
                and request.get("increment", {}).get(version_column) == {"Integer": 1}
                and version_column not in changes):
            return
        raise ValueError("queued edits require the expected version and incrementing that version by one")
    if not set(changes).issubset(expected):
        raise ValueError("queued edits require expected old values for every replaced field, or a configured version column")


class SqsUpdateQueue:
    """An existing, private FIFO queue. Install the optional `s3-queue` extra.

    `send_timeout_ms` splits the network budget across connect/read timeouts.
    DNS, credentials, and OS scheduling can exceed it; it is not a wall-clock SLA.
    Pass a configured client to integrate another trusted credential setup.
    """
    def __init__(self, queue_url: str, *, region: str, send_timeout_ms: int = 250, client=None):
        parsed = urlsplit(queue_url)
        if (parsed.scheme != "https" or parsed.username or parsed.password or parsed.port
                or parsed.query or parsed.fragment
                or not re.fullmatch(r"sqs\.[a-z0-9-]+\.amazonaws\.com(?:\.cn)?", parsed.hostname or "")
                or not re.fullmatch(r"/\d{12}/[A-Za-z0-9_-]+\.fifo", parsed.path)
                or parsed.hostname.split(".")[1] != region):
            raise ValueError("queue_url must be a regional HTTPS SQS FIFO queue in the supplied region")
        if type(send_timeout_ms) is not int or not 50 <= send_timeout_ms <= 30_000:
            raise ValueError("send_timeout_ms must be an integer in 50..30000")
        if client is None:
            import boto3
            from botocore.config import Config
            # One reusable client; no hidden SDK retry can multiply the budget.
            client = boto3.client("sqs", region_name=region, config=Config(
                connect_timeout=send_timeout_ms / 2000,
                read_timeout=send_timeout_ms / 2000,
                retries={"mode": "standard", "total_max_attempts": 1},
            ))
        self._client = client
        self.queue_url = queue_url
        self.send_timeout_ms = send_timeout_ms

    def send(self, body: str, *, group_id: str, operation_id: str) -> dict[str, Any]:
        if len(body.encode("utf-8")) > _MAX_BODY:
            raise ValueError("queued update envelope exceeds 256 KiB")
        # Include intent in queue deduplication. A mistakenly reused ID with a
        # different body must reach native validation, not be silently discarded
        # by SQS as an identical successful submission.
        dedup = hashlib.sha256(body.encode("utf-8")).hexdigest()
        response = self._client.send_message(QueueUrl=self.queue_url, MessageBody=body,
            MessageGroupId=group_id, MessageDeduplicationId=dedup)
        if not response.get("MessageId"):
            raise RuntimeError("SQS did not acknowledge the update; reuse its operation ID")
        return {"status": "queued", "operation_id": operation_id,
                "message_id": response["MessageId"], "committed": False}


class QueuedUpdates:
    """Explicit acknowledgement policy; normal Database.update stays synchronous.

    version_column is trusted application configuration. Every writer of those
    records must maintain that version. Otherwise, supply expected old values
    for every field being replaced. FIFO does not order unrelated direct writers.
    """
    def __init__(self, database: Database, queue: SqsUpdateQueue, *, version_column: str | None = None):
        if version_column is not None and (not isinstance(version_column, str) or not version_column):
            raise ValueError("version_column must be a nonempty column name")
        self.database = database
        self.queue = queue
        self.version_column = version_column

    def submit(self, request: UpdateRequest, *, mode: str = "quick",
               retry: RetryOptions | None = None) -> dict[str, Any]:
        """committed / quick / queued. Queue errors propagate, never become success.

        Quick reserves the queue's send budget out of the native foreground
        budget. No new attempts start once that budget is spent. No worker is
        left running in the responding Lambda. Cold open is outside this method.
        """
        from . import BusyError, DeadlineExceededError, StorageUnavailableError
        if mode not in ("committed", "quick", "queued"):
            raise ValueError("mode must be committed, quick, or queued")
        retry = RetryOptions() if retry is None else retry
        if not isinstance(retry, RetryOptions):
            raise TypeError("retry must be RetryOptions")
        started = time.monotonic()
        encoded = _request_json(request)  # Snapshot mutable input once.
        if mode == "committed":
            return self.database._update_serialized(encoded, retry)
        content = json.loads(encoded)
        _guard_queued_replacement(content, self.version_column)
        target = json.loads(self.database._db.update_target(encoded))
        body = json.dumps({"format": 1, "database_id": target["database_id"], "request": content},
                          separators=(",", ":"), sort_keys=True, allow_nan=False)
        if len(body.encode("utf-8")) > _MAX_BODY:
            raise ValueError("queued update envelope exceeds 256 KiB")
        if mode == "quick":
            remaining = retry.timeout_ms - self.queue.send_timeout_ms - int((time.monotonic() - started) * 1000)
            if remaining > 0:
                try:
                    return self.database._update_serialized(encoded, replace(retry, timeout_ms=remaining))
                except (BusyError, DeadlineExceededError, StorageUnavailableError) as error:
                    # Unknown commits are reconciled by the SAME operation ID in
                    # the worker. Never replay under a newly generated ID.
                    _log.info("overlay_update_handoff operation_id=%s reason=%s",
                              content["operation_id"], type(error).__name__)
        result = self.queue.send(body, group_id=target["message_group_id"], operation_id=content["operation_id"])
        result["foreground_ms"] = (time.monotonic() - started) * 1000
        return result


def process_batch(database: Database, event: dict[str, Any], *, queue_arn: str,
                  version_column: str | None = None, retry: RetryOptions | None = None,
                  remaining_time_ms=None) -> dict[str, Any]:
    """FIFO Lambda handler core. Configure ReportBatchItemFailures and a DLQ.

    Only a deployment-configured database and queue ARN are accepted; messages
    cannot select filesystem roots, credentials, native libraries, or raw SQL.
    Failed/unprocessed messages remain queued. Condition failures are terminal
    native receipts, visible through update_status, not silently retried forever.
    """
    if not re.fullmatch(r"arn:(?:aws|aws-cn|aws-us-gov):sqs:[a-z0-9-]+:\d{12}:[A-Za-z0-9_-]+\.fifo", queue_arn):
        raise ValueError("queue_arn must identify the trusted FIFO queue")
    records = event["Records"]
    if not isinstance(records, list) or any(not isinstance(r, dict) or not r.get("messageId") for r in records):
        raise ValueError("invalid SQS batch")
    retry = RetryOptions(timeout_ms=30_000, max_retries=8, allow_compaction=True) if retry is None else retry
    if not isinstance(retry, RetryOptions):
        raise TypeError("retry must be RetryOptions")
    for index, record in enumerate(records):
        operation_id = "unvalidated"
        try:
            if record.get("eventSource") != "aws:sqs" or record.get("eventSourceARN") != queue_arn:
                raise ValueError("unexpected SQS event source")
            raw = record["body"]
            if not isinstance(raw, str) or len(raw.encode("utf-8")) > _MAX_BODY:
                raise ValueError("invalid update message size")
            body = json.loads(raw)
            if set(body) != {"format", "database_id", "request"} or type(body["format"]) is not int or body["format"] != 1:
                raise ValueError("invalid update envelope")
            encoded = json.dumps(body["request"], allow_nan=False, separators=(",", ":"))
            target = json.loads(database._db.update_target(encoded))
            if target["database_id"] != body["database_id"] or record.get("attributes", {}).get("MessageGroupId") != target["message_group_id"]:
                raise ValueError("update belongs to another database or FIFO group")
            operation_id = body["request"]["operation_id"]
            _guard_queued_replacement(body["request"], version_column)
            options = retry
            if remaining_time_ms is not None:
                remaining = int(remaining_time_ms()) - 1000
                if remaining <= 0:
                    raise TimeoutError("insufficient Lambda time to start another update")
                options = replace(retry, timeout_ms=min(retry.timeout_ms, remaining))
            result = database._update_serialized(encoded, options)
            _log.info("overlay_update_result operation_id=%s status=%s", operation_id, result["status"])
        except Exception as error:
            # Actionable catch: tell Lambda exactly which messages must remain
            # queued. Do not acknowledge a failure or process later FIFO messages.
            _log.warning("overlay_update_retry operation_id=%s error_type=%s", operation_id, type(error).__name__)
            return {"batchItemFailures": [{"itemIdentifier": r["messageId"]} for r in records[index:]]}
    return {"batchItemFailures": []}
