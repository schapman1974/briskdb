"""Optional FIFO writer: mount the overlay root and configure its trusted ARN.

Deployment requirements:
- Private, encrypted FIFO queue and FIFO DLQ; bounded redrive attempts.
- ReportBatchItemFailures on the Lambda event source mapping.
- Queue visibility >= six times the Lambda timeout; alarm on DLQ depth and age.
- Least-privilege SQS consumption, S3 prefix GetObject/PutObject, bucket
  ListBucket (to distinguish missing receipts from denied access), EFS mount, and
  KMS access as needed. TLS, encrypted logs, and audited data access.
- BRISKDB_STORAGE_MODE=s3-overlay, BRISKDB_OVERLAY_ROOT, BRISKDB_UPDATE_QUEUE_ARN.
- Optional BRISKDB_UPDATE_VERSION_COLUMN when all writers maintain that version.

Opening is per invocation, not a permanently warm database connection. The
adapter uses native operation IDs, so retries after process loss remain safe.
No deployment or infrastructure is created by importing/running this module.
"""
import os

from briskdb.s3_overlay import Database
from briskdb.s3_overlay_queue import process_batch


def handler(event, context):
    with Database.from_env() as database:
        return process_batch(database, event,
            queue_arn=os.environ["BRISKDB_UPDATE_QUEUE_ARN"],
            version_column=os.environ.get("BRISKDB_UPDATE_VERSION_COLUMN"),
            remaining_time_ms=context.get_remaining_time_in_millis)
