#!/bin/bash

#####
# send-to-sqs.sh
# Purpose: Duplicati "run script" hook (--run-script-after) that publishes a backup job's
#          result output to an AWS SQS queue, for use with dupReport's protocol=sqs incoming
#          server support. Shell/AWS-CLI equivalent of send-to-sqs.py.
#
# Requires: AWS CLI (aws) installed and configured with credentials that can send messages
#           to the target queue - e.g. via `aws configure`, an IAM instance role, or the
#           AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables. Unlike send-to-sqs.py,
#           this script does not read credentials from sqs.ini; it relies entirely on however
#           the aws CLI is configured on this machine.
#
# Config:   Set SQS_QUEUE_URL below, or export it in the environment before Duplicati runs this
#           script, so the same script can be reused across hosts without editing it.
#           The queue must be a FIFO queue (name ends in .fifo), since this script uses
#           --message-group-id/--message-deduplication-id, which standard queues reject.
#####

set -euo pipefail

# Full URL of the target FIFO queue. Override by exporting SQS_QUEUE_URL before invoking this
# script instead of editing the default below.
SQS_QUEUE_URL="${SQS_QUEUE_URL:-https://sqs.us-east-1.amazonaws.com/123456789012/DuplicatiQueue.fifo}"

# Only backup jobs produce a report worth sending. Other operations (e.g. Delete, Repair) are ignored.
if [ "${DUPLICATI__OPERATIONNAME:-}" != "Backup" ]; then
    exit 0
fi

if [ -z "${DUPLICATI__RESULTFILE:-}" ] || [ ! -f "$DUPLICATI__RESULTFILE" ]; then
    echo "send-to-sqs.sh: DUPLICATI__RESULTFILE is not set or does not exist; nothing to send." 1>&2
    exit 0
fi

if ! command -v aws >/dev/null 2>&1; then
    echo "send-to-sqs.sh: aws CLI not found in PATH." 1>&2
    exit 1
fi

backupName="${DUPLICATI__backup_name:-${DUPLICATI__backup_id:-Unknown}}"
# Escape backslashes and double quotes so the backup name can't break the JSON payload below.
backupNameEscaped=$(printf '%s' "$backupName" | sed 's/\\/\\\\/g; s/"/\\"/g')

dedupId=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')
# RFC 2822 date, matching the format email.utils.formatdate() produces for send-to-sqs.py,
# which dupReport parses on the receiving end. Requires GNU date (date -R); not available on
# BSD/macOS date - install GNU coreutils or swap in an equivalent RFC 2822 date command there.
msgDate=$(date -R)

# A failed publish here is surfaced as a non-zero exit, which Duplicati logs as a warning
# (per --run-script-after semantics) without affecting the backup job itself. Append `|| true`
# to the --run-script-after invocation of this script if you'd rather it fail silently.
aws sqs send-message \
    --queue-url "$SQS_QUEUE_URL" \
    --message-body "file://$DUPLICATI__RESULTFILE" \
    --message-deduplication-id "$dedupId" \
    --message-group-id "DuplicatiReports" \
    --message-attributes "{\"subject\":{\"StringValue\":\"Duplicati Backup report for ${backupNameEscaped}\",\"DataType\":\"String\"},\"date\":{\"StringValue\":\"${msgDate}\",\"DataType\":\"String\"}}"
