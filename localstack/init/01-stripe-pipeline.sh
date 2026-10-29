#!/usr/bin/env bash
# Creates the local version of the production pipeline:
#
#   EventBridge bus (named like Stripe's partner bus)
#     -> rule matching source prefix aws.partner/stripe.com
#       -> SQS queue (visibility 60s) with a redrive policy
#         -> dead-letter queue after 5 failed receives
#   plus a DynamoDB table for idempotency, with TTL on expires_at.
#
# LocalStack runs this from /etc/localstack/init/ready.d when it starts.
# It's safe to run again: everything is create-or-update.
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"
BUS="aws.partner/stripe.com/ed_local_development"
RULE="stripe-events-to-sqs"
QUEUE="stripe-events"
DLQ="stripe-events-dlq"
TABLE="stripe-events-dedupe"

echo "creating event bus ${BUS}"
awslocal events create-event-bus --name "${BUS}" >/dev/null 2>&1 || true

echo "creating queues"
DLQ_URL=$(awslocal sqs create-queue --queue-name "${DLQ}" \
  --attributes MessageRetentionPeriod=1209600 \
  --query QueueUrl --output text)
DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:${DLQ}"

QUEUE_URL=$(awslocal sqs create-queue --queue-name "${QUEUE}" --query QueueUrl --output text)
QUEUE_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:${QUEUE}"
RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT}:rule/${BUS}/${RULE}"

# Visibility timeout 60s (the worker's handler timeout is 30s). After 5
# receives without a delete, SQS moves the message to the DLQ.
cat > /tmp/queue-attributes.json <<JSON
{
  "VisibilityTimeout": "60",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"5\"}",
  "Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"AllowEventBridgeRule\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"events.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"${QUEUE_ARN}\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"${RULE_ARN}\"}}}]}"
}
JSON
awslocal sqs set-queue-attributes --queue-url "${QUEUE_URL}" --attributes file:///tmp/queue-attributes.json

echo "creating rule ${RULE}"
awslocal events put-rule \
  --name "${RULE}" \
  --event-bus-name "${BUS}" \
  --event-pattern '{"source":[{"prefix":"aws.partner/stripe.com"}]}' >/dev/null
awslocal events put-targets \
  --rule "${RULE}" \
  --event-bus-name "${BUS}" \
  --targets "Id=stripe-events-queue,Arn=${QUEUE_ARN}" >/dev/null

echo "creating dedupe table ${TABLE}"
if ! awslocal dynamodb describe-table --table-name "${TABLE}" >/dev/null 2>&1; then
  awslocal dynamodb create-table \
    --table-name "${TABLE}" \
    --attribute-definitions AttributeName=event_id,AttributeType=S \
    --key-schema AttributeName=event_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
  awslocal dynamodb wait table-exists --table-name "${TABLE}"
fi
awslocal dynamodb update-time-to-live \
  --table-name "${TABLE}" \
  --time-to-live-specification Enabled=true,AttributeName=expires_at >/dev/null 2>&1 || true

echo "stripe pipeline ready: QUEUE_URL=${QUEUE_URL} DLQ_URL=${DLQ_URL}"
