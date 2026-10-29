# EXAMPLE ONLY. This is a minimal starting point for real AWS, not a
# production module. Read it, adjust it, and run `terraform plan` before you
# apply anything.
#
# Prerequisite: create the Amazon EventBridge destination in Stripe first.
# Stripe then creates a pending partner event source in your account named
# aws.partner/stripe.com/ed_..., and you pass that name in as
# stripe_event_source_name. Creating the bus below associates it.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  description = "The AWS region you picked when creating the Stripe event destination."
  type        = string
}

variable "stripe_event_source_name" {
  description = "The partner event source Stripe created, e.g. aws.partner/stripe.com/ed_61Pg..."
  type        = string

  validation {
    condition     = startswith(var.stripe_event_source_name, "aws.partner/stripe.com/")
    error_message = "This should be the Stripe partner event source name, starting with aws.partner/stripe.com/."
  }
}

variable "name" {
  description = "Prefix for the queue, rule, and table names."
  type        = string
  default     = "stripe-events"
}

# Associates Stripe's partner event source with a partner event bus. For
# partner buses the name must match the event source name.
resource "aws_cloudwatch_event_bus" "stripe" {
  name              = var.stripe_event_source_name
  event_source_name = var.stripe_event_source_name
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = 1209600 # 14 days, the SQS maximum
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "events" {
  name = var.name
  # Keep this comfortably above the worker's handler timeout (30s).
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600 # 4 days
  sqs_managed_sse_enabled    = true

  # After 5 receives without a delete, SQS moves the message to the DLQ.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_event_rule" "stripe_to_sqs" {
  name           = "${var.name}-to-sqs"
  event_bus_name = aws_cloudwatch_event_bus.stripe.name
  description    = "Send every Stripe event on the partner bus to SQS."

  event_pattern = jsonencode({
    source = [{ prefix = "aws.partner/stripe.com" }]
  })
}

resource "aws_cloudwatch_event_target" "sqs" {
  rule           = aws_cloudwatch_event_rule.stripe_to_sqs.name
  event_bus_name = aws_cloudwatch_event_bus.stripe.name
  arn            = aws_sqs_queue.events.arn
}

# Lets this one rule, and nothing else, send to the queue.
resource "aws_sqs_queue_policy" "events" {
  queue_url = aws_sqs_queue.events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowStripeRule"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.events.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.stripe_to_sqs.arn }
      }
    }]
  })
}

resource "aws_dynamodb_table" "dedupe" {
  name         = "${var.name}-dedupe"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"

  attribute {
    name = "event_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

output "queue_url" {
  description = "Set this as QUEUE_URL for the worker."
  value       = aws_sqs_queue.events.id
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.id
}

output "dedupe_table" {
  description = "Set this as DEDUPE_TABLE for the worker."
  value       = aws_dynamodb_table.dedupe.name
}
