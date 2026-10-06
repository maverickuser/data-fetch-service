data "aws_caller_identity" "current" {}

locals {
  lambda_kinds     = toset(["api", "admission", "pull", "delivery", "reconciler"])
  state_arn        = aws_s3_bucket.state.arn
  artifact_arn     = aws_s3_bucket.artifacts.arn
  state_prefixes   = ["coordination", "acceptance", "requests", "runs", "listings"]
  state_objects    = [for prefix in local.state_prefixes : "${local.state_arn}/${prefix}/*"]
  artifact_objects = ["${local.artifact_arn}/runs/*"]
  queue_consumers = {
    admission = "ingress"
    pull      = "pull"
    delivery  = "delivery"
  }
  queue_publishers = {
    # Admission and API repair pending dispatches for joined runs, which may target either queue.
    api        = [aws_sqs_queue.work["pull"].arn, aws_sqs_queue.work["delivery"].arn]
    admission  = [aws_sqs_queue.work["pull"].arn, aws_sqs_queue.work["delivery"].arn]
    pull       = [aws_sqs_queue.work["delivery"].arn]
    delivery   = []
    reconciler = [aws_sqs_queue.work["pull"].arn, aws_sqs_queue.work["delivery"].arn]
  }
  # Reconciler resumes interrupted deliveries; API writes delivery-retry child manifests.
  artifact_readers   = toset(["api", "pull", "delivery", "reconciler"])
  artifact_writers   = toset(["api", "pull"])
  processor_invokers = toset(["delivery", "reconciler"])
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  for_each           = local.lambda_kinds
  name               = "data-fetch-service-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "lambda" {
  for_each = local.lambda_kinds
  name     = "data-fetch-service-${each.key}-runtime"
  role     = aws_iam_role.lambda[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Sid      = "OwnLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["${aws_cloudwatch_log_group.lambda[each.key].arn}:*"]
        }, {
        Sid      = "StateObjectReadWrite"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = local.state_objects
        }, {
        # Unconditioned so S3 reports a missing key as 404; GetObject carries no s3:prefix key.
        Sid      = "StateListing"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = local.state_arn
      }],
      [for kind in [each.key] : {
        Sid      = "ReadRunArtifacts"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = local.artifact_objects
      } if contains(local.artifact_readers, kind)],
      [for kind in [each.key] : {
        Sid      = "ListRunArtifactsForMissingObjectResolution"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = local.artifact_arn
      } if contains(local.artifact_readers, kind)],
      [for kind in [each.key] : {
        Sid      = "WriteRunArtifacts"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = local.artifact_objects
      } if contains(local.artifact_writers, kind)],
      [for kind in [each.key] : {
        Sid      = "ConsumeOwnQueue"
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = aws_sqs_queue.work[local.queue_consumers[each.key]].arn
      } if contains(keys(local.queue_consumers), kind)],
      [for kind in [each.key] : {
        Sid      = "PublishInternalWork"
        Effect   = "Allow"
        Action   = "sqs:SendMessage"
        Resource = local.queue_publishers[each.key]
      } if length(local.queue_publishers[kind]) > 0],
      [for kind in [each.key] : {
        Sid      = "SubmitToProcessorAPI"
        Effect   = "Allow"
        Action   = "execute-api:Invoke"
        Resource = local.processor.processor_submission_route_arn
      } if contains(local.processor_invokers, kind)]
    )
  })
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "data-fetch-service-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "data-fetch-service-scheduler-targets"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ScheduledIngress"
      Effect   = "Allow"
      Action   = "sqs:SendMessage"
      Resource = aws_sqs_queue.work["ingress"].arn
      }, {
      Sid      = "ScheduledReconciliation"
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.handler["reconciler"].arn
    }]
  })
}
