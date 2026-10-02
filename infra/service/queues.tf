locals {
  queue_timeouts = {
    ingress  = 30
    pull     = 900
    delivery = 180
  }
}

resource "aws_sqs_queue" "dlq" {
  for_each                  = local.queue_timeouts
  name                      = "data-fetch-service-${each.key}-dlq"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "work" {
  for_each                   = local.queue_timeouts
  name                       = "data-fetch-service-${each.key}"
  visibility_timeout_seconds = each.value * 6
  message_retention_seconds  = 345600
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[each.key].arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  for_each  = local.queue_timeouts
  queue_url = aws_sqs_queue.dlq[each.key].id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.work[each.key].arn]
  })
}

resource "aws_sqs_queue_policy" "ingress" {
  queue_url = aws_sqs_queue.work["ingress"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Sid       = "ServiceScheduler"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.scheduler.arn }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.work["ingress"].arn
      }],
      [for arn in var.external_producer_role_arns : {
        Sid       = "Producer${substr(sha256(arn), 0, 12)}"
        Effect    = "Allow"
        Principal = { AWS = arn }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.work["ingress"].arn
      }],
      [for arn in var.native_eventbridge_rule_arns : {
        Sid       = "NativeRule${substr(sha256(arn), 0, 12)}"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.work["ingress"].arn
        Condition = { ArnEquals = { "aws:SourceArn" = arn } }
      }]
    )
  })
}
