locals {
  telemetry_operations = {
    api        = "request"
    admission  = "consume"
    pull       = "consume"
    delivery   = "consume"
    reconciler = "scan"
  }
}

resource "aws_cloudwatch_metric_alarm" "queue_backlog_age" {
  for_each            = local.queue_timeouts
  alarm_name          = "data-fetch-service-${each.key}-oldest-message"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = each.key == "pull" ? 7200 : 3600
  treat_missing_data  = "notBreaching"
  dimensions = {
    QueueName = aws_sqs_queue.work[each.key].name
  }
}

resource "aws_cloudwatch_metric_alarm" "dlq_visible" {
  for_each            = local.queue_timeouts
  alarm_name          = "data-fetch-service-${each.key}-dlq-visible"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  dimensions = {
    QueueName = aws_sqs_queue.dlq[each.key].name
  }
}

resource "aws_cloudwatch_metric_alarm" "handler_retries" {
  for_each            = local.telemetry_operations
  alarm_name          = "data-fetch-service-${each.key}-retry-outcomes"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "OperationCount"
  namespace           = "DataFetchService"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  treat_missing_data  = "notBreaching"
  dimensions = {
    component = each.key
    operation = each.value
    outcome   = "retry"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each            = local.telemetry_operations
  alarm_name          = "data-fetch-service-${each.key}-invocation-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = aws_lambda_function.handler[each.key].function_name
  }
}
