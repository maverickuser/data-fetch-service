locals {
  lambda_settings = {
    api        = { memory = 256, timeout = 30, storage = 512 }
    admission  = { memory = 256, timeout = 30, storage = 512 }
    pull       = { memory = 2048, timeout = 900, storage = 4096 }
    delivery   = { memory = 256, timeout = 180, storage = 512 }
    reconciler = { memory = 256, timeout = 60, storage = 512 }
  }
  release_prefix = "releases/${var.deployment_commit}/${replace(var.config_revision, ":", "-")}"
}

resource "aws_cloudwatch_log_group" "lambda" {
  for_each          = local.lambda_settings
  name              = "/aws/lambda/data-fetch-service-${each.key}"
  retention_in_days = 30
}

resource "aws_lambda_function" "handler" {
  for_each                       = local.lambda_settings
  function_name                  = "data-fetch-service-${each.key}"
  role                           = aws_iam_role.lambda[each.key].arn
  package_type                   = "Zip"
  runtime                        = "provided.al2023"
  handler                        = "bootstrap"
  architectures                  = ["arm64"]
  memory_size                    = each.value.memory
  timeout                        = each.value.timeout
  s3_bucket                      = var.package_bucket
  s3_key                         = "${local.release_prefix}/${each.key}.zip"
  source_code_hash               = var.package_sha256[each.key]
  reserved_concurrent_executions = -1

  ephemeral_storage {
    size = each.value.storage
  }

  environment {
    variables = {
      DEPLOYMENT_COMMIT  = var.deployment_commit
      STATE_BUCKET       = aws_s3_bucket.state.bucket
      ARTIFACT_BUCKET    = aws_s3_bucket.artifacts.bucket
      PULL_QUEUE_URL     = aws_sqs_queue.work["pull"].url
      DELIVERY_QUEUE_URL = aws_sqs_queue.work["delivery"].url
    }
  }

  depends_on = [terraform_data.processor_contract, aws_iam_role_policy.lambda, aws_cloudwatch_log_group.lambda]
}

resource "aws_lambda_event_source_mapping" "sqs" {
  for_each                = local.queue_consumers
  function_name           = aws_lambda_function.handler[each.key].arn
  event_source_arn        = aws_sqs_queue.work[each.value].arn
  batch_size              = 1
  enabled                 = each.key == "admission" ? var.enable_ingress_consumption : true
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = each.key == "pull" ? 2 : each.key == "delivery" ? 5 : 10
  }
}
