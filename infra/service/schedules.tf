locals {
  source_config = yamldecode(file("${path.module}/../../config/events.yaml"))
  prod_config   = yamldecode(file("${path.module}/../../config/environments/prod.yaml"))
  schedules = {
    for event in local.source_config.events : event.id => event
    if event.enabled && try(event.schedule.enabled, false)
  }
}

resource "terraform_data" "configuration_contract" {
  input = var.config_revision
  lifecycle {
    precondition {
      condition     = local.source_config.schema_version == 1 && local.prod_config.schema_version == 1 && local.prod_config.environment == "prod" && local.prod_config.resource_prefix == "data-fetch-service" && local.prod_config.aws_region == var.aws_region
      error_message = "Terraform and bundled production configuration must select the same region and resource prefix."
    }
    precondition {
      condition     = try(local.prod_config.processor.url, local.source_config.processor.url) == local.processor.processor_api_endpoint
      error_message = "Bundled processor.url must equal the processor state's submission endpoint."
    }
    precondition {
      condition     = length(local.schedules) == 1 && contains(keys(local.schedules), "daily-bhavcopy") && local.schedules["daily-bhavcopy"].schedule.expression == "cron(0 20 ? * MON-FRI *)" && local.schedules["daily-bhavcopy"].schedule.timezone == "Asia/Kolkata" && local.schedules["daily-bhavcopy"].schedule.inputs.exchangeName == "BSE"
      error_message = "Enabled source schedule must be the configured 20:00 IST weekday BSE event with exchangeName BSE."
    }
  }
}

resource "aws_scheduler_schedule" "source" {
  for_each                     = local.schedules
  name                         = "data-fetch-service-${each.key}"
  schedule_expression          = each.value.schedule.expression
  schedule_expression_timezone = each.value.schedule.timezone
  state                        = var.enable_bse_schedule ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sqs_queue.work["ingress"].arn
    role_arn = aws_iam_role.scheduler.arn
    input = jsonencode({
      specversion     = "1.0"
      id              = "<aws.scheduler.scheduled-time>"
      source          = "<aws.scheduler.schedule-arn>"
      type            = "com.bondplatform.data.pull.requested.v1"
      time            = "<aws.scheduler.scheduled-time>"
      datacontenttype = "application/json"
      data = {
        schema_version = 1
        event_type     = each.key
        inputs         = each.value.schedule.inputs
        schedule_arn   = "<aws.scheduler.schedule-arn>"
        scheduled_time = "<aws.scheduler.scheduled-time>"
      }
    })
  }

  depends_on = [aws_iam_role_policy.scheduler, terraform_data.configuration_contract]
}

resource "aws_scheduler_schedule" "reconciler" {
  name                = "data-fetch-service-reconciler"
  schedule_expression = "rate(5 minutes)"
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.handler["reconciler"].arn
    role_arn = aws_iam_role.scheduler.arn
    input    = "{}"
  }

  depends_on = [aws_iam_role_policy.scheduler]
}

resource "aws_lambda_permission" "scheduler_reconciler" {
  statement_id  = "AllowSchedulerInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.handler["reconciler"].function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.reconciler.arn
}
