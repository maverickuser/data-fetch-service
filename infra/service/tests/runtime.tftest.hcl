mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/mock" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:ap-south-1:123456789012:log-group:mock" }
  }
  mock_resource "aws_apigatewayv2_api" {
    defaults = { execution_arn = "arn:aws:execute-api:ap-south-1:123456789012:mock" }
  }
  mock_resource "aws_sqs_queue" {
    defaults = { arn = "arn:aws:sqs:ap-south-1:123456789012:mock" }
  }
  mock_resource "aws_lambda_function" {
    defaults = {
      arn        = "arn:aws:lambda:ap-south-1:123456789012:function:mock"
      invoke_arn = "arn:aws:apigateway:ap-south-1:lambda:path/2015-03-31/functions/arn:aws:lambda:ap-south-1:123456789012:function:mock/invocations"
    }
  }
  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:ap-south-1:123456789012:certificate/00000000-0000-0000-0000-000000000000"
      domain_validation_options = [
        {
          domain_name           = "fetch.kagent.app"
          resource_record_name  = "_validation.fetch.kagent.app"
          resource_record_type  = "CNAME"
          resource_record_value = "_validation.acm-validations.aws"
        }
      ]
    }
  }
  mock_resource "aws_scheduler_schedule" {
    defaults = { arn = "arn:aws:scheduler:ap-south-1:123456789012:schedule/default/mock" }
  }
}

override_data {
  target = data.aws_iam_policy_document.lambda_assume
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.scheduler_assume
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.bucket_tls["artifacts"]
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.bucket_tls["state"]
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

variables {
  aws_region             = "ap-south-1"
  processor_state_bucket = "test-processor-state"
  processor_state_key    = "processor.tfstate"
  hosted_zone_id         = "Z1234567890"
  deployment_commit      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  config_revision        = "sha256:44e1c1fcf478af773ef390eb4dd6b6f9f4bfbf048653079fb24ce0c90ea9d673"
  package_bucket         = "test-release-packages"
  package_sha256 = {
    api        = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    admission  = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    pull       = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    delivery   = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    reconciler = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
  }
}

override_data {
  target = data.terraform_remote_state.network
  values = {
    outputs = {
      vpc_id                         = "vpc-test"
      private_subnet_ids_by_az       = { ap-south-1a = "subnet-a", ap-south-1b = "subnet-b" }
      private_route_table_ids_by_az  = { ap-south-1a = "rtb-a", ap-south-1b = "rtb-b" }
      nat_gateway_ids_by_az          = { ap-south-1a = "nat-a", ap-south-1b = "nat-b" }
      s3_endpoint_id                 = "vpce-s3"
      sqs_endpoint_id                = "vpce-sqs"
      logs_endpoint_id               = "vpce-logs"
      fetch_lambda_security_group_id = "sg-fetch"
    }
  }
}

override_data {
  target = data.terraform_remote_state.processor
  values = {
    outputs = {
      vpc_id                         = "vpc-test"
      processor_api_endpoint         = "https://processing.kagent.app/v1/event-ingestions"
      processor_submission_route_arn = "arn:aws:execute-api:ap-south-1:123456789012:processor/prod/POST/v1/event-ingestions"
    }
  }
}

override_data {
  target = data.aws_subnet.private["ap-south-1a"]
  values = { vpc_id = "vpc-test", availability_zone = "ap-south-1a", map_public_ip_on_launch = false }
}

override_data {
  target = data.aws_subnet.private["ap-south-1b"]
  values = { vpc_id = "vpc-test", availability_zone = "ap-south-1b", map_public_ip_on_launch = false }
}

override_data {
  target = data.aws_route_table.private["ap-south-1a"]
  values = { vpc_id = "vpc-test", routes = [{ cidr_block = "0.0.0.0/0", nat_gateway_id = "nat-a" }] }
}

override_data {
  target = data.aws_route_table.private["ap-south-1b"]
  values = { vpc_id = "vpc-test", routes = [{ cidr_block = "0.0.0.0/0", nat_gateway_id = "nat-b" }] }
}

override_data {
  target = data.aws_vpc_endpoint.s3
  values = { vpc_id = "vpc-test", route_table_ids = ["rtb-a", "rtb-b"], policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":\"*\",\"Condition\":{\"StringEquals\":{\"aws:ResourceAccount\":\"123456789012\"}}}]}" }
}

override_data {
  target = data.aws_vpc_endpoint.sqs
  values = { vpc_id = "vpc-test", private_dns_enabled = true }
}

override_data {
  target = data.aws_vpc_endpoint.logs
  values = { vpc_id = "vpc-test", private_dns_enabled = true }
}

override_data {
  target = data.aws_security_group.lambda
  values = { vpc_id = "vpc-test" }
}

override_data {
  target = data.aws_route53_zone.shared
  values = { name = "kagent.app.", private_zone = false }
}

override_resource {
  target = aws_s3_bucket.artifacts
  values = { arn = "arn:aws:s3:::data-fetch-service-artifacts" }
}

override_resource {
  target = aws_s3_bucket.state
  values = { arn = "arn:aws:s3:::data-fetch-service-state" }
}

override_resource {
  target = aws_sqs_queue.work["ingress"]
  values = { arn = "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-ingress" }
}

override_resource {
  target = aws_sqs_queue.work["pull"]
  values = { arn = "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-pull" }
}

override_resource {
  target = aws_sqs_queue.work["delivery"]
  values = { arn = "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-delivery" }
}

run "production_shape" {
  command = apply

  assert {
    condition     = aws_sqs_queue.work["ingress"].visibility_timeout_seconds == 180 && aws_sqs_queue.work["pull"].visibility_timeout_seconds == 5400 && aws_sqs_queue.work["delivery"].visibility_timeout_seconds == 1080
    error_message = "Queue visibility must be six times its Lambda timeout."
  }
  assert {
    condition     = alltrue([for name, queue in aws_sqs_queue.work : queue.message_retention_seconds == 345600 && jsondecode(queue.redrive_policy).maxReceiveCount == 5 && queue.name == "data-fetch-service-${name}"])
    error_message = "Service queues need four-day retention, five receives and fixed names."
  }
  assert {
    condition     = alltrue([for name, queue in aws_sqs_queue.dlq : queue.message_retention_seconds == 1209600 && queue.name == "data-fetch-service-${name}-dlq"])
    error_message = "DLQs need fourteen-day retention and fixed names."
  }
  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.artifacts.rule[0].filter[0].prefix == "runs/" && aws_s3_bucket_lifecycle_configuration.artifacts.rule[0].expiration[0].days == 30
    error_message = "Run artifacts must expire after thirty days."
  }
  assert {
    condition     = toset([for rule in aws_s3_bucket_lifecycle_configuration.state.rule : rule.filter[0].prefix if length(rule.expiration) > 0 && try(rule.expiration[0].days, 0) == 30]) == toset(["runs/", "requests/", "listings/"])
    error_message = "Only run state, request receipts and listings expire after thirty days; coordination/ and acceptance/ are retained."
  }
  assert {
    condition     = alltrue([for config in [aws_s3_bucket_lifecycle_configuration.artifacts, aws_s3_bucket_lifecycle_configuration.state] : anytrue([for rule in config.rule : length(rule.abort_incomplete_multipart_upload) > 0 && try(rule.abort_incomplete_multipart_upload[0].days_after_initiation, 0) == 1])])
    error_message = "Abandoned multipart uploads must be removed after one day."
  }
  assert {
    condition     = alltrue([for fn in aws_lambda_function.handler : !contains(keys(fn.environment[0].variables), "AWS_REGION")])
    error_message = "AWS_REGION is reserved by Lambda and must not be set."
  }
  assert {
    condition     = alltrue([for kind in ["api", "admission", "pull", "delivery", "reconciler"] : contains([for statement in jsondecode(aws_iam_role_policy.lambda[kind].policy).Statement : statement.Resource if statement.Sid == "StateObjectReadWrite"][0], "arn:aws:s3:::data-fetch-service-state/acceptance/*")])
    error_message = "Every role needs the acceptance/ state prefix."
  }
  assert {
    condition     = toset([for kind in ["api", "admission", "pull", "delivery", "reconciler"] : kind if contains(jsondecode(aws_iam_role_policy.lambda[kind].policy).Statement[*].Sid, "SubmitToProcessorAPI")]) == toset(["delivery", "reconciler"]) && [for statement in jsondecode(aws_iam_role_policy.lambda["delivery"].policy).Statement : statement.Resource if statement.Sid == "SubmitToProcessorAPI"][0] == "arn:aws:execute-api:ap-south-1:123456789012:processor/prod/POST/v1/event-ingestions"
    error_message = "Only Delivery and the Reconciler's delivery resume may invoke the single processor submission route."
  }
  assert {
    condition     = toset([for kind in ["api", "admission", "pull", "delivery", "reconciler"] : kind if contains(jsondecode(aws_iam_role_policy.lambda[kind].policy).Statement[*].Sid, "WriteRunArtifacts")]) == toset(["api", "pull"]) && toset([for kind in ["api", "admission", "pull", "delivery", "reconciler"] : kind if contains(jsondecode(aws_iam_role_policy.lambda[kind].policy).Statement[*].Sid, "ReadRunArtifacts")]) == toset(["api", "pull", "delivery", "reconciler"])
    error_message = "Artifact access must match each handler's use; Admission has none."
  }
  assert {
    condition = { for kind in ["api", "admission", "pull", "delivery", "reconciler"] : kind => toset(flatten([for statement in jsondecode(aws_iam_role_policy.lambda[kind].policy).Statement : statement.Resource if statement.Sid == "PublishInternalWork"])) } == {
      api        = toset(["arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-pull", "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-delivery"])
      admission  = toset(["arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-pull", "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-delivery"])
      pull       = toset(["arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-delivery"])
      delivery   = toset([])
      reconciler = toset(["arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-pull", "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-delivery"])
    }
    error_message = "Each role may publish only to the internal queues its dispatch repair can target."
  }
  assert {
    condition = { for kind in ["admission", "pull", "delivery"] : kind => one([for statement in jsondecode(aws_iam_role_policy.lambda[kind].policy).Statement : statement.Resource if statement.Sid == "ConsumeOwnQueue"]) } == {
      admission = "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-ingress"
      pull      = "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-pull"
      delivery  = "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-delivery"
    }
    error_message = "Each consumer may read only its own queue."
  }
  assert {
    condition     = alltrue([for kind in ["api", "admission", "pull", "delivery", "reconciler"] : alltrue([for statement in jsondecode(aws_iam_role_policy.lambda[kind].policy).Statement : !can(statement.Condition) if statement.Action == "s3:ListBucket"])])
    error_message = "ListBucket must be unconditioned so missing keys return 404 rather than 403."
  }
  assert {
    condition     = output.ingress_queue_arn == "arn:aws:sqs:ap-south-1:123456789012:data-fetch-service-ingress" && output.ingress_queue_arn == aws_sqs_queue.work["ingress"].arn && output.api_url == "https://fetch.kagent.app"
    error_message = "Producer and API outputs must keep their agreed names and values."
  }
  assert {
    condition     = aws_lambda_function.handler["pull"].memory_size == 2048 && aws_lambda_function.handler["pull"].timeout == 900 && aws_lambda_function.handler["pull"].ephemeral_storage[0].size == 4096 && length(aws_lambda_function.handler) == 5
    error_message = "Pull needs its configured memory, disk and timeout; five Lambdas must exist."
  }
  assert {
    condition     = !aws_lambda_event_source_mapping.sqs["admission"].enabled && aws_lambda_event_source_mapping.sqs["pull"].enabled && aws_scheduler_schedule.source["daily-bhavcopy"].state == "DISABLED"
    error_message = "Ingress and BSE schedule need safe staged defaults."
  }
  assert {
    condition     = aws_scheduler_schedule.source["daily-bhavcopy"].schedule_expression == "cron(0 20 ? * MON-FRI *)" && aws_scheduler_schedule.source["daily-bhavcopy"].schedule_expression_timezone == "Asia/Kolkata" && jsondecode(aws_scheduler_schedule.source["daily-bhavcopy"].target[0].input).data.inputs.exchangeName == "BSE"
    error_message = "BSE schedule must match compiled configuration."
  }
  assert {
    condition     = jsondecode(aws_scheduler_schedule.source["daily-bhavcopy"].target[0].input).id == "<aws.scheduler.scheduled-time>" && jsondecode(aws_scheduler_schedule.source["daily-bhavcopy"].target[0].input).source == "<aws.scheduler.schedule-arn>"
    error_message = "Scheduled identity must use logical time and schedule ARN."
  }
  assert {
    condition     = contains(jsondecode(aws_iam_role_policy.lambda["api"].policy).Statement[*].Sid, "ListRunArtifactsForMissingObjectResolution") && contains(jsondecode(aws_iam_role_policy.lambda["pull"].policy).Statement[*].Sid, "WriteRunArtifacts")
    error_message = "API and Pull need scoped artifact read/list and Pull needs write."
  }
  assert {
    condition     = output.shared_vpc_id == "vpc-test" && output.deployment_commit == var.deployment_commit
    error_message = "Outputs must identify the shared VPC and exact release."
  }
}

run "rejects_processor_in_other_vpc" {
  command = plan

  override_data {
    target = data.terraform_remote_state.processor
    values = {
      outputs = {
        vpc_id                         = "vpc-other"
        processor_api_endpoint         = "https://processing.kagent.app/v1/event-ingestions"
        processor_submission_route_arn = "arn:aws:execute-api:ap-south-1:123456789012:processor/prod/POST/v1/event-ingestions"
      }
    }
  }

  expect_failures = [terraform_data.network_contract]
}

run "rejects_processor_endpoint_not_in_bundled_config" {
  command = plan

  override_data {
    target = data.terraform_remote_state.processor
    values = {
      outputs = {
        vpc_id                         = "vpc-test"
        processor_api_endpoint         = "https://other.kagent.app/v1/event-ingestions"
        processor_submission_route_arn = "arn:aws:execute-api:ap-south-1:123456789012:processor/prod/POST/v1/event-ingestions"
      }
    }
  }

  expect_failures = [terraform_data.network_contract, terraform_data.configuration_contract]
}

run "rejects_endpoint_policy_for_another_account" {
  command = plan

  override_data {
    target = data.aws_vpc_endpoint.s3
    values = { vpc_id = "vpc-test", route_table_ids = ["rtb-a", "rtb-b"], policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":\"*\",\"Condition\":{\"StringEquals\":{\"aws:ResourceAccount\":\"999999999999\"}}}]}" }
  }

  expect_failures = [terraform_data.network_contract]
}

run "accepts_endpoint_policy_naming_both_buckets" {
  command = plan

  override_data {
    target = data.aws_vpc_endpoint.s3
    values = { vpc_id = "vpc-test", route_table_ids = ["rtb-a", "rtb-b"], policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::data-fetch-service-artifacts/*\",\"arn:aws:s3:::data-fetch-service-state/*\"]}]}" }
  }
}
