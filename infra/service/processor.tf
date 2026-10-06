data "terraform_remote_state" "processor" {
  backend = "s3"
  config = {
    bucket = var.processor_state_bucket
    key    = var.processor_state_key
    region = var.processor_state_region
  }
}

locals {
  processor = data.terraform_remote_state.processor.outputs
}

resource "terraform_data" "processor_contract" {
  input = local.processor.processor_submission_route_arn

  lifecycle {
    precondition {
      condition     = local.processor.processor_api_endpoint == "https://processing.kagent.app/v1/event-ingestions" && can(regex("^arn:aws:execute-api:${var.aws_region}:[0-9]{12}:[^/]+/[^/]+/POST/v1/event-ingestions$", local.processor.processor_submission_route_arn))
      error_message = "Processor state must expose its HTTPS submission endpoint and one POST execute-api route ARN."
    }
  }
}
