# The processor runs in this account. Its API ID is assigned by AWS and may
# change when its API is recreated, so the submission grant wildcards the API
# ID and stage and pins only this account, region, method, and route.
locals {
  processor_api_endpoint         = "https://processing.kagent.app/v1/event-ingestions"
  processor_submission_route_arn = "arn:aws:execute-api:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*/*/POST/v1/event-ingestions"
}
