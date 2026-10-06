output "ingress_queue_url" {
  description = "Service-owned ingress queue URL for approved producers."
  value       = aws_sqs_queue.work["ingress"].url
}

output "ingress_queue_arn" {
  description = "Service-owned ingress queue ARN for approved producers."
  value       = aws_sqs_queue.work["ingress"].arn
}

output "api_url" {
  value = "https://${aws_apigatewayv2_domain_name.fetch_api.domain_name}"
}

output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "state_bucket" {
  value = aws_s3_bucket.state.bucket
}

output "lambda_function_names" {
  value = { for name, fn in aws_lambda_function.handler : name => fn.function_name }
}

output "deployment_commit" {
  value = var.deployment_commit
}

output "config_revision" {
  value = var.config_revision
}
