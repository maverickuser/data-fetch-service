locals {
  api_routes = toset([
    "POST /v1/events/{event_type}/runs",
    "POST /v1/runs/{run_id}/reruns",
    "POST /v1/runs/{run_id}/delivery-retries",
    "GET /v1/events/{event_type}",
    "GET /v1/events/{event_type}/active",
    "GET /v1/events/{event_type}/runs",
    "GET /v1/runs/{run_id}",
    "GET /v1/runs/{run_id}/history",
    "GET /v1/runs/{run_id}/pulls",
    "GET /v1/runs/{run_id}/delivery",
    "GET /v1/requests/{producer}/{event_id}"
  ])
}

resource "aws_apigatewayv2_api" "http" {
  name          = "data-fetch-service"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.handler["api"].invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 29000
}

resource "aws_apigatewayv2_route" "api" {
  for_each  = local.api_routes
  api_id    = aws_apigatewayv2_api.http.id
  route_key = each.value
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_cloudwatch_log_group" "api_access" {
  name              = "/aws/apigateway/data-fetch-service"
  retention_in_days = 30
}

resource "aws_apigatewayv2_stage" "prod" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_burst_limit = 20
    throttling_rate_limit  = 10
  }

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      requestTime    = "$context.requestTime"
      routeKey       = "$context.routeKey"
      status         = "$context.status"
      responseLength = "$context.responseLength"
      integrationErr = "$context.integrationErrorMessage"
    })
  }

  depends_on = [aws_apigatewayv2_route.api]
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.handler["api"].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http.execution_arn}/*/*"
}

data "aws_route53_zone" "shared" {
  zone_id      = var.hosted_zone_id
  private_zone = false
}

resource "terraform_data" "domain_contract" {
  input = var.hosted_zone_id
  lifecycle {
    precondition {
      condition     = data.aws_route53_zone.shared.name == "kagent.app."
      error_message = "hosted_zone_id must identify the existing public kagent.app zone."
    }
  }
}

resource "aws_acm_certificate" "fetch_api" {
  domain_name       = "fetch.kagent.app"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "certificate_validation" {
  zone_id         = var.hosted_zone_id
  name            = one(aws_acm_certificate.fetch_api.domain_validation_options).resource_record_name
  type            = one(aws_acm_certificate.fetch_api.domain_validation_options).resource_record_type
  records         = [one(aws_acm_certificate.fetch_api.domain_validation_options).resource_record_value]
  ttl             = 60
  allow_overwrite = true
  depends_on      = [terraform_data.domain_contract]
}

resource "aws_acm_certificate_validation" "fetch_api" {
  certificate_arn         = aws_acm_certificate.fetch_api.arn
  validation_record_fqdns = [aws_route53_record.certificate_validation.fqdn]
}

resource "aws_apigatewayv2_domain_name" "fetch_api" {
  domain_name = "fetch.kagent.app"
  domain_name_configuration {
    certificate_arn = aws_acm_certificate_validation.fetch_api.certificate_arn
    endpoint_type   = "REGIONAL"
    security_policy = "TLS_1_2"
  }
}

resource "aws_apigatewayv2_api_mapping" "fetch_api" {
  api_id      = aws_apigatewayv2_api.http.id
  domain_name = aws_apigatewayv2_domain_name.fetch_api.id
  stage       = aws_apigatewayv2_stage.prod.id
}

resource "aws_route53_record" "fetch_alias" {
  zone_id = var.hosted_zone_id
  name    = "fetch.kagent.app"
  type    = "A"
  alias {
    name                   = aws_apigatewayv2_domain_name.fetch_api.domain_name_configuration[0].target_domain_name
    zone_id                = aws_apigatewayv2_domain_name.fetch_api.domain_name_configuration[0].hosted_zone_id
    evaluate_target_health = false
  }
  depends_on = [terraform_data.domain_contract]
}
