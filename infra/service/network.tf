data "terraform_remote_state" "network" {
  backend = "s3"
  config = {
    bucket = var.network_state_bucket
    key    = var.network_state_key
    region = var.network_state_region
  }
}

data "terraform_remote_state" "processor" {
  backend = "s3"
  config = {
    bucket = var.processor_state_bucket
    key    = var.processor_state_key
    region = var.processor_state_region
  }
}

locals {
  network              = data.terraform_remote_state.network.outputs
  processor            = data.terraform_remote_state.processor.outputs
  private_subnets      = local.network.private_subnet_ids_by_az
  private_route_tables = local.network.private_route_table_ids_by_az
  nat_gateways         = local.network.nat_gateway_ids_by_az
}

data "aws_subnet" "private" {
  for_each = local.private_subnets
  id       = each.value
}

data "aws_route_table" "private" {
  for_each       = local.private_route_tables
  route_table_id = each.value
}

data "aws_vpc_endpoint" "s3" {
  id = local.network.s3_endpoint_id
}

data "aws_vpc_endpoint" "sqs" {
  id = local.network.sqs_endpoint_id
}

data "aws_vpc_endpoint" "logs" {
  id = local.network.logs_endpoint_id
}

data "aws_security_group" "lambda" {
  id = local.network.fetch_lambda_security_group_id
}

resource "terraform_data" "network_contract" {
  input = local.network.vpc_id

  lifecycle {
    precondition {
      condition     = local.network.vpc_id == local.processor.vpc_id
      error_message = "Fetch and processor Lambdas must use the same shared VPC."
    }
    precondition {
      condition     = local.processor.processor_api_endpoint == "https://processing.kagent.app/v1/event-ingestions" && can(regex("^arn:aws:execute-api:${var.aws_region}:[0-9]{12}:[^/]+/[^/]+/POST/v1/event-ingestions$", local.processor.processor_submission_route_arn))
      error_message = "Processor state must expose its HTTPS submission endpoint and one POST execute-api route ARN."
    }
    precondition {
      condition     = length(local.private_subnets) >= 2 && length(setsubtract(toset(keys(local.private_subnets)), toset(keys(local.private_route_tables)))) == 0 && length(setsubtract(toset(keys(local.private_route_tables)), toset(keys(local.private_subnets)))) == 0 && length(setsubtract(toset(keys(local.private_subnets)), toset(keys(local.nat_gateways)))) == 0
      error_message = "At least two private subnets need matching per-AZ route tables and NAT gateways."
    }
    precondition {
      condition     = alltrue([for az, subnet in data.aws_subnet.private : subnet.vpc_id == local.network.vpc_id && subnet.availability_zone == az && !subnet.map_public_ip_on_launch])
      error_message = "Every Lambda subnet must be private, in the shared VPC, and in its declared AZ."
    }
    precondition {
      condition     = alltrue([for az, table in data.aws_route_table.private : table.vpc_id == local.network.vpc_id && anytrue([for route in table.routes : route.cidr_block == "0.0.0.0/0" && route.nat_gateway_id == local.nat_gateways[az]])])
      error_message = "Every private subnet route table needs its declared NAT default route."
    }
    precondition {
      condition     = data.aws_security_group.lambda.vpc_id == local.network.vpc_id
      error_message = "Fetch Lambda security group must belong to the shared VPC."
    }
    precondition {
      condition     = alltrue([for endpoint in [data.aws_vpc_endpoint.s3, data.aws_vpc_endpoint.sqs, data.aws_vpc_endpoint.logs] : endpoint.vpc_id == local.network.vpc_id]) && data.aws_vpc_endpoint.sqs.private_dns_enabled && data.aws_vpc_endpoint.logs.private_dns_enabled
      error_message = "S3, SQS and CloudWatch Logs endpoints must belong to the shared VPC; interface endpoints need private DNS."
    }
    precondition {
      condition     = contains(data.aws_vpc_endpoint.s3.route_table_ids, values(local.private_route_tables)[0]) && alltrue([for table_id in values(local.private_route_tables) : contains(data.aws_vpc_endpoint.s3.route_table_ids, table_id)])
      error_message = "S3 gateway endpoint must be associated with every private route table."
    }
    precondition {
      condition     = alltrue([for bucket_arn in [aws_s3_bucket.artifacts.arn, aws_s3_bucket.state.arn] : strcontains(data.aws_vpc_endpoint.s3.policy, bucket_arn)])
      error_message = "Shared S3 endpoint policy must allow both service buckets."
    }
  }
}
