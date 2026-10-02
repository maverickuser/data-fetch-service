terraform {
  required_version = "~> 1.16.4"

  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.61.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Service     = "data-fetch-service"
      Environment = "prod"
      ManagedBy   = "terraform"
    }
  }
}
