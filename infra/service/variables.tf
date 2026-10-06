variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "hosted_zone_id" {
  type        = string
  description = "Existing public Route 53 hosted zone ID for kagent.app."
  validation {
    condition     = can(regex("^[A-Z0-9]+$", var.hosted_zone_id))
    error_message = "hosted_zone_id must be a Route 53 hosted-zone ID."
  }
}

variable "processor_state_bucket" {
  type        = string
  description = "Bucket containing the processor service Terraform state."
}

variable "processor_state_key" {
  type        = string
  description = "Processor state key with its API endpoint and submission route ARN."
}

variable "processor_state_region" {
  type    = string
  default = "ap-south-1"
}

variable "deployment_commit" {
  type        = string
  description = "Exact source commit bundled in every Lambda package."
  validation {
    condition     = can(regex("^[0-9a-f]{40}$", var.deployment_commit))
    error_message = "deployment_commit must be a full Git SHA."
  }
}

variable "config_revision" {
  type        = string
  description = "Config revision reported by cmd/configcheck for the packaged release."
  validation {
    condition     = can(regex("^sha256:[0-9a-f]{64}$", var.config_revision))
    error_message = "config_revision must be a sha256 revision."
  }
}

variable "package_bucket" {
  type        = string
  description = "Existing immutable release-package bucket."
}

variable "package_sha256" {
  type        = map(string)
  description = "Base64 SHA-256 of each Lambda ZIP, keyed by api/admission/pull/delivery/reconciler."
  validation {
    condition     = length(keys(var.package_sha256)) == 5 && length(setsubtract(toset(keys(var.package_sha256)), toset(["api", "admission", "pull", "delivery", "reconciler"]))) == 0 && alltrue([for value in values(var.package_sha256) : can(regex("^[A-Za-z0-9+/]{43}=$", value))])
    error_message = "package_sha256 must provide five base64 SHA-256 values."
  }
}

variable "enable_ingress_consumption" {
  type        = bool
  default     = false
  description = "Activate the external ingress SQS event-source mapping after release preflight."
}

variable "enable_bse_schedule" {
  type        = bool
  default     = false
  description = "Activate the configured weekday BSE schedule after release preflight."
}

variable "external_producer_role_arns" {
  type        = set(string)
  default     = []
  description = "Approved external IAM roles allowed to send to this service's ingress queue."
}

variable "native_eventbridge_rule_arns" {
  type        = set(string)
  default     = []
  description = "Approved EventBridge rule ARNs allowed to send native events to ingress."
}

variable "processor_reader_role_arns" {
  type        = set(string)
  default     = []
  description = "Processor IAM roles allowed to read manifests and files under runs/ in the artifact bucket."
  validation {
    condition     = alltrue([for arn in var.processor_reader_role_arns : can(regex("^arn:aws:iam::[0-9]{12}:role/.+$", arn))])
    error_message = "processor_reader_role_arns must contain IAM role ARNs."
  }
}
