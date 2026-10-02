locals {
  buckets = {
    artifacts = "data-fetch-service-artifacts"
    state     = "data-fetch-service-state"
  }
  expiring_state_prefixes = ["runs", "requests", "listings"]
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = local.buckets.artifacts
  force_destroy = false
}

resource "aws_s3_bucket" "state" {
  bucket        = local.buckets.state
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "private" {
  for_each                = local.buckets
  bucket                  = each.key == "state" ? aws_s3_bucket.state.id : aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "private" {
  for_each = local.buckets
  bucket   = each.key == "state" ? aws_s3_bucket.state.id : aws_s3_bucket.artifacts.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "private" {
  for_each = local.buckets
  bucket   = each.key == "state" ? aws_s3_bucket.state.id : aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "private" {
  for_each = local.buckets
  bucket   = each.key == "state" ? aws_s3_bucket.state.id : aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "expire-run-artifacts"
    status = "Enabled"
    filter {
      prefix = "runs/"
    }
    expiration {
      days = 30
    }
  }
  rule {
    id     = "remove-abandoned-uploads-and-old-versions"
    status = "Enabled"
    filter {}
    expiration {
      expired_object_delete_marker = true
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# coordination/ and acceptance/ hold the long-lived accepted fingerprint and are never expired here.
resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  dynamic "rule" {
    for_each = local.expiring_state_prefixes
    content {
      id     = "expire-${rule.value}"
      status = "Enabled"
      filter {
        prefix = "${rule.value}/"
      }
      expiration {
        days = 30
      }
    }
  }
  rule {
    id     = "remove-abandoned-uploads-and-old-versions"
    status = "Enabled"
    filter {}
    expiration {
      expired_object_delete_marker = true
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

data "aws_iam_policy_document" "bucket_tls" {
  for_each = local.buckets
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      each.key == "state" ? aws_s3_bucket.state.arn : aws_s3_bucket.artifacts.arn,
      "${each.key == "state" ? aws_s3_bucket.state.arn : aws_s3_bucket.artifacts.arn}/*"
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
  dynamic "statement" {
    for_each = each.key == "artifacts" && length(var.processor_reader_role_arns) > 0 ? [1] : []
    content {
      sid       = "ProcessorReadsRunArtifacts"
      effect    = "Allow"
      actions   = ["s3:GetObject"]
      resources = ["${aws_s3_bucket.artifacts.arn}/runs/*"]
      principals {
        type        = "AWS"
        identifiers = var.processor_reader_role_arns
      }
    }
  }
}

resource "aws_s3_bucket_policy" "private_tls" {
  for_each = local.buckets
  bucket   = each.key == "state" ? aws_s3_bucket.state.id : aws_s3_bucket.artifacts.id
  policy   = data.aws_iam_policy_document.bucket_tls[each.key].json
}
