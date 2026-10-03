mock_provider "aws" {}

override_data {
  target = data.aws_iam_policy_document.tls
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

run "package_bucket" {
  command = apply

  assert {
    condition     = output.package_bucket == "data-fetch-service-packages" && !aws_s3_bucket.packages.force_destroy
    error_message = "The package bucket keeps its agreed name and is never force-destroyed."
  }
  assert {
    condition     = aws_s3_bucket_versioning.packages.versioning_configuration[0].status == "Enabled" && aws_s3_bucket_public_access_block.packages.restrict_public_buckets && aws_s3_bucket_public_access_block.packages.block_public_policy
    error_message = "Release packages must be versioned and private."
  }
}
