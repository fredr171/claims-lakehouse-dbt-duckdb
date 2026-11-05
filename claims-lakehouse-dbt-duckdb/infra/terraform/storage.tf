# Data buckets: versioning + lifecycle (immutable bronze safety net).
#
# The pipeline itself never overwrites or deletes a delivery: raw and the landing
# archive/quarantine are keyed by checksum (one key per delivered file), and dbt staging
# picks the current delivery of each file (stg_<feed>__deliveries). Versioning covers
# what the pipeline cannot: a mistaken delete or overwrite by a person or a bug becomes a
# noncurrent version that can be restored for noncurrent_version_retention_days.
#
# The buckets were created outside Terraform (data sources in main.tf); these resources
# only own their versioning and lifecycle configuration. Each lifecycle resource owns the
# bucket's ENTIRE lifecycle configuration, so every rule for a bucket lives here.
# Not enabled: Object Lock (WORM). It would make deliveries undeletable even by an admin,
# the step a regulated org takes on top of this.

locals {
  versioned_buckets = {
    landing = data.aws_s3_bucket.landing.id
    raw     = data.aws_s3_bucket.raw.id
  }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = local.versioned_buckets
  bucket   = each.value

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  for_each = local.versioned_buckets
  bucket   = each.value

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {} # whole bucket

    # Overwritten/deleted versions (mostly the transform state, rewritten every run).
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_retention_days
    }
    expiration {
      expired_object_delete_marker = true
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.data]
}
