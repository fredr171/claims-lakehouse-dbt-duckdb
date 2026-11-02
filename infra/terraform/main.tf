# Ingestion Lambda: S3 landing (.zip) -> validate -> raw (Parquet).
# The buckets already exist (created by hand with Block Public Access and
# SSE-S3); they are only referenced here, never modified, except for the
# landing bucket's event notification.

data "aws_caller_identity" "current" {}

data "aws_s3_bucket" "landing" {
  bucket = var.landing_bucket
}

data "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket
}

locals {
  landing_arn = data.aws_s3_bucket.landing.arn
  raw_arn     = data.aws_s3_bucket.raw.arn
}

# --- Logs --------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days
}

# --- IAM: least privilege, scoped to the prefixes the pipeline touches --------

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingest" {
  name               = "${var.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
}

data "aws_iam_policy_document" "ingest" {
  # ListBucket makes a missing object return 404 instead of 403. The pipeline
  # relies on that for "no manifest yet" and for verifying copies.
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket"]
    resources = [local.landing_arn, local.raw_arn]
  }

  statement {
    sid       = "ReadAndRemoveIncomingZips"
    actions   = ["s3:GetObject", "s3:GetObjectTagging", "s3:DeleteObject"]
    resources = ["${local.landing_arn}/${var.landing_prefix}*"]
  }

  statement {
    sid     = "WriteArchiveAndQuarantine"
    actions = ["s3:GetObject", "s3:PutObject", "s3:PutObjectTagging"]
    resources = [
      "${local.landing_arn}/archive/*",
      "${local.landing_arn}/quarantine/*",
    ]
  }

  statement {
    sid     = "WriteRawParquetAndManifests"
    actions = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = [
      "${local.raw_arn}/${var.landing_prefix}*",
      "${local.raw_arn}/_manifests/*",
    ]
  }

  statement {
    sid       = "WriteOwnLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.ingest.arn}:*"]
  }
}

resource "aws_iam_role_policy" "ingest" {
  name   = "${var.function_name}-s3-logs"
  role   = aws_iam_role.ingest.id
  policy = data.aws_iam_policy_document.ingest.json
}

# --- Function ------------------------------------------------------------------

resource "aws_lambda_function" "ingest" {
  function_name    = var.function_name
  description      = "DE-SynPUF landing zip -> validated raw Parquet (claims-etl)"
  role             = aws_iam_role.ingest.arn
  runtime          = "python3.13"
  architectures    = ["x86_64"]
  handler          = "claims_etl.ingestion.lambda_handler.handler"
  filename         = var.lambda_zip_path
  source_code_hash = filebase64sha256(var.lambda_zip_path)
  layers           = [var.pandas_layer_arn]
  memory_size      = var.memory_mb
  timeout          = var.timeout_seconds

  ephemeral_storage {
    size = var.ephemeral_storage_mb
  }

  environment {
    variables = {
      CLAIMS_ETL_CONFIG_DIR = "/var/task/config" # config/ is bundled in the zip
    }
  }

  depends_on = [aws_cloudwatch_log_group.ingest, aws_iam_role_policy.ingest]
}

# S3 invokes asynchronously. On `error` the handler raises, so Lambda retries
# (the zip is still in landing). `quarantined` returns normally: bad data is final.
resource "aws_lambda_function_event_invoke_config" "ingest" {
  function_name                = aws_lambda_function.ingest.function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = 3600
}

# --- Trigger: landing bucket -> Lambda ------------------------------------------------

resource "aws_lambda_permission" "allow_landing_bucket" {
  statement_id   = "AllowInvokeFromLandingBucket"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.ingest.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = local.landing_arn
  source_account = data.aws_caller_identity.current.account_id # confused-deputy guard
}

# Note: this resource owns the bucket's ENTIRE notification configuration.
# Archive/quarantine keys live outside landing_prefix, so moves never re-trigger.
resource "aws_s3_bucket_notification" "landing" {
  bucket = data.aws_s3_bucket.landing.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingest.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.landing_prefix
    filter_suffix       = ".zip"
  }

  depends_on = [aws_lambda_permission.allow_landing_bucket]
}
