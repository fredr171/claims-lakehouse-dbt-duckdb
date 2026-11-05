# Transform runner: ingestion's FileIngested events -> EventBridge -> SQS FIFO -> Lambda
# claims-etl-transform, which runs `dbt build` (staging + intermediate) against the DuckDB
# state file in the raw bucket (_transform/state/). See src/claims_etl/transform/.
#
# Ordering and single writer: the queue is FIFO and every event goes to ONE message group,
# so at most one build runs at a time; up to 10 queued events share one build. The state
# upload is also conditional on the ETag it started from.
# Failure handling: data failures (STOP/WARN tests) are final answers and delete the
# messages; `error` outcomes raise, the batch is retried, and after max_receive_count
# receives it moves to the dead-letter queue.

locals {
  transform_name = "${var.function_name_prefix}-transform"
  transform_tags = { Component = "transform" }
}

# --- Deployment package (over 50 MB, so it goes through S3) ----------------------------

resource "aws_s3_bucket" "artifacts" {
  bucket = var.artifacts_bucket
  tags   = local.transform_tags
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_object" "transform_package" {
  bucket      = aws_s3_bucket.artifacts.id
  key         = "lambda/transform_lambda.zip"
  source      = var.transform_zip_path
  source_hash = filemd5(var.transform_zip_path)
  tags        = local.transform_tags

  depends_on = [
    aws_s3_bucket_public_access_block.artifacts,
    aws_s3_bucket_server_side_encryption_configuration.artifacts,
  ]
}

# --- Queue ----------------------------------------------------------------------------

resource "aws_sqs_queue" "transform_dlq" {
  name                      = "${local.transform_name}-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600 # 14 days (the maximum)
  sqs_managed_sse_enabled   = true
  tags                      = local.transform_tags
}

resource "aws_sqs_queue" "transform" {
  name                      = "${local.transform_name}.fifo"
  fifo_queue                = true
  message_retention_seconds = 345600 # 4 days
  # AWS guidance for Lambda consumers: at least 6x the function timeout.
  visibility_timeout_seconds = 6 * var.transform_timeout_seconds
  sqs_managed_sse_enabled    = true
  tags                       = local.transform_tags

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.transform_dlq.arn
    maxReceiveCount     = var.transform_max_receive_count
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "transform_dlq" {
  queue_url = aws_sqs_queue.transform_dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.transform.arn]
  })
}

# --- EventBridge: FileIngested -> queue -----------------------------------------------

resource "aws_cloudwatch_event_rule" "file_ingested" {
  name        = "${local.transform_name}-file-ingested"
  description = "Ingestion loaded a file into raw -> rebuild that feed's staging + intermediate"
  event_pattern = jsonencode({
    source        = ["claims-etl.ingestion"] # claims_etl/events.py EVENT_SOURCE
    "detail-type" = ["FileIngested"]         # claims_etl/events.py DETAIL_TYPE
  })
  tags = local.transform_tags
}

resource "aws_cloudwatch_event_target" "transform_queue" {
  rule      = aws_cloudwatch_event_rule.file_ingested.name
  target_id = "transform-queue"
  arn       = aws_sqs_queue.transform.arn

  sqs_target {
    message_group_id = "transform" # one group: builds run one at a time, in order
  }
}

data "aws_iam_policy_document" "transform_queue" {
  statement {
    sid       = "AllowFileIngestedRule"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.transform.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.file_ingested.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "transform" {
  queue_url = aws_sqs_queue.transform.id
  policy    = data.aws_iam_policy_document.transform_queue.json
}

# --- Function -------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "transform" {
  name              = "/aws/lambda/${local.transform_name}"
  retention_in_days = var.log_retention_days
  tags              = local.transform_tags
}

resource "aws_iam_role" "transform" {
  name               = "${local.transform_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  tags               = local.transform_tags
}

data "aws_iam_policy_document" "transform" {
  # ListBucket: DuckDB globs the raw partitions; a missing state object returns 404.
  statement {
    sid       = "ListRawBucket"
    actions   = ["s3:ListBucket"]
    resources = [local.raw_arn]
  }

  statement {
    sid       = "ReadRawParquet"
    actions   = ["s3:GetObject"]
    resources = [for p in var.landing_prefixes : "${local.raw_arn}/${p}*"]
  }

  # State file, its backup copy, and run artifacts (config/transform.yml).
  statement {
    sid       = "ReadWriteTransformState"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${local.raw_arn}/_transform/*"]
  }

  statement {
    sid = "ConsumeTransformQueue"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [aws_sqs_queue.transform.arn]
  }

  statement {
    sid       = "WriteOwnLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.transform.arn}:*"]
  }
}

resource "aws_iam_role_policy" "transform" {
  name   = "${local.transform_name}-s3-sqs-logs"
  role   = aws_iam_role.transform.id
  policy = data.aws_iam_policy_document.transform.json
}

resource "aws_lambda_function" "transform" {
  function_name    = local.transform_name
  description      = "FileIngested -> dbt build of staging + intermediate (claims-etl)"
  role             = aws_iam_role.transform.arn
  runtime          = "python3.13"
  architectures    = ["x86_64"] # the bundled DuckDB extensions are linux_amd64
  handler          = "claims_etl.transform.lambda_handler.handler"
  s3_bucket        = aws_s3_object.transform_package.bucket
  s3_key           = aws_s3_object.transform_package.key
  source_code_hash = filebase64sha256(var.transform_zip_path)
  memory_size      = var.transform_memory_mb
  timeout          = var.transform_timeout_seconds
  tags             = local.transform_tags

  ephemeral_storage {
    size = var.ephemeral_storage_mb # the DuckDB working copy + dbt target/
  }

  environment {
    variables = {
      CLAIMS_ETL_CONFIG_DIR       = "/var/task/config"
      CLAIMS_DBT_PROJECT_DIR      = "/var/task/dbt_project" # scripts/build_lambda.py
      CLAIMS_DBT_TARGET           = "lambda"                # dbt/profiles.yml
      CLAIMS_DUCKDB_EXTENSION_DIR = "/var/task/duckdb_extensions"
      HOME                        = "/tmp" # DuckDB and dbt write under ~; /var/task is read-only
    }
  }

  depends_on = [aws_cloudwatch_log_group.transform, aws_iam_role_policy.transform]
}

resource "aws_lambda_event_source_mapping" "transform_queue" {
  event_source_arn = aws_sqs_queue.transform.arn
  function_name    = aws_lambda_function.transform.arn
  batch_size       = 10 # FIFO maximum; queued events share one dbt build
  enabled          = true
}

# --- Ingestion may publish FileIngested to the default bus -----------------------------

data "aws_iam_policy_document" "ingest_events" {
  statement {
    sid       = "PublishFileIngested"
    actions   = ["events:PutEvents"]
    resources = ["arn:aws:events:${var.region}:${data.aws_caller_identity.current.account_id}:event-bus/default"]
    condition {
      test     = "StringEquals"
      variable = "events:source"
      values   = ["claims-etl.ingestion"]
    }
  }
}

resource "aws_iam_role_policy" "ingest_events" {
  name   = "${var.function_name}-events"
  role   = aws_iam_role.ingest.id
  policy = data.aws_iam_policy_document.ingest_events.json
}
