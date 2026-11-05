# Quarantine notifier: landing quarantine/*.error.json -> GitHub issue.
# Decoupled from ingestion; a failure here never affects ingestion or quarantine.
#
# Secret handling: the GitHub token lives in SSM as a SecureString created
# OUTSIDE Terraform (aws ssm put-parameter), so it never enters Terraform state.
# Terraform only references it by name. The repo name is not secret and is managed here.

locals {
  ssm_prefix           = "/claims-etl/"
  github_token_param   = "${local.ssm_prefix}GITHUB_TOKEN"
  github_repo_param    = "${local.ssm_prefix}GITHUB_REPO"
  notifier_name        = "${var.function_name_prefix}-notify"
  notify_tags          = { Component = "notifier" }
  ssm_param_arn_prefix = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter"
}

resource "aws_ssm_parameter" "github_repo" {
  name        = local.github_repo_param
  description = "owner/name of the repo that receives quarantine issues"
  type        = "String"
  value       = var.github_repo
  tags        = local.notify_tags
}

resource "aws_cloudwatch_log_group" "notify" {
  name              = "/aws/lambda/${local.notifier_name}"
  retention_in_days = var.log_retention_days
  tags              = local.notify_tags
}

resource "aws_iam_role" "notify" {
  name               = "${local.notifier_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  tags               = local.notify_tags
}

data "aws_iam_policy_document" "notify" {
  # ListBucket turns "missing object" into 404 (not 403); needed for "no ticket record yet".
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket"]
    resources = [local.landing_arn, local.raw_arn]
  }

  statement {
    sid       = "ReadQuarantineReports"
    actions   = ["s3:GetObject"]
    resources = ["${local.landing_arn}/quarantine/*"]
  }

  statement {
    sid       = "ReadWriteTicketRecords"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${local.raw_arn}/_tickets/*"]
  }

  statement {
    sid     = "ReadGitHubSettings"
    actions = ["ssm:GetParameter"]
    resources = [
      "${local.ssm_param_arn_prefix}${local.github_token_param}",
      "${local.ssm_param_arn_prefix}${local.github_repo_param}",
    ]
  }

  # SecureString decryption with the AWS-managed aws/ssm key, only via SSM.
  statement {
    sid       = "DecryptViaSsmOnly"
    actions   = ["kms:Decrypt"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.region}.amazonaws.com"]
    }
  }

  statement {
    sid       = "WriteOwnLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.notify.arn}:*"]
  }
}

resource "aws_iam_role_policy" "notify" {
  name   = "${local.notifier_name}-s3-ssm-logs"
  role   = aws_iam_role.notify.id
  policy = data.aws_iam_policy_document.notify.json
}

resource "aws_lambda_function" "notify" {
  function_name    = local.notifier_name
  description      = "Quarantine .error.json -> GitHub issue or comment (claims-etl)"
  role             = aws_iam_role.notify.arn
  runtime          = "python3.13"
  architectures    = ["x86_64"]
  handler          = "claims_etl.notifications.lambda_handler.handler"
  filename         = var.lambda_zip_path # same package as ingestion; no pandas layer needed
  source_code_hash = filebase64sha256(var.lambda_zip_path)
  memory_size      = 256
  # Must exceed GitHubClient's worst case (4 attempts x 15 s request timeout,
  # plus up to 3 waits of <= 60 s), so rate limits end in a clean TicketingError.
  timeout = 300
  tags    = local.notify_tags

  environment {
    variables = {
      CLAIMS_ETL_CONFIG_DIR      = "/var/task/config"
      CLAIMS_ETL_SETTINGS_SOURCE = "ssm"
    }
  }

  depends_on = [aws_cloudwatch_log_group.notify, aws_iam_role_policy.notify]
}

# The handler raises on failure, so async retries re-deliver the event.
resource "aws_lambda_function_event_invoke_config" "notify" {
  function_name                = aws_lambda_function.notify.function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = 3600
}

resource "aws_lambda_permission" "notify_from_landing_bucket" {
  statement_id   = "AllowInvokeFromLandingBucket"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.notify.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = local.landing_arn
  source_account = data.aws_caller_identity.current.account_id
}
