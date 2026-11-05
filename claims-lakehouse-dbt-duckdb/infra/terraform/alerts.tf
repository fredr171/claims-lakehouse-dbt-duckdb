# Pipeline alerts -> GitHub issues, through the existing notifier Lambda (no email).
#
#   transform run report.json (raw bucket) ──S3 trigger──► claims-etl-notify ──► issue
#   CloudWatch alarm state change ──► SNS claims-etl-alerts ──► claims-etl-notify ──► issue
#
# The report path covers every build that finishes (stopped / error, and closes issues
# on passed / warned). The alarms cover failures that leave no report. Alarm names must
# match alerts.next_steps.alarms in config/notifications.yml.
# Not alarmed: the notifier itself (its alarm would be delivered by the function that is
# failing). Its errors are in /aws/lambda/claims-etl-notify; the GitHub token expiry is in
# CLAUDE.md.

locals {
  alerts_name = "${var.function_name_prefix}-alerts"
  alerts_tags = { Component = "alerts" }
  alarm_defaults = {
    period              = 60
    evaluation_periods  = 1
    threshold           = 0
    comparison_operator = "GreaterThanThreshold"
  }
}

# --- SNS topic -> notifier -------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name = local.alerts_name
  tags = local.alerts_tags
}

data "aws_iam_policy_document" "alerts_topic" {
  statement {
    sid       = "AllowCloudWatchAlarms"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts_topic.json
}

resource "aws_lambda_permission" "notify_from_alerts_topic" {
  statement_id  = "AllowInvokeFromAlertsTopic"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notify.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.alerts.arn
}

resource "aws_sns_topic_subscription" "alerts_to_notify" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.notify.arn
}

# --- Transform run reports -> notifier ----------------------------------------------------

resource "aws_lambda_permission" "notify_from_raw_bucket" {
  statement_id   = "AllowInvokeFromRawBucket"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.notify.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = local.raw_arn
  source_account = data.aws_caller_identity.current.account_id # confused-deputy guard
}

# Owns the raw bucket's ENTIRE notification configuration (it had none before), so every
# raw-bucket trigger lives here.
resource "aws_s3_bucket_notification" "raw" {
  bucket = data.aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.notify.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "_transform/runs/"
    filter_suffix       = "/report.json"
  }

  depends_on = [aws_lambda_permission.notify_from_raw_bucket]
}

# --- Alarms (each -> SNS on ALARM and on OK, so the issue opens and closes itself) -----------

resource "aws_cloudwatch_metric_alarm" "transform_dlq_not_empty" {
  alarm_name          = "${local.transform_name}-dlq-not-empty"
  alarm_description   = "A transform batch ran out of retries and sits in the DLQ."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.transform_dlq.name }
  statistic           = "Maximum"
  period              = local.alarm_defaults.period
  evaluation_periods  = local.alarm_defaults.evaluation_periods
  threshold           = local.alarm_defaults.threshold
  comparison_operator = local.alarm_defaults.comparison_operator
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.alerts_tags
}

resource "aws_cloudwatch_metric_alarm" "events_undelivered" {
  alarm_name          = "${var.function_name_prefix}-events-undelivered"
  alarm_description   = "FileIngested events EventBridge gave up on (rule target DLQ)."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.file_ingested_dlq.name }
  statistic           = "Maximum"
  period              = local.alarm_defaults.period
  evaluation_periods  = local.alarm_defaults.evaluation_periods
  threshold           = local.alarm_defaults.threshold
  comparison_operator = local.alarm_defaults.comparison_operator
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.alerts_tags
}

resource "aws_cloudwatch_metric_alarm" "events_delivery_failed" {
  alarm_name          = "${var.function_name_prefix}-events-delivery-failed"
  alarm_description   = "EventBridge failed to deliver a FileIngested event to the transform queue."
  namespace           = "AWS/Events"
  metric_name         = "FailedInvocations"
  dimensions          = { RuleName = aws_cloudwatch_event_rule.file_ingested.name }
  statistic           = "Sum"
  period              = local.alarm_defaults.period
  evaluation_periods  = local.alarm_defaults.evaluation_periods
  threshold           = local.alarm_defaults.threshold
  comparison_operator = local.alarm_defaults.comparison_operator
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.alerts_tags
}

resource "aws_cloudwatch_metric_alarm" "ingest_errors" {
  alarm_name          = "${aws_lambda_function.ingest.function_name}-errors"
  alarm_description   = "Ingestion raised (bad data is quarantined instead, so this is infrastructure)."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.ingest.function_name }
  statistic           = "Sum"
  period              = local.alarm_defaults.period
  evaluation_periods  = local.alarm_defaults.evaluation_periods
  threshold           = local.alarm_defaults.threshold
  comparison_operator = local.alarm_defaults.comparison_operator
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.alerts_tags
}

# A killed transform (timeout / out of memory) writes no report. Lambda logs these lines
# to the function's log group itself.
resource "aws_cloudwatch_log_metric_filter" "transform_killed" {
  name           = "${local.transform_name}-killed"
  log_group_name = aws_cloudwatch_log_group.transform.name
  pattern        = "?\"Task timed out\" ?\"Runtime exited\""
  metric_transformation {
    name          = "TransformKilled"
    namespace     = "ClaimsEtl"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "transform_killed" {
  alarm_name          = "${local.transform_name}-killed"
  alarm_description   = "The transform Lambda was killed (timeout or out of memory) before writing a report."
  namespace           = "ClaimsEtl"
  metric_name         = aws_cloudwatch_log_metric_filter.transform_killed.metric_transformation[0].name
  statistic           = "Sum"
  period              = local.alarm_defaults.period
  evaluation_periods  = local.alarm_defaults.evaluation_periods
  threshold           = local.alarm_defaults.threshold
  comparison_operator = local.alarm_defaults.comparison_operator
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = local.alerts_tags
}
