output "function_name" {
  value = aws_lambda_function.ingest.function_name
}

output "function_arn" {
  value = aws_lambda_function.ingest.arn
}

output "role_arn" {
  value = aws_iam_role.ingest.arn
}

output "log_group" {
  value = aws_cloudwatch_log_group.ingest.name
}

output "trigger" {
  value = [for p in var.landing_prefixes : "s3://${var.landing_bucket}/${p}**/*.csv (ObjectCreated)"]
}

output "notifier_function_name" {
  value = aws_lambda_function.notify.function_name
}

output "notifier_log_group" {
  value = aws_cloudwatch_log_group.notify.name
}

output "notifier_trigger" {
  value = "s3://${var.landing_bucket}/quarantine/**/*.error.json (ObjectCreated)"
}
