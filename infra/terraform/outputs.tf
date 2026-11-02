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
  value = "s3://${var.landing_bucket}/${var.landing_prefix}**/*.zip (ObjectCreated)"
}
