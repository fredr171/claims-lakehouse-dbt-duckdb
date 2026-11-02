variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-2"
}

variable "landing_bucket" {
  description = "Existing landing bucket (zips as delivered). Not managed here."
  type        = string
  default     = "fredr-claims-landing-2026"
}

variable "raw_bucket" {
  description = "Existing raw/bronze bucket (Parquet + manifests). Not managed here."
  type        = string
  default     = "fredr-claims-raw-2026"
}

variable "landing_prefix" {
  description = "Only .zip objects under this prefix trigger ingestion. Must match config/ingestion.yml."
  type        = string
  default     = "cms/desynpuf/"
}

variable "function_name" {
  description = "Name of the ingestion Lambda function."
  type        = string
  default     = "claims-etl-ingest"
}

variable "lambda_zip_path" {
  description = "Deployment package built by scripts/build_lambda.py."
  type        = string
  default     = "../../build/lambda.zip"
}

variable "pandas_layer_arn" {
  description = <<-EOT
    AWS SDK for pandas public layer providing pyarrow + numpy. Must match the
    runtime's Python version. 3.18.0 for Python 3.13 ships pyarrow 24.0.0.
  EOT
  type        = string
  default     = "arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python313:17"
}

variable "memory_mb" {
  description = "Lambda memory (MB). CPU scales with memory; pyarrow benefits from more."
  type        = number
  default     = 1024
}

variable "timeout_seconds" {
  description = "Lambda timeout. The 2008 beneficiary file takes ~4 s locally."
  type        = number
  default     = 300
}

variable "ephemeral_storage_mb" {
  description = "/tmp size (MB): holds the downloaded zip and the local Parquet file."
  type        = number
  default     = 2048
}

variable "log_retention_days" {
  description = "CloudWatch log retention for the function."
  type        = number
  default     = 30
}
