variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-2"
}

variable "landing_bucket" {
  description = "Existing landing bucket (CSV files as delivered). Not managed here."
  type        = string
  default     = "fredr-claims-landing-2026"
}

variable "raw_bucket" {
  description = "Existing raw/bronze bucket (Parquet + manifests). Not managed here."
  type        = string
  default     = "fredr-claims-raw-2026"
}

variable "marts_bucket" {
  description = "Existing marts/gold bucket: one Parquet file per dbt mart, written by the transform Lambda. Must match marts_root in config/transform.yml. Not managed here."
  type        = string
  default     = "fredr-claims-marts-2026"
}

variable "landing_prefixes" {
  description = "One prefix per feed; .csv objects under each trigger ingestion. Must match landing_prefixes in config/ingestion.yml."
  type        = list(string)
  default     = ["cms/cclf/", "humana/ma/"]

  validation {
    condition     = length(var.landing_prefixes) > 0 && alltrue([for p in var.landing_prefixes : endswith(p, "/")])
    error_message = "landing_prefixes must be a non-empty list of prefixes ending in '/'."
  }
}

variable "function_name" {
  description = "Name of the ingestion Lambda function."
  type        = string
  default     = "claims-etl-ingest"
}

variable "function_name_prefix" {
  description = "Prefix for functions other than ingestion (e.g. <prefix>-notify)."
  type        = string
  default     = "claims-etl"
}

variable "github_repo" {
  description = "owner/name of the repo that receives quarantine issues (stored in SSM)."
  type        = string
  default     = "fredr171/claims-lakehouse-dbt-duckdb"
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
  description = "/tmp size (MB): holds the downloaded CSV and the local Parquet file."
  type        = number
  default     = 2048
}

variable "log_retention_days" {
  description = "CloudWatch log retention for the function."
  type        = number
  default     = 30
}

# --- Transform runner (transform.tf) ---------------------------------------------------

variable "artifacts_bucket" {
  description = "Bucket (managed here) holding deployment packages too big for direct upload."
  type        = string
  default     = "fredr-claims-artifacts-2026"
}

variable "transform_zip_path" {
  description = "Transform package built by scripts/build_lambda.py --transform."
  type        = string
  default     = "../../build/transform_lambda.zip"
}

variable "transform_memory_mb" {
  description = "Transform Lambda memory (MB); DuckDB uses up to 80% of it."
  type        = number
  default     = 2048
}

variable "transform_timeout_seconds" {
  description = "Transform Lambda timeout. A two-feed build takes ~15 s locally."
  type        = number
  default     = 600
}

variable "transform_max_receive_count" {
  description = "Receives of a failing batch before it moves to the dead-letter queue."
  type        = number
  default     = 3
}

variable "noncurrent_version_retention_days" {
  description = "Days an overwritten or deleted object version stays restorable in the landing and raw buckets."
  type        = number
  default     = 90

  validation {
    condition     = var.noncurrent_version_retention_days >= 1
    error_message = "noncurrent_version_retention_days must be at least 1."
  }
}
