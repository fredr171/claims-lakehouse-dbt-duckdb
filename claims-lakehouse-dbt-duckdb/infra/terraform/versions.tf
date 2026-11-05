terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State is local for now (gitignored: *.tfstate). Move to an S3 backend
  # with locking before anyone else, or CI, runs this.
}

provider "aws" {
  region = var.region
  # Credentials: default AWS CLI profile locally; never hardcoded.

  default_tags {
    tags = {
      Project   = "claims-etl"
      ManagedBy = "terraform"
      # Component is tagged per resource (ingestion / notifier / transform).
    }
  }
}
