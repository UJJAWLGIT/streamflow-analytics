# =============================================================================
# StreamFlow Analytics — Production AWS Infrastructure
# =============================================================================
# Resources:
#   - S3 Lakehouse buckets (bronze / silver / gold / mlflow / logs)
#   - EMR Serverless application (4096 vCPU / 19,200 GB)
#   - Kinesis Data Stream (100 shards, 7-day retention)
#   - AWS Glue catalog + crawlers
#   - IAM roles (least-privilege)
#   - CloudWatch dashboards + metric alarms
#   - SNS topic → PagerDuty
# =============================================================================

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "streamflow-terraform-state"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "streamflow-terraform-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "streamflow-analytics"
      Environment = "prod"
      Owner       = "ujjawl.kumar"
      ManagedBy   = "terraform"
      CostCenter  = "data-engineering"
    }
  }
}

# =============================================================================
# Variables
# =============================================================================

variable "aws_region"        { default = "us-east-1" }
variable "environment"       { default = "prod" }
variable "project_name"      { default = "streamflow-analytics" }
variable "pagerduty_endpoint" {
  description = "PagerDuty HTTPS endpoint for SNS alerts"
  sensitive   = true
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# =============================================================================
# S3 Lakehouse Buckets
# =============================================================================

resource "aws_s3_bucket" "bronze" {
  bucket = "${local.name_prefix}-bronze"
}

resource "aws_s3_bucket" "silver" {
  bucket = "${local.name_prefix}-silver"
}

resource "aws_s3_bucket" "gold" {
  bucket = "${local.name_prefix}-gold"
}

resource "aws_s3_bucket" "mlflow" {
  bucket = "${local.name_prefix}-mlflow"
}

resource "aws_s3_bucket" "logs" {
  bucket = "${local.name_prefix}-logs"
}

# Lifecycle policies — transition to cheaper storage tiers
resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "bronze_tiering"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365   # Raw events: keep 1 year
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "gold_lifecycle" {
  bucket = aws_s3_bucket.gold.id

  rule {
    id     = "gold_tiering"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    # Gold tables: no expiration (permanent reporting data)
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    bronze = aws_s3_bucket.bronze.id
    silver = aws_s3_bucket.silver.id
    gold   = aws_s3_bucket.gold.id
    mlflow = aws_s3_bucket.mlflow.id
    logs   = aws_s3_bucket.logs.id
  }

  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =============================================================================
# EMR Serverless Application
# =============================================================================

resource "aws_emrserverless_application" "streamflow" {
  name          = local.name_prefix
  release_label = "emr-6.15.0"
  type          = "SPARK"

  initial_capacity {
    initial_capacity_type = "Driver"

    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "4vCPU"
        memory = "16GB"
        disk   = "200GB"
      }
    }
  }

  maximum_capacity {
    cpu    = "4096vCPU"     # 4096 vCPU max
    memory = "19200GB"      # 19.2 TB RAM max
    disk   = "120000GB"     # 120 TB disk max
  }

  auto_stop_config {
    enabled              = true
    idle_timeout_minutes = 15
  }

  network_configuration {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.emr.id]
  }

  tags = {
    AssetId = "2240558407951667756"
  }
}

# =============================================================================
# Kinesis Data Stream (Real-time ingestion)
# =============================================================================

resource "aws_kinesis_stream" "cancel_flow_events" {
  name             = "${local.name_prefix}-events"
  shard_count      = 100    # 100 shards = 100K records/sec ingestion rate
  retention_period = 168    # 7 days

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.kinesis.key_id

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

resource "aws_kms_key" "kinesis" {
  description             = "KMS key for Kinesis encryption"
  deletion_window_in_days = 7
}

# =============================================================================
# AWS Glue Catalog
# =============================================================================

resource "aws_glue_catalog_database" "bronze" {
  name = "${var.project_name}_bronze"
  description = "Bronze layer — raw ingestion tables"
}

resource "aws_glue_catalog_database" "silver" {
  name = "${var.project_name}_silver"
  description = "Silver layer — curated Delta Lake tables"
}

resource "aws_glue_catalog_database" "gold" {
  name = "${var.project_name}_gold"
  description = "Gold layer — governed 3-star reporting tables"
}

# =============================================================================
# IAM — EMR Execution Role (least-privilege)
# =============================================================================

resource "aws_iam_role" "emr_execution" {
  name = "${local.name_prefix}-emr-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "emr_execution_policy" {
  name = "streamflow-emr-policy"
  role = aws_iam_role.emr_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn, "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,   "${aws_s3_bucket.gold.arn}/*",
          aws_s3_bucket.logs.arn,   "${aws_s3_bucket.logs.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]
        Resource = ["arn:aws:glue:*:*:catalog", "arn:aws:glue:*:*:database/*", "arn:aws:glue:*:*:table/*/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData", "logs:CreateLogGroup", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

# =============================================================================
# CloudWatch — Dashboards + Alarms
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "data_freshness" {
  alarm_name          = "${local.name_prefix}-data-freshness-breach"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DataFreshnessLagHours"
  namespace           = "StreamFlowAnalytics"
  period              = 3600
  statistic           = "Maximum"
  threshold           = 8.0   # SLO: data fresher than 8 hours
  alarm_description   = "P1: rpt_cancel_flow_final_metrics data freshness > 8h SLO"
  alarm_actions       = [aws_sns_topic.pagerduty_p1.arn]
  ok_actions          = [aws_sns_topic.pagerduty_p1.arn]
}

resource "aws_cloudwatch_metric_alarm" "dq_pass_rate" {
  alarm_name          = "${local.name_prefix}-dq-pass-rate-low"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DQPassRate"
  namespace           = "StreamFlowAnalytics"
  period              = 3600
  statistic           = "Minimum"
  threshold           = 0.95
  alarm_description   = "P1: DQ pass rate < 95% on CRITICAL checks"
  alarm_actions       = [aws_sns_topic.pagerduty_p1.arn]
}

# =============================================================================
# SNS → PagerDuty Alerting
# =============================================================================

resource "aws_sns_topic" "pagerduty_p1" {
  name = "${local.name_prefix}-pagerduty-p1"
}

resource "aws_sns_topic_subscription" "pagerduty_https" {
  topic_arn = aws_sns_topic.pagerduty_p1.arn
  protocol  = "https"
  endpoint  = var.pagerduty_endpoint
}

# =============================================================================
# Outputs
# =============================================================================

output "bronze_bucket" { value = aws_s3_bucket.bronze.id }
output "silver_bucket" { value = aws_s3_bucket.silver.id }
output "gold_bucket"   { value = aws_s3_bucket.gold.id }
output "emr_app_id"    { value = aws_emrserverless_application.streamflow.id }
output "kinesis_stream_arn" { value = aws_kinesis_stream.cancel_flow_events.arn }
output "emr_execution_role_arn" { value = aws_iam_role.emr_execution.arn }
