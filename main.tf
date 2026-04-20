terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "s3_bucket_name" {
  description = "S3 bucket name for project storage"
  type        = string
}

provider "aws" {
  profile = "hiv-project"
}

locals {
  glue_crawlers = {
    tce_case_crawler         = "s3://${var.s3_bucket_name}/hivdb-tce/tce_case/"
    tce_drug_crawler         = "s3://${var.s3_bucket_name}/hivdb-tce/tce_by_drug_category/"
    tce_isolates_crawler     = "s3://${var.s3_bucket_name}/hivdb-tce/tce_isolates/"
    tce_measurements_crawler = "s3://${var.s3_bucket_name}/hivdb-tce/tce_measurements/"
    tce_mutations_crawler    = "s3://${var.s3_bucket_name}/hivdb-tce/tce_mutations/"
    tce_regimens_crawler     = "s3://${var.s3_bucket_name}/hivdb-tce/tce_regimens/"
    tce_treatments_crawler   = "s3://${var.s3_bucket_name}/hivdb-tce/tce_treatments/"
    tce_year_crawler         = "s3://${var.s3_bucket_name}/hivdb-tce/tce_by_year/"
    dlt_measurements_crawler = "s3://${var.s3_bucket_name}/test/partitioned_measurements/"
  }
}

resource "aws_s3_bucket" "hiv_data" {
  bucket        = var.s3_bucket_name
  force_destroy = true
}

resource "aws_athena_database" "hivdb_tce" {
  name   = "hivdb_tce"
  bucket = aws_s3_bucket.hiv_data.bucket
}

resource "aws_glue_catalog_database" "hivdb_tce" {
  name = "hivdb_tce"
}

resource "aws_iam_role" "glue_crawler_role" {
  name = "glue_crawler_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3_read_only" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_glue_crawler" "this" {
  for_each = local.glue_crawlers

  name          = each.key
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.hivdb_tce.name

  s3_target {
    path = each.value
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  lineage_configuration {
    crawler_lineage_settings = "DISABLE"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_s3_read_only,
  ]
}