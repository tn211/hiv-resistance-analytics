terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = "hiv-project"
}

resource "aws_s3_bucket" "hiv_data" {
  bucket = "hiv-data-022784797781"
  force_destroy = true
}

resource "aws_athena_database" "hiv_analysis" {
  name   = "hiv_analysis"
  bucket = aws_s3_bucket.hiv_data.bucket
}

resource "aws_iam_role" "glue_role" {
  name = "glue_crawler_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.glue_role.name
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
  role       = aws_iam_role.glue_role.name
}

resource "aws_glue_crawler" "hiv_crawler" {
  database_name = "hiv_analysis"
  name          = "hiv_crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://hiv-data-022784797781"
  }

  schema_change_policy {
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}