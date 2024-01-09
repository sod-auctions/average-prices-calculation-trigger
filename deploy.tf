terraform {
  backend "s3" {
    bucket = "sod-auctions-deployments"
    key    = "terraform/average_prices_calculation_trigger"
    region = "us-east-1"
  }
}

provider "aws" {
  region = "us-east-1"
}

variable "app_name" {
  type    = string
  default = "average_prices_calculation_trigger"
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/bootstrap"
  output_path = "${path.module}/lambda_function.zip"
}

data "local_file" "lambda_zip_contents" {
  filename = data.archive_file.lambda_zip.output_path
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.app_name}_execution_role"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Action" : "sts:AssumeRole",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Effect" : "Allow"
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_exec_policy" {
  role   = aws_iam_role.lambda_exec.id
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
          "s3:PutObject"
        ],
        "Resource" : [
          "arn:aws:s3:::sod-auctions",
          "arn:aws:s3:::sod-auctions/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
          "glue:GetUserDefinedFunction",
          "glue:GetUserDefinedFunctions"
        ],
        "Resource" : "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "average_prices_calculation_trigger" {
  function_name    = var.app_name
  architectures    = ["arm64"]
  memory_size      = 128
  handler          = "bootstrap"
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.local_file.lambda_zip_contents.content_md5
  runtime          = "provided.al2023"
  timeout          = 60
}

resource "aws_lambda_function_event_invoke_config" "example" {
  function_name                = aws_lambda_function.average_prices_calculation_trigger.function_name
  maximum_retry_attempts       = 0
}
