# Private bucket for the collected data and the app bundle. Encrypted, never public, TLS-only, and
# self-expiring so a forgotten bucket stops growing.

resource "aws_s3_bucket" "data" {
  bucket_prefix = "${var.name}-data-"
  force_destroy = var.bucket_force_destroy
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-collected-data"
    status = "Enabled"
    filter {}

    expiration {
      days = var.bucket_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 7
    }
  }
}

resource "aws_s3_bucket_policy" "data" {
  bucket = aws_s3_bucket.data.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.data]
}

# The application bundle the instance installs at first boot. Updating it does not change a running
# instance; see docs/guides/08-aws.md for rolling out new code.
resource "aws_s3_object" "bundle" {
  bucket      = aws_s3_bucket.data.id
  key         = local.bundle_key
  source      = local.bundle_file
  source_hash = fileexists(local.bundle_file) ? filemd5(local.bundle_file) : null

  lifecycle {
    precondition {
      condition     = fileexists(local.bundle_file)
      error_message = "App bundle not found at ${local.bundle_file}. Build it first: python infra/aws/build_bundle.py"
    }
  }
}
