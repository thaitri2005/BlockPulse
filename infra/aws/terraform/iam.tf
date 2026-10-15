# The instance's only permissions: be managed through Systems Manager (no SSH keys, no open ports), read the
# app bundle, and write collected data and status. It cannot delete objects or touch any other bucket.

locals {
  instance_policy = {
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListOwnBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.data.arn]
      },
      {
        Sid      = "ReadBundle"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${aws_s3_bucket.data.arn}/artifacts/*"]
      },
      {
        Sid      = "WriteDataAndStatus"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.data.arn}/data/*", "${aws_s3_bucket.data.arn}/status/*"]
      },
    ]
  }
}

resource "aws_iam_role" "collector" {
  name_prefix = "${var.name}-collector-"
  description = "BlockPulse collector instance role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.collector.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "collector" {
  name   = "bucket-access"
  role   = aws_iam_role.collector.id
  policy = jsonencode(local.instance_policy)
}

resource "aws_iam_instance_profile" "collector" {
  name_prefix = "${var.name}-collector-"
  role        = aws_iam_role.collector.name
}
