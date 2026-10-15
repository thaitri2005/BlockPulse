data "aws_partition" "current" {}

# Which CPU architecture the chosen instance type uses decides which Amazon Linux image to start.
data "aws_ec2_instance_type" "selected" {
  instance_type = var.instance_type
}

# Pick an availability zone that actually offers the instance type (not every zone has every type).
data "aws_ec2_instance_type_offerings" "available" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
}

# The latest Amazon Linux 2023 image, published by AWS as a public SSM parameter.
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${local.architecture}"
}

# Fixed once, when the stack is first created, so the deadline and its tag never drift on later applies.
resource "time_static" "created" {}

locals {
  architecture      = contains(data.aws_ec2_instance_type.selected.supported_architectures, "arm64") ? "arm64" : "x86_64"
  availability_zone = sort(data.aws_ec2_instance_type_offerings.available.locations)[0]
  deadline          = timeadd(time_static.created.rfc3339, "${var.max_runtime_hours}h")
  bundle_file       = startswith(var.bundle_path, "/") || can(regex("^[A-Za-z]:", var.bundle_path)) ? var.bundle_path : "${path.module}/${var.bundle_path}"
  bundle_key        = "artifacts/blockpulse-app.zip"
  burstable         = startswith(var.instance_type, "t")

  user_data = templatefile("${path.module}/templates/user-data.sh.tftpl", {
    bucket          = aws_s3_bucket.data.bucket
    bundle_key      = local.bundle_key
    region          = var.region
    deadline        = local.deadline
    source_url      = var.source_url
    session_seconds = var.session_seconds
  })
}
