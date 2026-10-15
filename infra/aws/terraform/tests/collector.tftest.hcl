# Offline tests: the AWS provider is mocked, so no account, credentials or network calls are needed and nothing
# is created. They check the security and cost properties of the configuration, not AWS behavior itself.
# Run from infra/aws/terraform:  terraform init && terraform test

mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_ec2_instance_type" {
    defaults = { supported_architectures = ["arm64"] }
  }
  mock_data "aws_ec2_instance_type_offerings" {
    defaults = { locations = ["us-west-2b", "us-west-2a"] }
  }
  mock_data "aws_ssm_parameter" {
    defaults = { value = "ami-0123456789abcdef0" }
  }
  mock_resource "aws_s3_bucket" {
    defaults = {
      arn    = "arn:aws:s3:::blockpulse-data-test"
      bucket = "blockpulse-data-test"
      id     = "blockpulse-data-test"
    }
  }
  mock_resource "aws_instance" {
    defaults = { id = "i-0123456789abcdef0" }
  }
}

variables {
  bundle_path = "tests/fixtures/fake-bundle.zip"
  alert_email = "owner@example.com"
}

run "secure_and_cost_capped_defaults" {
  command = apply

  # No way in: no ingress rules, and outbound limited to HTTPS.
  assert {
    condition     = length(aws_security_group.collector.ingress) == 0
    error_message = "The collector security group must not allow any inbound traffic."
  }
  assert {
    condition     = alltrue([for rule in aws_security_group.collector.egress : rule.from_port == 443 && rule.to_port == 443 && rule.protocol == "tcp"])
    error_message = "Outbound traffic must be limited to TCP 443."
  }
  assert {
    condition     = length(aws_default_security_group.locked.ingress) == 0 && length(aws_default_security_group.locked.egress) == 0
    error_message = "The VPC default security group must have no rules."
  }

  # Instance hardening.
  assert {
    condition     = aws_instance.collector.metadata_options[0].http_tokens == "required"
    error_message = "IMDSv2 must be required."
  }
  assert {
    condition     = aws_instance.collector.root_block_device[0].encrypted && aws_instance.collector.root_block_device[0].volume_type == "gp3"
    error_message = "The root disk must be encrypted gp3."
  }
  assert {
    condition     = aws_instance.collector.instance_initiated_shutdown_behavior == "stop"
    error_message = "The deadline poweroff must stop (not terminate) the instance so unsynced data survives."
  }
  assert {
    condition     = aws_instance.collector.credit_specification[0].cpu_credits == "standard"
    error_message = "Burstable instances must use standard CPU credits to avoid surplus-credit charges."
  }
  assert {
    condition     = aws_instance.collector.ami == "ami-0123456789abcdef0" && data.aws_ssm_parameter.al2023.name == "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
    error_message = "A Graviton instance type must boot the arm64 Amazon Linux 2023 image."
  }
  assert {
    condition     = aws_subnet.public.availability_zone == "us-west-2a"
    error_message = "The subnet must use the first zone (sorted) that offers the instance type."
  }

  # Bucket: private, encrypted, TLS-only, expiring.
  assert {
    condition = alltrue([
      aws_s3_bucket_public_access_block.data.block_public_acls,
      aws_s3_bucket_public_access_block.data.block_public_policy,
      aws_s3_bucket_public_access_block.data.ignore_public_acls,
      aws_s3_bucket_public_access_block.data.restrict_public_buckets,
    ])
    error_message = "All four S3 public access blocks must be on."
  }
  assert {
    condition     = jsondecode(aws_s3_bucket_policy.data.policy).Statement[0].Condition.Bool["aws:SecureTransport"] == "false" && jsondecode(aws_s3_bucket_policy.data.policy).Statement[0].Effect == "Deny"
    error_message = "The bucket must deny requests that don't use TLS."
  }
  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.data.rule[0].expiration[0].days == 30
    error_message = "Objects must expire after the default 30 days."
  }
  assert {
    condition     = aws_s3_bucket.data.force_destroy == false
    error_message = "The bucket must not be force-destroyable by default."
  }

  # Least privilege: list the bucket, read the bundle, write data and status. Nothing else.
  assert {
    condition     = toset(flatten([for s in jsondecode(aws_iam_role_policy.collector.policy).Statement : s.Action])) == toset(["s3:ListBucket", "s3:GetObject", "s3:PutObject"])
    error_message = "The instance role may only list, get and put objects."
  }
  assert {
    condition     = alltrue([for r in flatten([for s in jsondecode(aws_iam_role_policy.collector.policy).Statement : s.Resource]) : startswith(r, "arn:aws:s3:::blockpulse-data-test")])
    error_message = "Every permission must be scoped to the collector's own bucket."
  }
  assert {
    condition     = aws_iam_role_policy_attachment.ssm.policy_arn == "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    error_message = "Session Manager access needs the AmazonSSMManagedInstanceCore policy."
  }

  # Deadline and start-up script.
  assert {
    condition     = aws_instance.collector.tags["ExpiresAt"] == timeadd(time_static.created.rfc3339, "96h")
    error_message = "The instance must carry an ExpiresAt tag 96 hours after creation."
  }
  assert {
    condition     = strcontains(local.user_data, "BLOCKPULSE_DEADLINE=\"${timeadd(time_static.created.rfc3339, "96h")}\"") && strcontains(local.user_data, "s3://blockpulse-data-test/artifacts/blockpulse-app.zip")
    error_message = "The start-up script must pass the deadline and download the bundle from the collector bucket."
  }

  # Budget alert created when an email is given.
  assert {
    condition     = length(aws_budgets_budget.monthly) == 1 && aws_budgets_budget.monthly[0].limit_amount == "15.00"
    error_message = "A 15 USD monthly budget must be created when alert_email is set."
  }
  assert {
    condition     = length(aws_budgets_budget.monthly[0].notification) == 3
    error_message = "The budget must alert at 50% and 100% actual and 100% forecast."
  }
}

run "x86_instance_type_boots_x86_image_without_credit_block" {
  command = plan

  variables {
    instance_type = "m6i.large"
  }

  override_data {
    target = data.aws_ec2_instance_type.selected
    values = { supported_architectures = ["x86_64"] }
  }

  assert {
    condition     = data.aws_ssm_parameter.al2023.name == "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
    error_message = "An x86 instance type must boot the x86_64 image."
  }
  assert {
    condition     = length(aws_instance.collector.credit_specification) == 0
    error_message = "Non-burstable instance types must not get a credit specification."
  }
}

run "no_budget_without_email" {
  command = plan

  variables {
    alert_email = null
  }

  assert {
    condition     = length(aws_budgets_budget.monthly) == 0
    error_message = "Without alert_email no budget is created."
  }

  expect_failures = [check.budget_alert_configured]
}

run "missing_bundle_is_reported_clearly" {
  command = plan

  variables {
    bundle_path = "tests/fixtures/does-not-exist.zip"
  }

  expect_failures = [aws_s3_object.bundle]
}

run "rejects_out_of_range_runtime" {
  command = plan

  variables {
    max_runtime_hours = 500
  }

  expect_failures = [var.max_runtime_hours]
}

run "rejects_bad_email_and_url" {
  command = plan

  variables {
    alert_email = "not-an-email"
    source_url  = "http://insecure.example/ws"
  }

  expect_failures = [var.alert_email, var.source_url]
}

run "rejects_bad_name" {
  command = plan

  variables {
    name = "Has_Upper"
  }

  expect_failures = [var.name]
}
