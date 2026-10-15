resource "aws_instance" "collector" {
  ami                                  = data.aws_ssm_parameter.al2023.value
  instance_type                        = var.instance_type
  subnet_id                            = aws_subnet.public.id
  vpc_security_group_ids               = [aws_security_group.collector.id]
  iam_instance_profile                 = aws_iam_instance_profile.collector.name
  associate_public_ip_address          = true   # outbound internet without a NAT gateway; nothing listens
  instance_initiated_shutdown_behavior = "stop" # the deadline poweroff stops compute charges but keeps the disk
  monitoring                           = false  # detailed monitoring costs extra
  user_data                            = local.user_data

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "disabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    encrypted             = true
    delete_on_termination = true
  }

  # Burstable instances default to "unlimited" credits on some types, which can bill extra under sustained load.
  dynamic "credit_specification" {
    for_each = local.burstable ? [1] : []
    content {
      cpu_credits = "standard"
    }
  }

  tags = {
    Name      = "${var.name}-collector"
    ExpiresAt = local.deadline
  }

  lifecycle {
    # A newer Amazon Linux image or an edited start-up script must not replace a running collector (and its
    # unsynced data). Roll out deliberately with `terraform apply -replace=aws_instance.collector`. The deadline is
    # baked into the start-up script, so its tag is frozen with it and always tells the truth about this instance.
    ignore_changes = [ami, user_data, tags["ExpiresAt"]]

    precondition {
      condition     = length(data.aws_ec2_instance_type_offerings.available.locations) > 0
      error_message = "${var.instance_type} is not offered in any availability zone of ${var.region}."
    }
  }

  depends_on = [aws_s3_object.bundle, aws_route_table_association.public, aws_iam_role_policy.collector]
}
