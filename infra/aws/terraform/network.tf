# A small dedicated network: one public subnet with an internet gateway, so the instance can reach the feed,
# S3, Systems Manager and PyPI without a NAT gateway (which would cost more than the instance). Nothing can
# connect in: the security group has no ingress rules, and administration goes through Session Manager.

resource "aws_vpc" "this" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.name}-collector" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-collector" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = "10.20.1.0/24"
  availability_zone       = local.availability_zone
  map_public_ip_on_launch = false # the instance asks for its own public address explicitly

  tags = { Name = "${var.name}-collector-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${var.name}-collector-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Strip the VPC's default security group of all rules so nothing can accidentally use it.
resource "aws_default_security_group" "locked" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-default-unused" }
}

resource "aws_security_group" "collector" {
  name        = "${var.name}-collector"
  description = "BlockPulse collector: no inbound access; outbound HTTPS only"
  vpc_id      = aws_vpc.this.id

  # DNS to the VPC resolver, time sync, and instance metadata are link-local and not filtered by security groups.
  egress {
    description = "HTTPS and WSS: mempool feed, S3, Systems Manager, package repositories"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name}-collector" }
}
