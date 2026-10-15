terraform {
  # terraform test with mock providers needs 1.7+; 1.9 adds cross-variable validation used in variables.tf.
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.13"
    }
  }

  # State is local on purpose: one person, one short-lived experiment. Keep terraform.tfstate until after
  # `terraform destroy`; without it Terraform can't find what it created (see docs/AWS_DEPLOY.md).
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "blockpulse"
      Component = "collector"
      ManagedBy = "terraform"
      Owner     = var.owner
    }
  }
}
