variable "region" {
  description = "AWS region. us-west-2 is close to the regional mempool.space node used by default and has low prices."
  type        = string
  default     = "us-west-2"
}

variable "name" {
  description = "Prefix for resource names."
  type        = string
  default     = "blockpulse"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.name))
    error_message = "name must be 3-21 lowercase letters, digits or hyphens, starting with a letter (it becomes part of an S3 bucket name)."
  }
}

variable "owner" {
  description = "Who to ask about these resources; added as an Owner tag."
  type        = string
  default     = "blockpulse-dev"
}

variable "instance_type" {
  description = "EC2 instance type. The default Graviton t4g.small (2 vCPU, 2 GiB) is plenty for capture and processing."
  type        = string
  default     = "t4g.small"
}

variable "root_volume_gb" {
  description = "Encrypted gp3 root disk. A day of collection uses about 4-5 GB; the collector stops capturing below 5 GiB free."
  type        = number
  default     = 40

  validation {
    condition     = var.root_volume_gb >= 20 && var.root_volume_gb <= 200
    error_message = "root_volume_gb must be between 20 and 200."
  }
}

variable "max_runtime_hours" {
  description = "Hours after creation when collection stops and the instance powers itself off. Fixed at creation."
  type        = number
  default     = 96

  validation {
    condition     = var.max_runtime_hours >= 1 && var.max_runtime_hours <= 336 && floor(var.max_runtime_hours) == var.max_runtime_hours
    error_message = "max_runtime_hours must be a whole number from 1 to 336 (two weeks)."
  }
}

variable "session_seconds" {
  description = "Length of each capture session. Hourly sessions keep files small and give the dataset many sessions."
  type        = number
  default     = 3600

  validation {
    condition     = var.session_seconds >= 300 && var.session_seconds <= 21600
    error_message = "session_seconds must be between 300 (5 minutes) and 21600 (6 hours)."
  }
}

variable "source_url" {
  description = "mempool.space-compatible WebSocket endpoint."
  type        = string
  default     = "wss://node203.sv1.mempool.space/api/v1/ws"

  validation {
    condition     = can(regex("^wss://[^/\\s]+/", var.source_url))
    error_message = "source_url must be a wss:// URL with a path."
  }
}

variable "bundle_path" {
  description = "App bundle built by infra/aws/build_bundle.py. Relative paths are resolved from this directory."
  type        = string
  default     = "../build/blockpulse-app.zip"
}

variable "bucket_retention_days" {
  description = "Objects in the data bucket are deleted after this many days, so a forgotten bucket doesn't grow forever."
  type        = number
  default     = 30

  validation {
    condition     = var.bucket_retention_days >= 1 && var.bucket_retention_days <= 365
    error_message = "bucket_retention_days must be between 1 and 365."
  }
}

variable "bucket_force_destroy" {
  description = "Allow `terraform destroy` to delete a bucket that still has data. Leave false until you have downloaded it."
  type        = bool
  default     = false
}

variable "alert_email" {
  description = "Email for the monthly cost budget alerts. Strongly recommended; null skips the budget."
  type        = string
  default     = null

  validation {
    condition     = var.alert_email == null || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must look like an email address, or be null."
  }
}

variable "budget_usd" {
  description = "Monthly budget that triggers alert emails (at 50% and 100% actual, and 100% forecast). Alerts can lag usage; they are not a hard cap."
  type        = number
  default     = 15

  validation {
    condition     = var.budget_usd > 0 && var.budget_usd <= 1000
    error_message = "budget_usd must be between 0 and 1000."
  }
}
