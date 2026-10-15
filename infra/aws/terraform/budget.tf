# An account-wide monthly cost budget with email alerts. AWS Budgets data lags real usage by hours, so this is
# an early warning, not a spending cap; the deadline poweroff and `terraform destroy` are what stop the spend.

resource "aws_budgets_budget" "monthly" {
  count = var.alert_email == null ? 0 : 1

  name         = "${var.name}-monthly"
  budget_type  = "COST"
  limit_amount = format("%.2f", var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = [
      { threshold = 50, type = "ACTUAL" },
      { threshold = 100, type = "ACTUAL" },
      { threshold = 100, type = "FORECASTED" },
    ]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value.threshold
      threshold_type             = "PERCENTAGE"
      notification_type          = notification.value.type
      subscriber_email_addresses = [var.alert_email]
    }
  }
}

check "budget_alert_configured" {
  assert {
    condition     = var.alert_email != null
    error_message = "No alert_email set, so no cost budget was created. Set alert_email in terraform.tfvars to get budget emails."
  }
}
