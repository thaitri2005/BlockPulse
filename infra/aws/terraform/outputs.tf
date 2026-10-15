output "region" {
  value = var.region
}

output "bucket" {
  description = "S3 bucket holding the collected data, status, and app bundle."
  value       = aws_s3_bucket.data.bucket
}

output "instance_id" {
  value = aws_instance.collector.id
}

output "collection_deadline" {
  description = "UTC time when collection stops and the instance powers off (fixed when the instance was created)."
  value       = aws_instance.collector.tags["ExpiresAt"]
}

output "status_command" {
  description = "Show the collector's latest status (updated every 10 minutes)."
  value       = "aws s3 cp s3://${aws_s3_bucket.data.bucket}/status/status.json - --region ${var.region}"
}

output "shell_command" {
  description = "Open a shell on the instance through Session Manager (needs the Session Manager plugin)."
  value       = "aws ssm start-session --target ${aws_instance.collector.id} --region ${var.region}"
}

output "download_command" {
  description = "Copy collected sessions into the local data/ folder (run from the repository root)."
  value       = "aws s3 sync s3://${aws_s3_bucket.data.bucket}/data/ data/ --region ${var.region}"
}

output "athena_workgroup" {
  description = "Athena workgroup for lake queries (per-query scan cap, encrypted results)."
  value       = aws_athena_workgroup.lake.name
}

output "glue_database" {
  description = "Glue database holding the lake tables: transactions, lifecycle_events, capture_sessions."
  value       = aws_glue_catalog_database.lake.name
}

output "athena_example_command" {
  description = "Run a query from the command line (results land in the bucket under athena-results/)."
  value       = "aws athena start-query-execution --work-group ${aws_athena_workgroup.lake.name} --query-execution-context Database=${aws_glue_catalog_database.lake.name} --query-string \"SELECT dt, hour, COUNT(*) FROM transactions GROUP BY dt, hour ORDER BY dt, hour\" --region ${var.region}"
}
