# Athena over the partitioned Parquet lake the collector uploads to s3://<bucket>/data/lake/.
# The Glue tables come from lake_schema.json (exported by `blockpulse lake-schema`, checked by a test), so the
# catalog can't drift from what the Python code writes. Partition projection computes partition locations from
# the dt/hour pattern, so new hours are queryable immediately: no crawler, no MSCK REPAIR, nothing to pay for.

locals {
  lake_schema   = jsondecode(file("${path.module}/lake_schema.json"))
  lake_location = "s3://${aws_s3_bucket.data.bucket}/data/lake"
  # Named demo queries shared with local DuckDB (`blockpulse lake-query <name>`).
  query_files = fileset("${path.module}/../../../analytics/queries", "*.sql")
}

resource "aws_glue_catalog_database" "lake" {
  name        = replace("${var.name}_lake", "-", "_")
  description = "BlockPulse partitioned Parquet lake, schema v${local.lake_schema.schema_version}"
}

resource "aws_glue_catalog_table" "lake" {
  for_each = local.lake_schema.tables

  name          = each.key
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "BlockPulse ${each.key} (Hive partitions: dt${each.value.hourly ? ", hour" : ""})"

  parameters = merge(
    {
      EXTERNAL                      = "TRUE"
      classification                = "parquet"
      "parquet.compression"         = "ZSTD"
      "projection.enabled"          = "true"
      "projection.dt.type"          = "date"
      "projection.dt.format"        = "yyyy-MM-dd"
      "projection.dt.range"         = "${var.lake_start_date},NOW"
      "projection.dt.interval"      = "1"
      "projection.dt.interval.unit" = "DAYS"
      "storage.location.template" = (each.value.hourly
        ? "${local.lake_location}/${each.key}/dt=$${dt}/hour=$${hour}/"
      : "${local.lake_location}/${each.key}/dt=$${dt}/")
    },
    each.value.hourly ? {
      "projection.hour.type"   = "integer"
      "projection.hour.range"  = "0,23"
      "projection.hour.digits" = "2"
    } : {}
  )

  partition_keys {
    name = "dt"
    type = "string"
  }

  dynamic "partition_keys" {
    for_each = each.value.hourly ? ["hour"] : []
    content {
      name = "hour"
      type = "int"
    }
  }

  storage_descriptor {
    location      = "${local.lake_location}/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }
}

# Queries run in this workgroup are capped per query and write encrypted, expiring results to the data bucket.
resource "aws_athena_workgroup" "lake" {
  name          = "${var.name}-lake"
  description   = "BlockPulse lake queries (per-query scan cap)"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "AUTO"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_athena_named_query" "demo" {
  for_each = local.query_files

  name        = trimsuffix(each.value, ".sql")
  description = "BlockPulse demo query (also runs locally: blockpulse lake-query ${trimsuffix(each.value, ".sql")})"
  database    = aws_glue_catalog_database.lake.name
  workgroup   = aws_athena_workgroup.lake.id
  query       = file("${path.module}/../../../analytics/queries/${each.value}")
}
