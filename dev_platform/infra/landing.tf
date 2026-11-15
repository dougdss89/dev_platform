# Managed landing volume, created only where it does not exist yet (the new prd
# account). Dev keeps its existing S3-backed volume, which Terraform does not own.
# Same 3-level name as dev so pipeline code and grants.tf work unchanged.
locals {
  landing = split(".", var.landing_volume_path) # [catalog, schema, volume]
}

resource "databricks_catalog" "landing" {
  count   = var.create_landing_volume ? 1 : 0
  name    = local.landing[0]
  comment = "Landing zone for source files"

  # Same reason as databricks_catalog.this (imported Default Storage catalog).
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [storage_root]
  }
}

resource "databricks_schema" "landing" {
  count        = var.create_landing_volume ? 1 : 0
  catalog_name = databricks_catalog.landing[0].name
  name         = local.landing[1]
}

resource "databricks_volume" "landing" {
  count        = var.create_landing_volume ? 1 : 0
  catalog_name = databricks_catalog.landing[0].name
  schema_name  = databricks_schema.landing[0].name
  name         = local.landing[2]
  volume_type  = "MANAGED"
}

# Reading a volume also needs USE on its catalog and schema.
resource "databricks_grant" "landing_catalog_use" {
  count      = var.create_landing_volume ? 1 : 0
  catalog    = databricks_catalog.landing[0].name
  principal  = var.sp_application_id
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "landing_schema_use" {
  count      = var.create_landing_volume ? 1 : 0
  schema     = "${databricks_catalog.landing[0].name}.${databricks_schema.landing[0].name}"
  principal  = var.sp_application_id
  privileges = ["USE_SCHEMA"]
}
