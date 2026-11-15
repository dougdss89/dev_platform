# List granted permissions for the service principal account in Databricks
# All privileges must be managed in this section

resource "databricks_grants" "layer_write" {
  for_each = local.layers
  schema = "${local.catalog}.${each.key}"
  # The schema is referenced by name only, so state the ordering explicitly.
  depends_on = [databricks_schema.layers]
  grant {
    principal  = var.sp_application_id
    # SELECT is separate from MODIFY in Unity Catalog: trusted reads raw, and MERGE reads its target.
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }

}

resource "databricks_grant" "catalog_use"{
    catalog = local.catalog
    principal = var.sp_application_id
    privileges = ["USE_CATALOG"]
    depends_on = [databricks_catalog.this]
}

#in the shared volume databricks_grant is singular
resource "databricks_grant" "landing_read" {
    volume = var.landing_volume_path
    principal  = var.sp_application_id
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
    depends_on = [databricks_volume.landing]
  }