# The catalog itself is Terraform-managed. Existing catalogs must be imported once
# before the first apply in that workspace, e.g. in the default (dev) workspace:
#   terraform import databricks_catalog.this advworks_dev
resource "databricks_catalog" "this" {
  name    = local.catalog
  comment = "Data catalog for the ${terraform.workspace} environment"

  # Catalogs on Default Storage are created outside Terraform and imported: the API
  # reports storage_root, which is ForceNew, so without this a plan would replace
  # (delete + recreate) the catalog. prevent_destroy turns any such plan into an error.
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [storage_root]
  }
}
