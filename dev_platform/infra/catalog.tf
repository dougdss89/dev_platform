# The catalog itself is Terraform-managed. Existing catalogs must be imported once
# before the first apply in that workspace, e.g. in the default (dev) workspace:
#   terraform import databricks_catalog.this advworks_dev
resource "databricks_catalog" "this" {
  name    = local.catalog
  comment = "Data catalog for the ${terraform.workspace} environment"
}
