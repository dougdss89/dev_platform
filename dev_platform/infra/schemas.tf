# First managed resource: one throwaway schema, so the initial apply is safe.
# Once comfortable, replace this with the real raw/trusted/enriched schemas
# (and remove them from the bundle's conf/schemas.yml so only Terraform owns them).

locals {
  layers = {
    raw      = "Raw layer - data as landed, ingestion metadata only"
    trusted  = "Trusted layer - cleaned, normalized, domain rules"
    enriched = "Enriched layer - business aggregates and measures"
  }
}

resource "databricks_schema" "layers" {
  for_each     = local.layers
  catalog_name = local.catalog
  name         = each.key
  comment      = each.value
}
