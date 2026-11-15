# Terraform root for platform/UC infra (catalog, schemas, SP, grants).
# DABs still owns the workloads (jobs/wheel) — keep the two from managing the
# same resource.
# Workspace -> catalog. A lookup (not a ternary) so an unexpected workspace name errors out
# instead of silently planning against advworks_dev.
#   default -> dev workspace (original account)
#   prd     -> NEW prod workspace/account (apply with -var databricks_profile=<prd profile>)
#   prod    -> legacy state of the old prod objects (kept until they are retired)
locals {
  catalogs = {
    default = "advworks_dev"
    prd     = "advworks"
    prod    = "advworks_prd"
  }
  catalog = local.catalogs[terraform.workspace]
}
terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source = "databricks/databricks"
      # ~> 1.0 resolves to the latest 1.x on `init`; pin the exact version it
      # locks (see .terraform.lock.hcl) once you're happy.
      version = "~> 1.0"
    }
  }

  # State is local for now (gitignored). Move to a remote backend (cloud bucket
  # or Terraform Cloud) before sharing with a team — never commit tfstate.
}

# Auth reuses your Databricks CLI profile for local runs. In CI, drop the profile
# and let the service-principal env vars (DATABRICKS_HOST/CLIENT_ID/CLIENT_SECRET)
# take over — same identity the bundle CD uses.
provider "databricks" {
  profile = var.databricks_profile
}
