# Terraform root for platform/UC infra (catalog, schemas, SP, grants).
# DABs still owns the workloads (jobs/wheel) — keep the two from managing the
# same resource.
locals {
  catalog = terraform.workspace == "prod" ? "advworks_prd" : "advworks_dev"
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
