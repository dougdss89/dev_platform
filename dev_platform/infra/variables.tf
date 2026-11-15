variable "databricks_profile" {
  description = "Databricks CLI profile used for local runs (empty in CI -> SP env vars)."
  type        = string
  default     = "community" # same profile the bundle's dev target uses
}

variable "catalog" {
  description = "Unity Catalog catalog to manage."
  type        = string
  default     = "advworks_dev"
}

variable "prd_catalog" {
  description = "Data catalog used for production deployments"
  type        = string
  default     = "advworks_prd"
}

variable "sp_application_id" {
  description = "Application ID of the service principal granted access (dev default; prd: -var sp_application_id=43bd6c8a-108b-4a96-a8af-aea768ab292e, sp_databricks_bundle_prd)"
  type        = string
  default     = "ce5fe75d-894b-4176-ac65-d25eb9569e95"
}

variable "landing_volume_path" {
  description = "Path to the landing data in Databricks. Mirroring the data stored in S3"
  type        = string
  default     = "example.raw.landing_data"
}

variable "create_landing_volume" {
  description = "Create the managed landing volume (true only where it does not exist yet, i.e. prd)."
  type        = bool
  default     = false
}
