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
  description = "Application ID of the sp_databricks_bundle service principal"
  type        = string
  default     = "ce5fe75d-894b-4176-ac65-d25eb9569e95"
}

variable "landing_volume_path" {
  description = "Path to the landing data in Databricks. Mirroring the data stored in S3"
  type        = string
  default     = "example.raw.landing_data"
}