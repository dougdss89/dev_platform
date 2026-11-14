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
  # TODO: set to the SP created in the new prod account (pass via -var or a tfvars file).
  default     = "00000000-0000-0000-0000-000000000000"
}

variable "landing_volume_path" {
  description = "Path to the landing data in Databricks. Mirroring the data stored in S3"
  type        = string
  default     = "example.raw.landing_data"
}