# infra/ — platform & governance as Terraform

Terraform owns the **platform/UC infra** (catalog, schemas, volumes, service
principal, grants). The **DABs bundle** (parent dir) owns the **workloads**
(jobs + wheel). A resource is managed by **one** of them, never both.

## Local usage

```bash
cd infra
terraform init                 # downloads the databricks provider, writes the lock
terraform plan                 # shows what would change (read-only)
terraform apply                # creates/updates (asks for confirmation)
terraform destroy              # tears the managed resources down
```

Auth uses the Databricks CLI profile in `var.databricks_profile` (default `doug`).

## Roadmap for this dir

1. Replace the demo schema with the real `raw` / `trusted` / `enriched` schemas,
   and remove them from the bundle's `conf/schemas.yml`.
2. Add the service principal + UC grants (replacing the manual `GRANT` SQL).
3. Move state to a remote backend before any team use.
4. Run `terraform plan`/`apply` in CI (GitHub Actions) ahead of `bundle deploy`,
   authenticating as the service principal (env vars, no profile).
