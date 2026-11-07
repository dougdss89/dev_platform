# dev_platform

A Databricks platform built with **Declarative Automation Bundles (DABs)** and
**Terraform**, implementing a Medallion architecture
(`raw` → `trusted` → `enriched`) on Unity Catalog and serverless compute for the
AdventureWorks dataset.

Transformations follow a class-based **Reader / Transformer / Writer** pattern,
are packaged as a Python wheel, and are orchestrated by layer-oriented Lakeflow
jobs.

## Status

| Area | State |
|------|-------|
| `customer` domain, landing → raw → trusted | Implemented (jobs + orchestrator) |
| Synthetic seed job (writes to the landing volume) | Implemented, schedule ships `PAUSED` |
| Unity Catalog infra (schemas, grants) as Terraform | Implemented in `infra/`, local state |
| Governance tags (`conf/data_tags.yml` + `apply-tags`) | Entrypoint implemented, not yet scheduled |
| `enriched` layer | Planned |
| Other domains | Planned |
| CI/CD workflows | Planned (to be committed) |

## Architecture

The engine is a single `Pipeline` (Template Method) that always runs
`read → transform* → write`. What varies per run are the three strategies a
**factory** injects, so adding a layer or a domain means writing a new factory,
not a new engine.

* **Readers / Writers** (`core/common/`): `TableReader`, `VolumeParquetReader`;
  `TableWriter`, `TableMergeWriter` (idempotent upsert).
* **Transformers**: generic and reusable in `transformers/`; domain-specific in
  `domains/<domain>/`.
* **Factories** (`domains/<domain>/pipeline.py`): one per layer transition,
  e.g. `build_customer_raw_ingestion` (landing → raw) and
  `build_customer_trusted` (raw → trusted).

Two levels of ordering:

* **Intra-layer**: the transformer list inside a factory (generic first, then
  domain rules).
* **Inter-layer**: `depends_on` in the orchestrator job
  (`seed` → `raw` → `trusted` → `enriched`).

| Layer | Source | Responsibility |
|-------|--------|----------------|
| `raw` | landing volume (parquet) | Ingestion metadata and audit only. The auditable system of record. |
| `trusted` | `raw` | Type casting, normalization, quality filters, dedup, then idempotent `MERGE`. |
| `enriched` | `trusted` | Business aggregates and joins *(planned)*. |

### Ownership split

| Owner | Manages |
|-------|---------|
| **Terraform** (`infra/`) | Catalog-level platform: schemas, service principal, grants |
| **DABs** (bundle) | Workloads: jobs and the wheel artifact |

A resource is managed by exactly one of them. See [`infra/README.md`](infra/README.md).

## Project structure

```
databricks.yml            Minimal bundle root; includes conf/ and jobs/
conf/                     variables, tags, per-environment targets, data-governance taxonomy
infra/                    Terraform for Unity Catalog infra
jobs/
  seed/                   Synthetic data writer (landing volume)
  workflows/
    raw/                  landing → raw jobs
    trusted/              raw → trusted jobs
    customer_orchestrator.job.yml   seed → raw → trusted
src/dev_platform/
  core/common/            Pipeline, base contracts, readers, writers, validators, logger
  transformers/           Generic, cross-domain transformers
  domains/customer/       Customer-specific transformers and pipeline factories
  entrypoints/            Thin drivers wired to python_wheel_task
  governance/             Unity Catalog tagging
  seeding/                Synthetic data generation
tests/                    Unit, composition and schema-contract tests (local PySpark)
```

Entry points (declared in `pyproject.toml`): `customer-ingest`,
`customer-trusted`, `seed-customer`, `apply-tags`.

## Environments

| Target | Catalog | Mode | Auth |
|--------|---------|------|------|
| `dev` (default) | `advworks_dev` | `development` (schedules paused) | CLI profile |
| `prod` | `advworks_prd` | `production` | Service principal (OAuth M2M) |

## Getting started

Install dependencies with [uv](https://docs.astral.sh/uv/):

```bash
uv sync --dev
```

Deploy and run in dev:

```bash
databricks auth login --host <workspace-url> --profile <name>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run customer_orchestrator -t dev
```

Production deploys are meant to run from CI/CD as the service principal.

## Testing

Tests run in an isolated `test` group with vanilla PySpark (Databricks Connect
only supports remote sessions):

```bash
uv run --only-group test pytest
```

Coverage: transformer **unit** tests, trusted-layer **schema contract**, and
factory **composition**.

## Roadmap

Planned deploys, in the order they are intended to ship. Each deploy is a
small, independently releasable slice. Scope may change as work progresses.

### Deploy 1: Delivery foundation

Make every later deploy reproducible and safe.

- [ ] Commit GitHub Actions workflows: CI on pull requests
      (`bundle validate -t prod` + `pytest`) and CD on push to `main`
      (`bundle deploy -t prod` as the service principal).
- [ ] Remove the leftover `pyproject.toml.bak` and tidy the repo root.

### Deploy 2: Customer trusted completeness

- [ ] Wire `DeriveCustomerType` and `DeriveCustomerIncome` into
      `build_customer_trusted` (after fixing their known bugs).
- [ ] Extend the schema-contract test to cover the derived columns.

### Deploy 3: Data quality and idempotency

- [ ] Runtime data-quality checks using the existing table/column validators.
- [ ] Delta `MERGE` idempotency integration test.
- [ ] Incremental landing → raw ingestion (currently a full load).

### Deploy 4: Governance

- [ ] Apply the `conf/data_tags.yml` taxonomy as Unity Catalog tags through a
      job task (the `apply-tags` entrypoint already exists).
- [ ] Column-level PII tagging.

### Deploy 5: `enriched` layer

- [ ] First `trusted → enriched` factory for `customer`.
- [ ] `enriched` job and an orchestrator task that depends on `trusted`.
- [ ] Decide which engine owns aggregation: imperative jobs (current) or
      Lakeflow Declarative Pipelines.

### Deploy 6: More domains

- [ ] `product`, `store`, and further AdventureWorks domains, reusing the
      factory template (one raw task and one trusted task per domain, running
      in parallel).

### Deploy 7: Infra hardening (Terraform)

- [ ] Move Terraform state to a remote backend before any team use.
- [ ] Run `terraform plan` / `apply` in CI ahead of `bundle deploy`, as the
      service principal.

### Ongoing

- [ ] Keep the seed job schedule `PAUSED` by default; enable the 30-minute
      cadence only when actively training, to avoid unnecessary cost.

## License

Not specified yet.
