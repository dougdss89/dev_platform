# dev_platform

Databricks Asset Bundle (DAB) implementing a Medallion architecture
(`raw` → `trusted` → `enriched`) on Unity Catalog and serverless compute for the
AdventureWorks dataset. Transformations follow a class-based
**Reader / Transformer / Writer** pattern, are packaged as a wheel, and are
orchestrated by layer-oriented Lakeflow jobs with CI/CD on GitHub Actions.

**Status:** the `customer` domain runs end-to-end in production —
`advworks_prd.raw.customer` → `advworks_prd.trusted.customer` — deployed through
CI/CD as the `sp_databricks_bundle` service principal.

## Architecture

The engine is a single `Pipeline` (Template Method) that always runs
`read → transform* → write`. What varies per run are the three strategies a
**factory** injects, so adding a layer or a domain means writing a new factory,
not a new engine.

* **Readers / Writers** (`core/common/`): `TableReader`, `VolumeParquetReader`;
  `TableWriter`, `TableMergeWriter` (idempotent upsert).
* **Transformers**: generic and reusable in `transformers/`; domain-specific in
  `domains/<domain>/`.
* **Factories** (`domains/<domain>/pipeline.py`): one per layer transition —
  e.g. `build_customer_raw_ingestion` (landing→raw) and
  `build_customer_trusted` (raw→trusted).

Two levels of ordering:

* **Intra-layer** — the transformer list inside a factory (generic first, then
  domain-specific rules).
* **Inter-layer** — `depends_on` in the `medallion` orchestrator job
  (`raw` → `trusted` → `enriched`).

| Layer | Source | Responsibility |
|-------|--------|----------------|
| `raw` | landing volume (parquet) | ingestion metadata/audit only — the auditable system-of-record |
| `trusted` | `raw` | generic normalization + domain rules, written via idempotent MERGE |
| `enriched` | `trusted` | business aggregates and joins *(planned)* |

## Project structure

* `databricks.yml`: Minimal root bundle configuration that includes settings from `conf/` and `jobs/`.
* `conf/`: Bundle configuration files:
  * `conf/variables.yml`: Variable declarations and defaults (catalog per target, service-principal Application ID, …).
  * `conf/tags.yml`: Bundle-wide resource tags (`presets.tags`).
  * `conf/data_tags.yml`: Data governance taxonomy and dataset tags for Unity Catalog.
  * `conf/targets/`: Per-environment targets (`dev.yml` uses a profile, `prod.yml` uses service-principal auth).
* `jobs/workflows/`: Lakeflow jobs organized by layer:
  * `raw/ingestion.job.yml` — landing→raw job (`raw-ingestion`), one task per domain.
  * `trusted/build.job.yml` — raw→trusted job (`trusted-build`), one task per domain.
  * `medallion.job.yml` — orchestrator chaining the layer jobs via `run_job_task`.
* `src/dev_platform/`: Python package built as a wheel artifact:
  * `core/common/`: `Pipeline` (Template Method), `BaseReader`/`BaseTransformer`/`BaseWriter`, table/column validators, logger.
  * `transformers/`: Generic, cross-domain transformers (cleaning, casting, filtering, address standardization, normalization, audit/metadata).
  * `domains/`: Domain-specific transformers and pipeline factories (currently `domains/customer/`).
  * `entrypoints/`: Thin drivers wired to `python_wheel_task` (`customer-ingest`, `customer-trusted`).
* `tests/`: Unit (transformers), composition/wiring, and schema-contract tests using local PySpark.
* `fixtures/`: Data fixtures used for testing.

## CI/CD

| Workflow | Trigger | Does |
|----------|---------|------|
| `.github/workflows/ci.yml` | PR → `main` | `bundle validate -t prod` + `pytest` (isolated `test` group with vanilla PySpark; `setup-java` provides the JDK the local SparkSession needs) |
| `.github/workflows/cd.yml` | push → `main` | `bundle deploy -t prod` as the service principal (OAuth M2M) |

Auth in CI/CD uses repository secrets `DATABRICKS_CLIENT_ID` and
`DATABRICKS_CLIENT_SECRET`. **CD deploys but does not run the jobs** — trigger
the first run manually (or add a `bundle run` step once you want it automated).

## Getting started

Develop with the CLI, or in VS Code / Cursor with the Databricks extension
(see https://docs.databricks.com/dev-tools/vscode-ext.html).

Install dependencies with **uv** (https://docs.astral.sh/uv/):

* `uv sync --dev` — development dependencies (includes Databricks Connect).
* Generate the editor's DABs JSON Schema once after cloning (gitignored; needs
  the `redhat.vscode-yaml` extension, already in `.vscode/extensions.json`):
  ```
  $ databricks bundle schema > .vscode/bundle_config_schema.json
  ```

## CLI usage

1. Authenticate (once): `databricks auth login --host <workspace-url> --profile <name>`.
2. Validate: `databricks bundle validate -t dev` (or `-t prod`).
3. Deploy a dev copy: `databricks bundle deploy -t dev` (schedules/triggers are paused in dev mode).
4. Run the end-to-end pipeline: `databricks bundle run medallion -t dev`
   (or a single layer: `raw_ingestion` / `trusted_build`).

Production deploys normally happen through CI/CD; to deploy prod manually you
need the service-principal credentials in the environment.

## Testing

Run the unit/contract/wiring tests in the isolated `test` group (vanilla
PySpark, without Databricks Connect, which only supports remote sessions):

```
$ uv run --only-group test pytest
```

The suite covers the three cheap data-engineering test types: transformer
**unit** tests, trusted-layer **schema contract**, and factory **composition**.

## Roadmap

* [ ] Wire `DeriveCustomerType` / `DeriveCustomerIncome` into the customer trusted factory (after fixing their known bugs).
* [ ] `enriched` layer (trusted → enriched): factory, `enriched-build` job, and an orchestrator task.
* [ ] Additional domains (product, store, …) reusing the same factory template.
* [ ] Declare Unity Catalog schemas as bundle resources (`resources/schemas.yml`) so `deploy` provisions them instead of manual `CREATE SCHEMA`.
* [ ] Apply the `conf/data_tags.yml` governance taxonomy as Unity Catalog tags.
* [ ] Runtime data-quality checks via the existing validators; add a Delta MERGE idempotency integration test.
* [ ] Incremental landing→raw ingestion (currently a full load).
