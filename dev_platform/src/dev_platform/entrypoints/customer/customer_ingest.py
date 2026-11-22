"""Entrypoint (composition root) — customer landing -> raw.

"Impure" edge of the pattern: this is the ONLY place that resolves the
SparkSession and reads job parameters. No business logic lives here — it only
wires the factory and triggers ``.run()``. DABs invokes this entry point via
``python_wheel_task``, passing the catalog as a positional argument (sys.argv[1]).
"""

import sys

from pyspark.sql import SparkSession

from dev_platform.domains.customer.pipeline import build_customer_raw_ingestion


def main() -> None:
    catalog = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    build_customer_raw_ingestion(spark, catalog).run()


if __name__ == "__main__":
    main()
