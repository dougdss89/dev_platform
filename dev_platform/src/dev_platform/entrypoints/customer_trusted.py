"""Entrypoint (composition root) — customer raw -> trusted.

Mesma ideia do customer_ingest: driver fino, zero regra. Troca só qual factory
é montada. O DABs chama este entry point depois do landing->raw (depends_on).
"""

import sys

from pyspark.sql import SparkSession

from dev_platform.domains.customer.pipeline import build_customer_trusted


def main() -> None:
    catalog = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    build_customer_trusted(spark, catalog).run()


if __name__ == "__main__":
    main()
