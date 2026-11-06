"""Entrypoint — seed one small customer batch into the landing volume.

One job run = one batch; the job's schedule provides the "keeps writing" cadence.
Invoked as a python_wheel_task with positional parameters:
    [landing_catalog, batch_size]   # both optional: default "example", 8
"""

import sys

from pyspark.sql import SparkSession

from dev_platform.seeding.customer_seed import seed_once


def main() -> None:
    landing_catalog = sys.argv[1] if len(sys.argv) > 1 else "example"
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    spark = SparkSession.builder.getOrCreate()
    volume_path = f"/Volumes/{landing_catalog}/raw/landing_data/customer"
    base_id, n = seed_once(spark, volume_path, batch_size)
    print(f"Seeded {n} rows into {volume_path} (base_id={base_id})")


if __name__ == "__main__":
    main()
