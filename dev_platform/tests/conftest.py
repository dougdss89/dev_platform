"""Test fixtures — a shared local SparkSession (session-scoped)."""

import shutil
import tempfile
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    warehouse = tempfile.mkdtemp(prefix="spark-warehouse-")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("dev_platform-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
