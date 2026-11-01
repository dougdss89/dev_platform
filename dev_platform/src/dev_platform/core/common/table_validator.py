"""MERGE preconditions for Unity Catalog targets.

Each validator checks one precondition against a fully-qualified table name
(``<catalog>.<schema>.<table>``) and raises on failure / returns None on success.
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class TableValidator(ABC):
    """One subclass per MERGE precondition."""

    @abstractmethod
    def validate(self, spark: SparkSession, table: str, df: DataFrame, merge_keys: list[str]) -> None:
        raise NotImplementedError


class TableExistsValidator(TableValidator):
    """MERGE INTO (unlike append) won't create the target — fail fast with a clear message."""

    def validate(self, spark: SparkSession, table: str, df: DataFrame, merge_keys: list[str]) -> None:
        if not spark.catalog.tableExists(table):
            raise ValueError(f"Cannot MERGE into {table}: table does not exist yet")


class NoDuplicateMergeKeysValidator(TableValidator):
    """Delta's MERGE fails at runtime if two source rows match the same target row — catch it first."""

    def validate(self, spark: SparkSession, table: str, df: DataFrame, merge_keys: list[str]) -> None:
        if not merge_keys:
            return None
        duplicates = df.groupBy(*merge_keys).count().filter("count > 1")
        if duplicates.count() > 0:
            raise ValueError(f"Duplicate merge keys found for {table}: {merge_keys}")
        return None


class CompositeValidator(TableValidator):
    """Runs several validators in sequence — the writer stays agnostic of how many exist."""

    def __init__(self, validators: list[TableValidator]):
        self._validators = validators

    def validate(self, spark: SparkSession, table: str, df: DataFrame, merge_keys: list[str]) -> None:
        for validator in self._validators:
            validator.validate(spark, table, df, merge_keys)
