from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from dev_platform.core.common.logger import get_logger


class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError


class CompositeTransformer(BaseTransformer):
    """Runs multiples transformations in sequence"""

    def __init__(self, transformers: list[BaseTransformer]):
        self._transformers = transformers

    def transform(self, df: DataFrame) -> DataFrame:
        for transformer in self._transformers:
            df = transformer.transform(df)

        return df


class LoggingTransformer(BaseTransformer):
    def __init__(self, transformer: BaseTransformer):
        self._transformer = transformer
        self._logger = get_logger(self.__class__.__name__)

    def transform(self, df: DataFrame) -> DataFrame:
        self._logger.info(f"Applying transformations via {type(self._transformer).__name__}")

        df = self._transformer.transform(df)

        self._logger.info(f"Fineshed transformation via {type(self._transformer).__name__}")

        return df
