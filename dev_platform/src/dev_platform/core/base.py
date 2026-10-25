from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseReader(ABC):
    """Strategy: qualquer fonte de dados, desde que saiba ler() um DataFrame."""

    @abstractmethod
    def read(self) -> DataFrame: ...


class BaseTransformer(ABC):
    """Strategy: qualquer transformação, desde que receba e devolva um DataFrame."""

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame: ...


class BaseWriter(ABC):
    """Strategy: qualquer destino, desde que saiba persistir um DataFrame."""

    @abstractmethod
    def write(self, df: DataFrame) -> None: ...
