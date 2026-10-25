from pyspark.sql import DataFrame

from dev_platform.core.base import BaseTransformer


class DropColumnsTransformer(BaseTransformer):
    """Remove colunas por nome. Genérico: usado para tirar metadados de ingestão
    (inserted_date, eltversion, _rescued_data...) comuns a qualquer tabela raw.
    """

    def __init__(self, columns: list[str]):
        self.columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        return df.drop(*self.columns)
