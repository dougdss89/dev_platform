"""Address standardization — genérico, reusável por qualquer dimensão com endereço.

Expande abreviações de logradouro e separa número/texto. Serve store, customer,
etc.; a coluna de entrada é parâmetro. Log fica a cargo do LoggingTransformer.
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract, regexp_replace, trim

from dev_platform.core.common.transformer import BaseTransformer

# Abreviação (regex, case-insensitive, word-boundary) -> forma completa.
# A ORDEM importa: aplicadas de cima pra baixo. É dado, não código — fácil revisar/estender.
STREET_ABBREVIATIONS = {
    r"(?i)\bln\b": "Lane",
    r"(?i)\brd\b": "Road",
    r"(?i)\bdr\b": "Drive",
    r"(?i)\bct\b": "Court",    
    r"(?i)\bst\b": "Street",
    r"(?i)\bblvd\b": "Boulevard",
    r"(?i)\bave\b": "Avenue",
    r"(?i)\bpk\b": "Parkway",
    r"(?i)\bterr\b": "Terrace",
    r"(?i)\bpl\b": "Plaza",
    r"(?i)\bmt\b": "Mountain",
    r"(?i)\bn\.\b": "North",
}


class StandardizeAddressTransformer(BaseTransformer):
    """Expande abreviações de logradouro e separa número/texto do endereço.

    Gera duas colunas novas:
      * ``addressnumber`` — primeiro número encontrado;
      * ``custaddress``   — texto do endereço (só letras), sem número, trimado.
    """

    def __init__(self, column: str = "addressline1"):
        self._column = column

    def transform(self, df: DataFrame) -> DataFrame:
        # Dobra os 12 regexp_replace numa única expressão -> um withColumn só.
        expanded = reduce(
            lambda c, kv: regexp_replace(c, kv[0], kv[1]),
            STREET_ABBREVIATIONS.items(),
            col(self._column),
        )

        return (
            df.withColumn(self._column, expanded)
            .withColumn("addressnumber", regexp_extract(self._column, r"(\d+)", 1))
            .withColumn(
                "custaddress",
                trim(regexp_replace(regexp_replace(self._column, r"\d+", ""), r"[^a-zA-Z ]", "")),
            )
        )

    