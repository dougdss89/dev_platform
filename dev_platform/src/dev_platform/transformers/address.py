"""Address standardization — generic and reusable across any dimension with an address.

Expands street type abbreviations and splits street number from street name.
Works for store, customer, etc.; the input column is configurable. Logging is
handled by LoggingTransformer.
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract, regexp_replace, trim

from dev_platform.core.common.transformer import BaseTransformer

# Abbreviation (regex, case-insensitive, word-boundary) -> full form.
# ORDER matters: applied top to bottom. Treated as data, not code — easy to review/extend.
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
    """Expands street type abbreviations and separates street number from street text.

    Generates two new columns:
      * ``addressnumber`` — first number found;
      * ``custaddress``   — address text (letters only), without numbers, trimmed.
    """

    def __init__(self, column: str = "addressline1"):
        self._column = column

    def transform(self, df: DataFrame) -> DataFrame:
        # Folds all 12 regexp_replace calls into a single expression -> one withColumn call.
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

    