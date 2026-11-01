"""Entrypoint (composition root) — customer landing -> raw.

Camada "suja" do padrão: é o ÚNICO lugar que resolve a SparkSession e lê
parâmetros do job. Nenhuma regra de negócio aqui — só monta a factory e dispara
``.run()``. O DABs chama este entry point via ``python_wheel_task``, passando o
catálogo como parâmetro posicional (sys.argv[1]).
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
