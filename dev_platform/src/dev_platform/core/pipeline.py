import logging

from dev_platform.core.base import BaseReader, BaseTransformer, BaseWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


class Pipeline:
    """Template Method: a ordem read -> transform* -> write é fixa;
    o que varia em cada chamada são as estratégias injetadas no construtor.
    """

    def __init__(
        self,
        name: str,
        reader: BaseReader,
        transformers: list[BaseTransformer],
        writer: BaseWriter,
    ):
        self.name = name
        self.reader = reader
        self.transformers = transformers
        self.writer = writer
        self.logger = logging.getLogger(f"pipeline.{name}")

    def run(self):
        self.logger.info("Iniciando pipeline")
        df = self.reader.read()

        # Chain of Responsibility: cada transformer recebe o df do anterior e passa adiante.
        for transformer in self.transformers:
            self.logger.info("Aplicando %s", transformer.__class__.__name__)
            df = transformer.transform(df)

        self.writer.write(df)
        self.logger.info("Pipeline concluído")
        return df
