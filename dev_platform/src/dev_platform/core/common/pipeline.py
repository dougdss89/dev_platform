import logging

from dev_platform.core.common.reader import BaseReader
from dev_platform.core.common.transformer import BaseTransformer
from dev_platform.core.common.writers import BaseWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


class Pipeline:
    """Template Method: the read -> transform* -> write order is fixed;
    what varies per call are the strategies injected into the constructor.
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
        self.logger.info("Starting pipeline")
        df = self.reader.read()

        # Chain of Responsibility: each transformer receives the DataFrame from the previous one and passes it along.
        for transformer in self.transformers:
            self.logger.info("Applying %s", transformer.__class__.__name__)
            df = transformer.transform(df)

        self.writer.write(df)
        self.logger.info("Pipeline completed")
        return df
