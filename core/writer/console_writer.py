from pyspark.sql import DataFrame
from core.writer.base_writer import BaseWriter


class ConsoleWriter(BaseWriter):
    def overwrite_partition(self, df: DataFrame):
        df.show(truncate=False)

    def overwrite(self, df: DataFrame):
        df.show(truncate=False)

    def append(self, df: DataFrame):
        df.show(truncate=False)
