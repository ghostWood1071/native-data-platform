from pyspark.sql import DataFrame

from core.reader.base_reader import BaseReader


class IcebergReader(BaseReader):
    def load(self) -> DataFrame:
        table = self.config.get("table")
        query = self.config.get("query")
        df = None
        if query:
            df = self.spark.sql(query)
        if not query:
            df = self.spark.table(table)
        return df
