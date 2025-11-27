from pyspark.sql import DataFrame
from core.reader.base_reader import BaseReader
from datetime import datetime


class JDBCReader(BaseReader):
    def load(self) -> DataFrame:
        reader = (
            self.spark.read.format("jdbc")
            .option("url", self.config.get("url"))

        )
        if self.config.get("table"):
            reader = reader.option("dbtable", self.config["table"])
        if self.config.get("properties"):
            config = self.config.get("properties")
            for k, v in config.items():
                reader = reader.option(k, v)
        if self.config.get("query"):
            reader = reader.option("query", self.config["query"])
        return reader.load()
