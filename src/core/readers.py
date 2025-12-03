from pyspark.sql import DataFrame
from src.core.interfaces import BaseReader


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
    
class DeltaReader(BaseReader):
    def load(self) -> DataFrame:
        table = self.config.get("table")
        query = self.config.get("query")
        df = None
        if query:
            if "${run_date}" in query:
                query = query.replace("${run_date}", self.run_data_date)
            df = self.spark.sql(query)
        if not query:
            df = self.spark.table(table)
        return df

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