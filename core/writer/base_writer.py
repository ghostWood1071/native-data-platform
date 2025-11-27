from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class BaseWriter:
    def __init__(self, spark: SparkSession, config: dict, run_data_date):
        self.spark = spark
        self.config = config
        self.run_data_date = run_data_date

    def append(self, df: DataFrame):
        raise NotImplementedError()

    def overwrite(self, df: DataFrame):
        raise NotImplementedError()

    def overwrite_partition(self, df: DataFrame):
        raise NotImplementedError()

    def upsert(self, df: DataFrame):
        raise NotImplementedError

    @staticmethod
    def execute(
        writer_class, spark: SparkSession, df: DataFrame, config: dict, run_data_date
    ):
        writer = writer_class(spark, config, run_data_date)
        write_method = getattr(writer, config.get("mode"))
        return write_method(df)
