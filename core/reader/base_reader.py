from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


class BaseReader:
    def __init__(self, spark: SparkSession, config: dict, run_data_date):
        self.spark = spark
        self.config = config
        self.run_data_date = run_data_date

    def load(self) -> DataFrame:
        raise NotImplementedError()

    @staticmethod
    def execute(read_class, spark: SparkSession, config: dict, run_data_date) -> dict:
        reader = read_class(spark, config, run_data_date)
        df = reader.load()
        return df
