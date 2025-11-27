from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


class BaseReader:
    def __init__(self, spark: SparkSession, config: dict, run_data_date):
        self.spark = spark
        self.config = config
        self.run_data_date = run_data_date

    def extract(self) -> DataFrame:
        raise NotImplementedError()


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


class BaseTransformer:
    def __init__(self, spark: SparkSession, config: dict, dfs:dict ,run_data_date):
        self.spark = spark
        self.config = config
        self.run_data_date = run_data_date
        self.dfs = dfs

    def transform(self):
        raise NotImplementedError()

