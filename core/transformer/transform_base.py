from pyspark.sql import SparkSession


class TransformBase:
    def __init__(self, spark: SparkSession, dfs: dict, run_data_date):
        self.spark = spark
        self.dfs = dfs
        self.run_data_date = run_data_date

    def transform(self):
        raise NotImplementedError()

    @staticmethod
    def execute(transform_class, spark: SparkSession, dfs: dict, run_data_date) -> dict:
        transformer = transform_class(spark, dfs, run_data_date)
        df_results = transformer.transform()
        return df_results
