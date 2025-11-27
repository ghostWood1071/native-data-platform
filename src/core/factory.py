import importlib
import json
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta


class Factory:
    @staticmethod
    def import_module(module_name):
        module_path = ".".join(module_name.split(".")[:-1])
        class_name = module_name.split(".")[-1]
        module = importlib.import_module(module_path)
        class_name_obj = getattr(module, class_name)
        return class_name_obj

    @staticmethod
    def read_config(json_path):
        with open(json_path, mode="r") as f:
            data = json.loads(f.read())
            return data

    @staticmethod
    def create_spark(config:dict):
        spark_config = config.get("spark-conf")
        engine_config = config.get("engine-conf")
        spark_builder = SparkSession.builder
        for config_name, config_val in spark_config.items():
            spark_builder = spark_builder.config(config_name, config_val)
        for config_name, config_val in engine_config.items():
            if config_val:
                config_func = getattr(spark_builder, config_name)
                config_func()
        return spark_builder.getOrCreate()

    @staticmethod
    def extract(spark: SparkSession, config: dict, run_data_date) -> dict:
        reader_class_name = f"src.core.readers.{config.get('reader')}"
        reader_class = Factory.import_module(reader_class_name)
        reader = reader_class(spark, config, run_data_date)
        df = reader.load()
        return df

    @staticmethod
    def transform(spark: SparkSession, config:dict, dfs: dict, run_data_date) -> dict:
        transform_class_name = f"src.core.transformers.{config.get('transformer')}"
        transform_config = config.get("transformation")
        transform_class = Factory.import_module(transform_class_name)
        transformer = transform_class(spark, transform_config, dfs, run_data_date)
        df_results = transformer.transform()
        return df_results

    @staticmethod
    def load( spark: SparkSession, df: DataFrame, config: dict, run_data_date) -> None:
        writer_class_name = f"src.core.writers.{config.get('writer')}"
        writer_class = Factory.import_module(writer_class_name)
        writer = writer_class(spark, config, run_data_date)
        write_method = getattr(writer, config.get("mode"))
        return write_method(df)



