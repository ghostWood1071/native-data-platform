from pyspark.sql import DataFrame
from src.core.interfaces import BaseWriter

class ConsoleWriter(BaseWriter):
    def overwrite_partition(self, df: DataFrame):
        df.show(truncate=False)

    def overwrite(self, df: DataFrame):
        df.show(truncate=False)

    def append(self, df: DataFrame):
        df.show(truncate=False)


class IcebergWriter(BaseWriter):
    def overwrite_partition(self, df: DataFrame):
        table_name = self.config.get("table_name")
        db_name = ".".join(self.config.get("table_name").split(".")[:-1])
        partition_cols = self.config.get("partition_by")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        df.createOrReplaceTempView(f"tmp_{table_name.split('.')[-1]}")
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_{table_name.split('.')[-1]} limit 0"
        )
        df.writeTo(table_name).partitionedBy(*partition_cols).overwritePartitions()

    def overwrite(self, df: DataFrame):
        table_name = self.config.get("table_name")
        db_name = ".".join(self.config.get("table_name").split(".")[:-1])
        partition_cols = self.config.get("partition_by")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        df.createOrReplaceTempView(f"tmp_{table_name.split('.')[-1]}")
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_{table_name.split('.')[-1]} limit 0"
        )
        self.spark.sql(f"DELETE FROM {table_name}")
        df.writeTo(table_name).partitionedBy(*partition_cols).append()

    def append(self, df: DataFrame):
        table_name = self.config.get("table_name")
        db_name = ".".join(self.config.get("table_name").split(".")[:-1])
        partition_cols = self.config.get("partition_by")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        df.createOrReplaceTempView(f"tmp_{table_name.split('.')[-1]}")
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_{table_name.split('.')[-1]} limit 0"
        )
        df.writeTo(table_name).partitionedBy(*partition_cols).append()
