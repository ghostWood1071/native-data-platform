import uuid

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

class DeltaWriter(BaseWriter):  
    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return f"`{identifier.replace('`', '``')}`"

    @classmethod
    def _quote_table_name(cls, table_name: str) -> str:
        return ".".join(cls._quote_identifier(part) for part in table_name.split("."))

    @staticmethod
    def _quote_string(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _ensure_table(self, df: DataFrame, mode: str):
        fqn_table_name = self.config.get("table_name")
        table_parts = fqn_table_name.rsplit(".", 2)
        table_name = table_parts[-1]
        db_name = table_parts[-2]
        partition_cols = self.config.get("partition_by", [])
        path = self.config.get("path", f"s3a://warehouse/{db_name}/{table_name}")
        quoted_table_name = self._quote_table_name(fqn_table_name)
        schema_ddl = ",\n".join(
            f"{self._quote_identifier(field.name)} {field.dataType.simpleString()}"
            for field in df.schema.fields
        )
        partition_ddl = ""
        if partition_cols:
            quoted_partitions = ", ".join(
                self._quote_identifier(column) for column in partition_cols
            )
            partition_ddl = f"\nPARTITIONED BY ({quoted_partitions})"

        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {self._quote_identifier(db_name)}"
        )
        if mode == "overwrite" and self.config.get("first", False):
            self.spark.sql(f"DROP TABLE IF EXISTS {quoted_table_name}")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {quoted_table_name} (
                {schema_ddl}
            )
            USING DELTA{partition_ddl}
            LOCATION {self._quote_string(path)}
        """)

    def _write_table(self, df: DataFrame, mode: str):
        self._ensure_table(df, mode)
        quoted_table_name = self._quote_table_name(self.config.get("table_name"))
        quoted_columns = ", ".join(
            self._quote_identifier(column) for column in df.columns
        )
        temp_view = f"_delta_writer_{uuid.uuid4().hex}"
        quoted_temp_view = self._quote_identifier(temp_view)
        insert_mode = "OVERWRITE TABLE" if mode == "overwrite" else "INTO TABLE"

        df.createOrReplaceTempView(temp_view)
        try:
            self.spark.sql(f"""
                INSERT {insert_mode} {quoted_table_name} ({quoted_columns})
                SELECT {quoted_columns}
                FROM {quoted_temp_view}
            """)
        finally:
            self.spark.catalog.dropTempView(temp_view)

    def overwrite_partition(self, df: DataFrame):
        self._write_table(df, "overwrite")

    def overwrite(self, df: DataFrame):
        self._write_table(df, "overwrite")

    def append(self, df: DataFrame):
        self._write_table(df, "append")

    def upsert(self, df: DataFrame):
        table_name = self.config.get("table_name")
        db_name = ".".join(self.config.get("table_name").split(".")[:-1])
        primary_key = self.config.get("primary_key")
        change_tracking_cols = self.config.get("change_tracking_column")

        merge_condition = " AND ".join([f"t.{col} = s.{col}" for col in primary_key])
        update_condition = " OR ".join([f"t.{col} <> s.{col}" for col in change_tracking_cols])
        set_expr = ", ".join([f"{c} = s.{c}" for c in change_tracking_cols])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"s.{c}" for c in df.columns])
        
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        df.createOrReplaceTempView(f"tmp_{table_name.split('.')[-1]}")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            AS SELECT * FROM tmp_{table_name.split('.')[-1]} LIMIT 0
        """)
        self.spark.sql(f"""
            MERGE INTO {table_name} t
            USING tmp_{table_name.split('.')[-1]} s ON {merge_condition}
            WHEN MATCHED AND {update_condition} THEN UPDATE SET {set_expr}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})  
        """)
