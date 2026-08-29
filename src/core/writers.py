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
    """Write through the catalog table so Spark can report end-to-end lineage.

    A path-based ``DataFrameWriter.save`` does not identify the logical output
    table.  ``saveAsTable`` keeps the source DataFrame plan and targets the fully
    qualified catalog table in the same write operation.
    """

    @staticmethod
    def _quote_identifier(identifier):
        return f"`{identifier.replace('`', '``')}`"

    @staticmethod
    def _quote_string(value):
        return value.replace("'", "''")

    def _is_delta_table(self, path):
        return self.spark._jvm.io.delta.tables.DeltaTable.isDeltaTable(
            self.spark._jsparkSession, path
        )

    def _table_details(self):
        parts = self.config["table_name"].split(".")
        if len(parts) != 3 or any(not part for part in parts):
            raise ValueError("Delta table_name must be catalog.database.table")

        quoted_parts = [self._quote_identifier(part) for part in parts]
        table_name = ".".join(quoted_parts)
        database_name = ".".join(quoted_parts[:2])
        default_path = f"s3a://warehouse/{parts[1]}/{parts[2]}"
        path = self.config.get("path", default_path)
        return table_name, database_name, path

    def _ensure_table(self, df, recreate):
        table_name, database_name, path = self._table_details()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        if recreate and self.config.get("first", False):
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        quoted_path = self._quote_string(path)
        if self._is_delta_table(path):
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"USING DELTA LOCATION '{quoted_path}'"
            )
            return table_name

        columns = ", ".join(
            f"{self._quote_identifier(field.name)} {field.dataType.simpleString()}"
            for field in df.schema.fields
        )
        partition_cols = self.config.get("partition_by", [])
        partition_clause = ""
        if partition_cols:
            quoted_partitions = ", ".join(
                self._quote_identifier(column) for column in partition_cols
            )
            partition_clause = f" PARTITIONED BY ({quoted_partitions})"

        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}) "
            f"USING DELTA{partition_clause} LOCATION '{quoted_path}'"
        )
        return table_name

    def _write(self, df, mode):
        self._ensure_table(
            df, recreate=self.config.get("first", False)
        )
        writer = (
            df.write.format("delta")
            .mode(mode)
            .option("path", self._table_details()[2])
        )
        partition_cols = self.config.get("partition_by", [])
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(self.config["table_name"])

    def overwrite_partition(self, df: DataFrame):
        self._write(df, mode="overwrite")

    def overwrite(self, df: DataFrame):
        self._write(df, mode="overwrite")

    def append(self, df: DataFrame):
        self._write(df, mode="append")
        

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
