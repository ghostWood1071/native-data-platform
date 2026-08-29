from uuid import uuid4

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

    A path-based ``DataFrameWriter.save`` exposes only the physical Delta path to
    OpenLineage.  Keeping the source DataFrame in a temporary view and executing
    one INSERT query gives Spark a single logical plan containing both the JDBC
    input and the Hive/Delta output, including the column projections.
    """

    @staticmethod
    def _quote_identifier(identifier):
        return f"`{identifier.replace('`', '``')}`"

    def _table_details(self):
        parts = self.config["table_name"].split(".")
        if len(parts) != 3 or any(not part for part in parts):
            raise ValueError("Delta table_name must be catalog.database.table")

        quoted_parts = [self._quote_identifier(part) for part in parts]
        table_name = ".".join(quoted_parts)
        database_name = ".".join(quoted_parts[:2])
        default_path = f"s3a://warehouse/{parts[1]}/{parts[2]}"
        path = self.config.get("path", default_path).replace("'", "''")
        return table_name, database_name, path

    def _ensure_table(self, df, recreate):
        table_name, database_name, path = self._table_details()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        if recreate and self.config.get("first", False):
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")

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
            f"USING DELTA{partition_clause} LOCATION '{path}'"
        )
        return table_name

    def _insert(self, df, overwrite):
        table_name = self._ensure_table(
            df, recreate=self.config.get("first", False)
        )
        temp_view = f"_delta_writer_{uuid4().hex}"
        quoted_columns = ", ".join(
            self._quote_identifier(column) for column in df.columns
        )
        command = "INSERT OVERWRITE TABLE" if overwrite else "INSERT INTO TABLE"

        df.createOrReplaceTempView(temp_view)
        try:
            self.spark.sql(
                f"{command} {table_name} ({quoted_columns}) "
                f"SELECT {quoted_columns} FROM {self._quote_identifier(temp_view)}"
            )
        finally:
            self.spark.catalog.dropTempView(temp_view)

    def overwrite_partition(self, df: DataFrame):
        self._insert(df, overwrite=True)

    def overwrite(self, df: DataFrame):
        self._insert(df, overwrite=True)

    def append(self, df: DataFrame):
        self._insert(df, overwrite=False)
        

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
