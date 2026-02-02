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
    def overwrite_partition(self, df: DataFrame):
        fqn_table_name = self.config.get("table_name") #catalog.database.table
        table_name = fqn_table_name.split(".")[2]
        db_name = fqn_table_name.split(".")[1]
        partition_cols = self.config.get("partition_by", [])
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        first = self.config.get("first", False)
        (
            df.write.format("delta")
                            .mode("overwrite")
                            .partitionBy(*partition_cols)
                            .option("path", self.config.get("path", f"s3a://warehouse/{db_name}/{table_name}"))
                            .save()
        )
        if first:
            self.spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
                USING delta
                LOCATION '{self.config.get("path", f"s3a://warehouse/{db_name}/{table_name}")}'
            """)

    def overwrite(self, df: DataFrame):
        fqn_table_name = self.config.get("table_name") #catalog.database.table
        table_name = fqn_table_name.split(".")[2]
        db_name = fqn_table_name.split(".")[1]
        partition_cols = self.config.get("partition_by", [])
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        (
            df.write.format("delta")
                            .mode("overwrite")
                            .partitionBy(*partition_cols)
                            .option("path", self.config.get("path", f"s3a://warehouse/{db_name}/{table_name}"))
                            .save()
        )
        first = self.config.get("first", False)
        if first:
            self.spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
                USING delta
                LOCATION '{self.config.get("path", f"s3a://warehouse/{db_name}/{table_name}")}'
            """)


    def append(self, df: DataFrame):
        fqn_table_name = self.config.get("table_name") #catalog.database.table
        table_name = fqn_table_name.split(".")[2]
        db_name = fqn_table_name.split(".")[1]
        partition_cols = self.config.get("partition_by", [])
        first = self.config.get("first", False)
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        (
            df.write.format("delta")
                            .mode("append")
                            .partitionBy(*partition_cols)
                            .option("path", self.config.get("path", f"s3a://warehouse/{db_name}/{table_name}"))
                            .save()
        )
        if first:
            self.spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
                USING delta
                LOCATION '{self.config.get("path", f"s3a://warehouse/{db_name}/{table_name}")}'
            """)
        

    def upsert(self, df: DataFrame):
        fqn_table_name = self.config.get("table_name") #catalog.database.table
        table_name = fqn_table_name.split(".")[2]
        db_name = fqn_table_name.split(".")[1]
        primary_key = self.config.get("primary_key")
        change_tracking_cols = self.config.get("change_tracking_column", df.columns)
        additional_merge_cond = self.config.get("additional_merge_cond", "")
        is_update = self.config.get("is_update", True)
        is_insert = self.config.get("is_insert", True)
        insert_cols = self.config.get("insert_cols", df.columns)
        first = self.config.get("first", False)
        partition_cols = self.config.get("partition_by", [])
        schema_ddl = self.config.get("schema_ddl",    
            ",\n".join(
                    f"{f.name} {f.dataType.simpleString()}"
                    for f in df.schema.fields
                )
        )

        merge_cond = " AND ".join([f"t.{col} = s.{col}" for col in primary_key])
        if additional_merge_cond:
            merge_cond += f" AND {additional_merge_cond}"

        update_cond = " OR ".join([f"t.{col} <> s.{col}" for col in change_tracking_cols])
        set_expr = ", ".join([f"{c} = s.{c}" for c in change_tracking_cols])
        insert_cols_expr = ", ".join(insert_cols)
        insert_vals = ", ".join([f"s.{c}" for c in insert_cols]) 
        
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        df.createOrReplaceTempView(f"tmp_{table_name}")

        if first:
            self.spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")

            self.spark.sql(f"""
            CREATE TABLE {db_name}.{table_name} (
                {schema_ddl}
            )
            USING DELTA
            PARTITIONED BY ({", ".join(partition_cols)})
            LOCATION 's3a://warehouse/{db_name}/{table_name}'
            """)

        merge_sql = f"""
        MERGE INTO {db_name}.{table_name} t
        USING tmp_{table_name} s ON {merge_cond}
            WHEN MATCHED AND s.op = 'u' THEN UPDATE SET {set_expr}
            WHEN MATCHED AND s.op = 'd' THEN DELETE 
            WHEN NOT MATCHED THEN INSERT ({insert_cols_expr}) VALUES ({insert_vals})
        """

        self.spark.sql(merge_sql)