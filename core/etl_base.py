from datetime import datetime, timedelta
from core.transformer.transform_base import TransformBase
from core.reader.base_reader import BaseReader
from core.writer.base_writer import BaseWriter
from pyspark.sql import SparkSession
from core import util
from pyspark.sql import functions as F

class ETL:
    @staticmethod
    def create_spark():
        spark = (
            SparkSession.builder.appName("ETL")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse")
            .enableHiveSupport()
            .getOrCreate()
        )
        return spark

    @staticmethod
    def extract_dfs(spark: SparkSession, args: dict, run_data_date):
        ds_df = {}
        class_dict = {}
        for table_name, source in args.items():
            reader_name = source.pop("reader")
            if not class_dict.get(reader_name):
                class_dict[reader_name] = util.import_module(reader_name)
            ds_df[table_name] = BaseReader.execute(
                class_dict[reader_name], spark, source, run_data_date
            )
        return ds_df

    @staticmethod
    def tranform_dfs(
        spark: SparkSession, dfs: dict, transform_conf: any, run_data_date
    ):
        if isinstance(transform_conf, str):
            transform_module = util.import_module(transform_conf)
            results = TransformBase.execute(transform_module, spark, dfs, run_data_date)
            return results
        if isinstance(transform_conf, dict):
            for table_name, source in transform_conf.items():
                for step in source:
                    func_name = step["op"]
                    args = step.get("args", {})
                    for k, v in args.items():
                        if isinstance(v, str) and v.startswith("F."):
                            args[k] = eval(v)
                    if func_name == "sql" and "query" in args and "view_name" in args:
                        dfs[table_name].createOrReplaceTempView(args["view_name"])
                        dfs[table_name] = spark.sql(args["query"])
                    elif func_name == "drop" and "cols" in args:
                        dfs[table_name] = dfs[table_name].drop(*args["cols"])
                    else:
                        dfs[table_name] = getattr(dfs[table_name], func_name)(**args)
            if dfs is None:
                raise Exception("transformation do not return any dataframe")
        return dfs

    @staticmethod
    def load(spark: SparkSession, dfs: dict, args: dict, run_data_date):
        class_dict = {}
        for table_name, table_config in args.items():
            writer_name = table_config.get("writer")
            if not class_dict.get(writer_name):
                class_dict[writer_name] = util.import_module(writer_name)
            BaseWriter.execute(
                class_dict[writer_name],
                spark,
                dfs.get(table_name),
                table_config,
                run_data_date,
            )

    def run(spark: SparkSession, json_conf, run_data_date):
        df_raw = ETL.extract_dfs(spark, json_conf["extract_dfs"], run_data_date)
        df_trans = ETL.tranform_dfs(
            spark, df_raw, json_conf["transform_dfs"], run_data_date
        )
        ETL.load(spark, df_trans, json_conf["load"], run_data_date)


def run(json_path: str):
    run_data_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    spark = ETL.create_spark()
    conf = util.read_json(json_path)
    print(conf)
    ETL.run(spark, conf, run_data_date)
