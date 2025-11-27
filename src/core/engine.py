from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from src.core.factory import Factory

class ETL:
    def execute(spark: SparkSession, config, run_data_date):
        dfs = {}
        for table_name, source in config.get("extract").items():
            print(f"[ETL.execute] extract data from {table_name} ...")
            dfs[table_name] = Factory.extract(spark, source, run_data_date)
        print("[ETL.execute] start transform data...")
        dfs = Factory.transform(spark, config.get("transform"), dfs, run_data_date)
        for df_name, df_load_config in config.get("load").items():
            print(f"[ETL.execute] load {df_name} to target ...")
            Factory.load(spark, dfs.get(df_name), df_load_config, run_data_date)




