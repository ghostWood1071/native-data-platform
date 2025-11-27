from src.core.interfaces import BaseTransformer
from src.core import util
from pyspark.sql import SparkSession, DataFrame

class PyTransformer(BaseTransformer):
    def transform(self):
        transformation = self.config
        from src.core.factory import Factory
        transform_class = Factory.import_module(f"src.business_logic.py_transform.{transformation}")
        transformer = transform_class(self.spark, self.config, self.dfs, self.run_data_date)
        return transformer.transform()

class ConfigTransformer(BaseTransformer):
    def transform(self):
        from pyspark.sql import functions as F
        for table_name, source in self.config.items():
            for step in source:
                func_name = step["op"]
                args = step.get("args", {})
                for k, v in args.items():
                    if isinstance(v, str) and v.startswith("F."):
                        args[k] = eval(v)
                if func_name == "sql" and "query" in args and "view_name" in args:
                    self.dfs[table_name].createOrReplaceTempView(args["view_name"])
                    self.dfs[table_name] = self.spark.sql(args["query"])
                elif func_name == "drop" and "cols" in args:
                    self.dfs[table_name] = self.dfs[table_name].drop(*args["cols"])
                else:
                    self.dfs[table_name] = getattr(self.dfs[table_name], func_name)(**args)
        return self.dfs


class SQLTransformer(BaseTransformer):
    def transform(self):
        sql_file_path = self.config
        for tbl_name, dataframe in self.dfs.items():
            dataframe.createOrReplaceTempView(tbl_name)
        sql_command = util.read_file(sql_file_path)
        sql_command = sql_command.replace('{$run_date}', f"'{self.run_data_date}'")
        return {
           "result":  self.spark.sql(sql_command)
        }