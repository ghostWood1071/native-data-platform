from src.core.transformers import PyTransformer
from pyspark.sql import functions as F
from datetime import datetime

"""
Generate a report showing the top 10 users who completed the most order payments in day.
"""
class TransformSilver2GoldenToTopUser(PyTransformer):
    def prepare_order_df(self, order_df):
        run_date = datetime.strptime(self.run_data_date, "%Y-%m-%d")
        order_df = (
            order_df.filter(
                (F.col("updated_at") >= F.lit(run_date))
                & (F.col("created_at") < F.lit(run_date))
                & (F.col("status") == F.lit("Completed"))
            )
            .groupBy("customer_id")
            .agg(F.count("*").alias("order_count"))
            .sort(F.col("order_count").desc())
            .limit(10)
        )
        return order_df

    def transform(self):
        customer_df = self.dfs.get("customers")
        order_df = self.prepare_order_df(self.dfs.get("orders")).cache()
        df_result = customer_df.alias("a").join(
            F.broadcast(order_df).alias("b"),
            on=F.col("a.id") == F.col("b.customer_id"),
            how="inner",
        ).select(F.expr("a.*"))
        return {
            "top_user": df_result
        }
