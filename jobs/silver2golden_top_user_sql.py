from src.core import runner

runner.run(
    "/opt/spark/platform-config/engine/spark-dev.json",
    "/opt/spark/platform-config/job/silver2golden_top_user_sql.json"
)
