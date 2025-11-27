from src.core import runner

runner.run(
    "/opt/spark/platform-config/engine/spark-dev.json",
    "/opt/spark/platform-config/job/source2silver_order.json"
)
