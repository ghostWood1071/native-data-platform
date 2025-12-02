from src.core import runner

runner.run(
    "/opt/spark/platform-config/engine/spark-dev-delta.json",
    "/opt/spark/platform-config/job/source2bronze_init_load_T_BACK_ADVANCE_WITHDRAW.json"
)
