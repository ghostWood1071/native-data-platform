from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator  # dummy

def build_task(task_row, dag):
    ttype = task_row["task_type"]
    params = task_row["task_params"] or {}

    if ttype == "bash":
        return BashOperator(
            task_id=task_row["task_id"],
            dag=dag,
            **params
        )

    if ttype == "spark":
        return SparkSubmitOperator(
            task_id=task_row["task_id"],
            dag=dag,
            application=params.get("application"),
            name=params.get("name", task_row["task_id"]),
            application_args=params.get("application_args", []),
            conf=params.get("conf", {}),
            jars=params.get("jars"),
            packages=params.get("packages"),
            driver_memory=params.get("driver_memory", "1g"),
            executor_memory=params.get("executor_memory", "2g"),
            executor_cores=params.get("executor_cores", 1),
        )

    raise ValueError(f"Unknown task_type: {ttype}")


def load_metadata():
    hook = PostgresHook(postgres_conn_id="metadata_db")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
                   SELECT dag_id, description, schedule_interval, start_date, is_active, default_args, tags
                   FROM etl_dag
                   WHERE is_active = true
                   """)
    dag_rows = cursor.fetchall()

    dags_meta = {}
    for row in dag_rows:
        dag_id, desc, schedule, start_date, is_active, default_args, tags = row
        dags_meta[dag_id] = {
            "dag_id": dag_id,
            "description": desc,
            "schedule_interval": schedule,
            "start_date": start_date,
            "default_args": default_args or {},
            "tags": tags or [],
            "tasks": [],
            "dependencies": []
        }

    if not dags_meta:
        return {}
    dag_ids = tuple(dags_meta.keys())
    cursor.execute("""
                   SELECT dag_id, task_id, task_type::text, task_params
                   FROM etl_task
                   WHERE dag_id = ANY(%s)
                   """, (list(dag_ids),))
    for dag_id, task_id, task_type, task_params in cursor.fetchall():
        dags_meta[dag_id]["tasks"].append({
            "task_id": task_id,
            "task_type": task_type,
            "task_params": task_params
        })

    cursor.execute("""
                   SELECT dag_id, upstream_task_id, downstream_task_id
                   FROM etl_dependency
                   WHERE dag_id = ANY(%s)
                   """, (list(dag_ids),))
    for dag_id, upstream, downstream in cursor.fetchall():
        dags_meta[dag_id]["dependencies"].append((upstream, downstream))

    cursor.close()
    conn.close()
    return dags_meta

metadata = load_metadata()
for dag_id, meta in metadata.items():
    da = meta["default_args"] or {}
    retry_delay_sec = int(da.pop("retry_delay", "300")) if "retry_delay" in da else 300

    default_args = {
        "owner": da.get("owner", "airflow"),
        "depends_on_past": da.get("depends_on_past", False),
        "email": da.get("email", []),
        "email_on_failure": da.get("email_on_failure", False),
        "email_on_retry": da.get("email_on_retry", False),
        "retries": da.get("retries", 1),
        "retry_delay": timedelta(seconds=retry_delay_sec),
        "start_date": meta["start_date"] or days_ago(1),
    }

    dag = DAG(
        dag_id=meta["dag_id"],
        description=meta["description"],
        schedule_interval=meta["schedule_interval"],
        default_args=default_args,
        catchup=False,
        tags=meta["tags"],
    )

    task_objs = {}
    for t in meta["tasks"]:
        task_obj = build_task(t, dag)
        task_objs[t["task_id"]] = task_obj

    for upstream_id, downstream_id in meta["dependencies"]:
        task_objs[upstream_id] >> task_objs[downstream_id]

    globals()[meta["dag_id"]] = dag
