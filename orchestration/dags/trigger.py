from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import json


def process_trigger_queue(**context):
    hook = PostgresHook(postgres_conn_id="metadata_db")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Lấy tất cả trigger đang PENDING
    cursor.execute("""
                   SELECT id, dag_id, conf
                   FROM etl_trigger_queue
                   WHERE status = 'PENDING'
                   ORDER BY trigger_time ASC
                       LIMIT 20;
                   """)

    triggers = cursor.fetchall()

    if not triggers:
        return []

    triggered_list = []

    for trigger_id, dag_id, conf_json in triggers:
        triggered_list.append({
            "id": trigger_id,
            "dag_id": dag_id,
            "conf": conf_json
        })

    # Trả ra để downstream task loop
    return triggered_list


def mark_trigger_status(trigger_id, status, error_message=None):
    hook = PostgresHook(postgres_conn_id="metadata_db")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
                   UPDATE etl_trigger_queue
                   SET status = %s,
                       error_message = %s,
                       triggered_at = NOW()
                   WHERE id = %s
                   """, (status, error_message, trigger_id))

    conn.commit()
    cursor.close()


def trigger_single_run(trigger_item, **context):
    trigger_id = trigger_item["id"]
    dag_id = trigger_item["dag_id"]
    conf = trigger_item["conf"]

    try:
        context["ti"].xcom_push(key="dag_id", value=dag_id)
        context["ti"].xcom_push(key="conf", value=conf)

        mark_trigger_status(trigger_id, "TRIGGERED")
        return True

    except Exception as e:
        mark_trigger_status(trigger_id, "ERROR", str(e))
        return False


with DAG(
        dag_id="trigger_worker",
        schedule_interval="*/1 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        default_args={"owner": "system"},
        max_active_runs=1
) as dag:

    load_triggers = PythonOperator(
        task_id="load_triggers",
        python_callable=process_trigger_queue,
        provide_context=True
    )

    # Task dynamic mapping: trigger nhiều DAG trong một lần chạy
    from airflow.operators.python import PythonOperator

    def _run_trigger(**context):
        ti = context["ti"]
        dag_id = ti.xcom_pull(task_ids="trigger_mapper", key="dag_id")
        conf = ti.xcom_pull(task_ids="trigger_mapper", key="conf")

        # Trigger DAG
        TriggerDagRunOperator(
            task_id=f"fire_{dag_id}",
            trigger_dag_id=dag_id,
            conf=conf,
        ).execute(context=context)


    trigger_mapper = PythonOperator.partial(
        task_id="trigger_mapper",
        python_callable=_run_trigger
    ).expand(op_args=[load_triggers.output])

    load_triggers >> trigger_mapper
