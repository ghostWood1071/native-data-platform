"""
Generate real Airflow DAG objects from DB-only serialized DAG rows.

The Jenkins compiler writes Airflow native metadata tables directly:
  - dag
  - serialized_dag
  - dag_tag

Airflow CLI task execution still loads DAG objects from files in the dags
folder. This generator makes those DB-only DAGs visible to DagBag by reading
serialized_dag.data and constructing equivalent in-memory DAG objects.

This file is intentionally read-only. It does not upsert, pause, tag, or
otherwise mutate Airflow metadata.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging
import os
from typing import Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.settings import Session
from sqlalchemy import text


DB_ONLY_FILELOC_PREFIX = os.getenv("DB_ONLY_DAG_FILELOC_PREFIX", "db-json://")
LOG = logging.getLogger(__name__)


SPARK_SUBMIT_FIELDS = [
    "application",
    "conf",
    "files",
    "py_files",
    "jars",
    "driver_class_path",
    "packages",
    "exclude_packages",
    "keytab",
    "principal",
    "proxy_user",
    "name",
    "application_args",
    "env_vars",
    "properties_file",
    "deploy_mode",
]


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _deserialize_timedelta(value: Any, default_seconds: float = 300.0) -> timedelta:
    if isinstance(value, dict) and value.get("__type") == "timedelta":
        return timedelta(seconds=float(value.get("__var", default_seconds)))

    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))

    return timedelta(seconds=default_seconds)


def _deserialize_datetime(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)

    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    return datetime(2025, 1, 1, tzinfo=timezone.utc)


def _deserialize_default_args(serialized_dag: dict[str, Any]) -> dict[str, Any]:
    raw_default_args = serialized_dag.get("default_args") or {}
    values = raw_default_args.get("__var", raw_default_args)

    if not isinstance(values, dict):
        values = {}

    return {
        "owner": values.get("owner", "airflow"),
        "email": values.get("email", []),
        "retries": int(values.get("retries", 0) or 0),
        "retry_delay": _deserialize_timedelta(values.get("retry_delay"), default_seconds=300.0),
        "start_date": _deserialize_datetime(serialized_dag.get("start_date")),
    }


def _deserialize_schedule(serialized_dag: dict[str, Any]) -> Any:
    timetable = serialized_dag.get("timetable") or {}
    timetable_type = timetable.get("__type")
    values = timetable.get("__var") or {}

    if timetable_type == "airflow.timetables.interval.CronDataIntervalTimetable":
        return values.get("expression")

    return None


def _task_var(serialized_task: dict[str, Any]) -> dict[str, Any]:
    values = serialized_task.get("__var") or {}
    return values if isinstance(values, dict) else {}


def _build_spark_submit_task(task_values: dict[str, Any], dag: DAG) -> SparkSubmitOperator:
    operator_kwargs = {
        "task_id": task_values["task_id"],
        "dag": dag,
        "conn_id": task_values.get("conn_id") or task_values.get("_conn_id") or "spark_k8s",
    }

    for field in SPARK_SUBMIT_FIELDS:
        if field in task_values:
            operator_kwargs[field] = task_values[field]

    return SparkSubmitOperator(**operator_kwargs)


def _build_task(task_values: dict[str, Any], dag: DAG):
    task_type = task_values.get("_task_type") or task_values.get("task_type")

    if task_type == "SparkSubmitOperator":
        return _build_spark_submit_task(task_values, dag)

    return EmptyOperator(task_id=task_values["task_id"], dag=dag)


def _load_serialized_dags() -> list[dict[str, Any]]:
    session = Session()

    try:
        rows = session.execute(
            text(
                """
                SELECT sd.dag_id, sd.data
                FROM serialized_dag sd
                JOIN dag d ON d.dag_id = sd.dag_id
                WHERE COALESCE(d.is_active, true) = true
                  AND sd.fileloc LIKE :fileloc_pattern
                ORDER BY sd.dag_id
                """
            ),
            {"fileloc_pattern": f"{DB_ONLY_FILELOC_PREFIX}%"},
        ).mappings()

        result = []

        for row in rows:
            payload = _json_value(row["data"])

            if isinstance(payload, dict) and isinstance(payload.get("dag"), dict):
                result.append(payload)

        return result

    finally:
        session.close()


def _build_dag(serialized_payload: dict[str, Any]) -> DAG:
    serialized_dag = serialized_payload["dag"]
    dag_id = serialized_dag.get("_dag_id")

    if not dag_id:
        raise ValueError("Serialized DAG payload is missing dag._dag_id")

    dag = DAG(
        dag_id=dag_id,
        description=serialized_dag.get("_description") or "",
        schedule_interval=_deserialize_schedule(serialized_dag),
        default_args=_deserialize_default_args(serialized_dag),
        catchup=bool(serialized_dag.get("catchup", False)),
        tags=serialized_dag.get("tags") or [],
        max_active_runs=int(serialized_dag.get("max_active_runs", 1) or 1),
    )

    task_by_id = {}
    downstream_by_id = {}

    for serialized_task in serialized_dag.get("tasks") or []:
        task_values = _task_var(serialized_task)
        task_id = task_values.get("task_id")

        if not task_id:
            continue

        task_by_id[task_id] = _build_task(task_values, dag)
        downstream_by_id[task_id] = task_values.get("downstream_task_ids") or []

    for upstream_id, downstream_ids in downstream_by_id.items():
        upstream_task = task_by_id.get(upstream_id)

        if upstream_task is None:
            continue

        for downstream_id in downstream_ids:
            downstream_task = task_by_id.get(downstream_id)

            if downstream_task is not None:
                upstream_task >> downstream_task

    return dag


try:
    for _serialized_payload in _load_serialized_dags():
        _dag = _build_dag(_serialized_payload)
        globals()[_dag.dag_id] = _dag
except Exception:
    LOG.exception("Failed to load DB-only DAGs from serialized_dag")
