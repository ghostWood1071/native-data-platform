import psycopg2
from psycopg2.extras import RealDictCursor
import json

class PostgresDB:
    def __init__(self, host, db, user, pwd, port=5432):
        self.conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=pwd,
            port=port
        )
        self.conn.autocommit = True

    def execute(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)

    def fetchall(self, query, params=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def fetchone(self, query, params=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()

def truncate_all(db):
    # db.execute("TRUNCATE TABLE etl_trigger_queue RESTART IDENTITY;")
    db.execute("DELETE FROM etl_dependency;")
    db.execute("DELETE FROM etl_task;")
    db.execute("DELETE FROM etl_dag;")




def get_insert_dags_cmd(dag_id, description, schedule_interval, start_date, is_active, tags, default_args):
    return f"""
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  '{dag_id}',
                  '{description}',
                  '{schedule_interval}',
                  '{start_date}',
                  {str(is_active).lower()},
                  ARRAY{str(tags)},
                  '{json.dumps(default_args)}'::jsonb
              );
    """

def get_insert_task_cmd(dag_id, task_id, task_type, task_params):
    return f"""
        INSERT INTO etl_task (dag_id, task_id, task_type, task_params)
        VALUES (
                   '{dag_id}',
                   '{task_id}',
                   '{task_type}',
                   '{json.dumps(task_params)}'::jsonb
               );
        """

def get_dependency_task_cmd(dag_id, up_task_id, down_task_id):
    return f"""
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('{dag_id}', '{up_task_id}', '{down_task_id}');
    """

def load_config(db, dag_config_path):
    with open(dag_config_path, mode = 'r') as f:
        config_json = json.loads(f.read())
        dag_id = config_json.get('dag_id')
        insert_dags_cmd = get_insert_dags_cmd(
            dag_id,
            config_json.get('description'),
            config_json.get('schedule'),
            config_json.get('start_date'),
            config_json.get('active'),
            config_json.get("tags"),
            config_json.get('default_args'),
        )
        db.execute(insert_dags_cmd)
        # print(insert_dags_cmd)
        if not config_json.get("tasks"):
            return
        for task in config_json.get("tasks"):
            current_task = task.get("task_id")
            insert_task_cmd = get_insert_task_cmd(
                dag_id, current_task, task.get("task_type"), task.get("task_params")
            )
            db.execute(insert_task_cmd)
            # print(insert_task_cmd)
            if not task.get("depend_on"):
                continue
            for depend_id in task.get("depend_on"):
                insert_depend_cmd = get_dependency_task_cmd(dag_id, depend_id, current_task)
                db.execute(insert_depend_cmd)
                # print(insert_depend_cmd)



def load_config_batch(db):
    config_path = "../config/workflow/"
    import os
    config_files = os.listdir(config_path)
    print(config_files)
    for config_file in config_files:
        path = f"{config_path}{config_file}"
        print(path)
        load_config(db, path)



if __name__ == "__main__":
    # Kết nối
    db = PostgresDB(
        host="localhost",
        db="airflow",
        user="hive",
        pwd="hive"
    )
    # db = None
    truncate_all(db)
    load_config_batch(db)
    print("DONE!")
