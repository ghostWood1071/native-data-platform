CREATE TABLE etl_dag (
     id SERIAL PRIMARY KEY,
     dag_id VARCHAR(100) UNIQUE NOT NULL,
     description TEXT,
     schedule_interval VARCHAR(50),
     start_date TIMESTAMP NOT NULL DEFAULT NOW(),
     is_active BOOLEAN NOT NULL DEFAULT TRUE,
     tags TEXT[],
     default_args JSONB
);

CREATE TYPE task_type AS ENUM ('bash', 'spark', 'python', 'dummy');

CREATE TABLE etl_task (
      id SERIAL PRIMARY KEY,
      dag_id VARCHAR(100) NOT NULL REFERENCES etl_dag(dag_id) ON DELETE CASCADE,
      task_id VARCHAR(100) NOT NULL,
      task_type task_type NOT NULL,
      operator_class  VARCHAR(255),
      task_params JSONB
);

CREATE TABLE etl_dependency (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL REFERENCES etl_dag(dag_id) ON DELETE CASCADE,
    upstream_task_id VARCHAR(100) NOT NULL,
    downstream_task_id VARCHAR(100) NOT NULL
);

CREATE TABLE etl_trigger_queue (
   id SERIAL PRIMARY KEY,
   dag_id VARCHAR(200) NOT NULL,       -- trigger DAG id
   conf JSONB,                          -- config
   trigger_time TIMESTAMP DEFAULT NOW(),
   status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING | TRIGGERED | ERROR
   error_message TEXT,
   triggered_at TIMESTAMP
);


INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
VALUES (
           'customer_etl',
           'ETL dữ liệu customer',
           '0 2 * * *',
           '2025-01-01 00:00:00',
           true,
           ARRAY['etl', 'customer'],
           '{
             "owner": "thinh",
             "email": ["de-team@example.com"],
             "retries": 2,
             "retry_delay": "300"
           }'::jsonb
       );


INSERT INTO etl_task (dag_id, task_id, task_type, task_params)
VALUES (
           'customer_etl',
           'transform_customer',
           'spark',
           '{
             "application": "/opt/jobs/transform_customer.py",
             "name": "transform_customer",
             "application_args": ["--run_date", "{{ ds }}"],
             "conf": {
               "spark.executor.memory": "4g"
             }
           }'::jsonb
       );

INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
VALUES
    ('customer_etl', 'extract_customer', 'transform_customer'),
    ('customer_etl', 'transform_customer', 'load_customer');

