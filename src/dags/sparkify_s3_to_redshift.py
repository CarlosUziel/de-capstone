from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from plugins.operators.data_quality import DataQualityOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.load_fact import LoadFactOperator
from sql_queries import STAGING_TABLES, STAR_TABLES, STAR_TABLES_INSERTS
from utils import process_config

user_config, dwh_config = (
    process_config(Path(__file__).parents[2].joinpath("_user.cfg")),
    process_config(Path(__file__).parents[2].joinpath("dwh.cfg")),
)

default_args = {
    "owner": "Carlos Uziel Pérez Malla",
    "depends_on_past": False,
    # "start_date": datetime(2022, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(hours=1),
    "catchup": False,
}

dag = DAG(
    "SPARKIFY_S3_to_Redshift",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
)

# 0. Start
start_operator = EmptyOperator(task_id="begin_execution", dag=dag)

# 1. Staging tables
stage_tables_tasks = {
    table_name: S3ToRedshiftOperator(
        task_id=f"load_{table_name}",
        dag=dag,
        schema="public",
        table=table_name,
        s3_bucket=dwh_config.get("S3", "BUCKET_NAME"),
        s3_key=dwh_config.get(
            "S3", table_name.split("_")[-1]
        ),  # assign from a task factory
        redshift_conn_id="aws_redshift",
        aws_conn_id="aws_credentials",
        column_list=[col.split(" ")[0] for col in table_args],
        copy_options=[
            "FORMAT JSON "
            + (
                f"AS '{dwh_config.get('S3', 'log_jsonpath')}'"
                if table_name == "staging_events"
                else "AS 'auto'"
            ),
        ],
        autocommit=True,
        method="REPLACE",
    )
    for table_name, table_args in STAGING_TABLES.items()
}

for task in stage_tables_tasks.values():
    start_operator.set_downstream(task)

# 1.1. Check that staging tables are not empty
check_stage_tables_tasks = {
    table_name: DataQualityOperator(
        task_id=f"check_{table_name}",
        dag=dag,
        conn_id="aws_redshift",
        checks=[
            {
                "sql": f"SELECT COUNT(*) FROM {table_name}",
                "reference_value": 0,
                "func": (lambda x, y: x > y),
            }
        ],
    )
    for table_name in STAGING_TABLES.keys()
}

for table_name, task in check_stage_tables_tasks.items():
    stage_tables_tasks[table_name].set_downstream(task)

# 2. Loaded staging
mid_operator = EmptyOperator(task_id="staging_ready", dag=dag)
for task in check_stage_tables_tasks.values():
    mid_operator.set_upstream(task)

# 3. Populate dim tables
insert_dim_tables = {
    table_name: LoadDimensionOperator(
        task_id=f"insert_{table_name}",
        dag=dag,
        redshift_conn_id="aws_redshift",
        sql=table_insert_sql,
        delete_load=table_name,
    )
    for table_name, table_insert_sql in STAR_TABLES_INSERTS.items()
    if "dim" in table_name
}
for task in insert_dim_tables.values():
    mid_operator.set_downstream(task)

# 3.1. Data quality on all dim tables
check_dim_tables = {
    table_name: DataQualityOperator(
        task_id=f"check_{table_name}",
        dag=dag,
        conn_id="aws_redshift",
        checks=[
            {
                "sql": f"SELECT COUNT(*) FROM {table_name}",
                "reference_value": 0,
                "func": (lambda x, y: x > y),
            }
        ],
    )
    for table_name in STAR_TABLES.keys()
    if "dim" in table_name
}
for table_name, task in check_dim_tables.items():
    insert_dim_tables[table_name].set_downstream(task)

# 4. Dim ready
mid2_operator = EmptyOperator(task_id="dim_ready", dag=dag)
for task in check_dim_tables.values():
    task.set_downstream(mid2_operator)

# 5. Insert facts tables
insert_fact_tables = {
    table_name: LoadFactOperator(
        task_id=f"insert_{table_name}",
        dag=dag,
        redshift_conn_id="aws_redshift",
        sql=table_insert_sql,
    )
    for table_name, table_insert_sql in STAR_TABLES_INSERTS.items()
    if "fact" in table_name
}
for task in insert_fact_tables.values():
    mid2_operator.set_downstream(task)

# 5.1. Data quality on all dim tables
check_fact_tables = {
    table_name: DataQualityOperator(
        task_id=f"check_{table_name}",
        dag=dag,
        conn_id="aws_redshift",
        checks=[
            {
                "sql": f"SELECT COUNT(*) FROM {table_name}",
                "reference_value": 0,
                "func": (lambda x, y: x > y),
            }
        ],
    )
    for table_name in STAR_TABLES.keys()
    if "fact" in table_name
}
for table_name, task in check_fact_tables.items():
    insert_fact_tables[table_name].set_downstream(task)

end_operator = EmptyOperator(task_id="all_ready", dag=dag)
for task in check_fact_tables.values():
    task.set_downstream(end_operator)
