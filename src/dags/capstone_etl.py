from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from data.tables import ON_LOAD_TABLES_CLEANING_ARGS
from plugins.operators.data_cleaning import DataCleaningOperator
from plugins.operators.data_quality import DataQualityOperator
from utils.io import process_config
from utils.spark import create_spark_session

user_config, dl_config = (
    process_config(Path(__file__).parents[2].joinpath("_user.cfg")),
    process_config(Path(__file__).parents[2].joinpath("dl.cfg")),
)
spark = create_spark_session(user_config, dl_config)

s3_bucket_prefix = dl_config.get("S3", "BUCKET_NAME") + "/clean"

default_args = {
    "owner": "DE Capstone",
    "depends_on_past": False,
    "start_date": datetime(2022, 12, 12),
    # "retries": 1,
    # "retry_delay": timedelta(hours=1),
    "catchup": False,
}

dag = DAG(
    "capstone_etl",
    default_args=default_args,
    description="Load and transform data in S3 data lake with Airflow",
    schedule_interval="@once",
)

# 0. Start
start_op = EmptyOperator(task_id="begin_execution", dag=dag)

# 1. Cleaning tables
clean_tables_tasks = {
    table_name: DataCleaningOperator(
        task_id=f"clean_{table_name}",
        dag=dag,
        spark=spark,
        table_name=table_name,
        s3_bucket_prefix=s3_bucket_prefix,
        **table_kwargs,
    )
    for table_name, table_kwargs in ON_LOAD_TABLES_CLEANING_ARGS.items()
}
for task in clean_tables_tasks.values():
    start_op.set_downstream(task)

# 2. Check completeness in cleaned tables
clean_tables_quality_check_tasks = {
    table_name: DataQualityOperator(
        task_id=f"check_{table_name}",
        dag=dag,
        table_parquet=f"s3a://{s3_bucket_prefix}/{table_name}",
        spark=spark,
        table_name=table_name,
        checks=[
            {
                "sql": f"SELECT COUNT(*) FROM {table_name}",
                "reference_value": 0,
                "func": (lambda x, y: x > y),
            }
        ],
    )
    for table_name in ON_LOAD_TABLES_CLEANING_ARGS.keys()
}
for table_name, clean_task in clean_tables_tasks.items():
    clean_task.set_downstream(clean_tables_quality_check_tasks[table_name])

# 3. Intermediate step - Clean ready
clean_ready_op = EmptyOperator(task_id="clean_ready", dag=dag)
for task in clean_tables_quality_check_tasks.values():
    task.set_downstream(clean_ready_op)
