from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from data.tables import ON_LOAD_TABLES_CLEANING_ARGS
from plugins.operators.data_cleaning import DataCleaningOperator
from utils import process_config
from utils.aws import create_s3_bucket
from utils.spark import create_spark_session

user_config, dl_config = (
    process_config(Path(__file__).parents[2].joinpath("_user.cfg")),
    process_config(Path(__file__).parents[2].joinpath("dl.cfg")),
)
spark = create_spark_session(user_config, dl_config)

assert create_s3_bucket(user_config, dl_config), "Error creating S3 bucket."
s3_bucket_prefix = dl_config.get("S3", "BUCKET_NAME") + "/clean"

default_args = {
    "owner": "DE Capstone",
    "depends_on_past": False,
    # "start_date": datetime(2022, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(hours=1),
    "catchup": False,
}

dag = DAG(
    "capstone_etl",
    default_args=default_args,
    description="Load and transform data in S3 data lake with Airflow",
    # schedule_interval="@hourly",
)

# 0. Start
start_operator = EmptyOperator(task_id="begin_execution", dag=dag)

# 1. Cleaning tables
clean_tables_tasks = {
    table_name: DataCleaningOperator(
        task_id=f"clean_{table_name}",
        dag=dag,
        spark=spark,
        s3_bucket_prefix=s3_bucket_prefix,
        **table_kwargs,
    )
    for table_name, table_kwargs in ON_LOAD_TABLES_CLEANING_ARGS.items()
}
for task in clean_tables_tasks.values():
    start_operator.set_downstream(task)
