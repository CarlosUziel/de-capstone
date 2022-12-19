from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from data.tables import ON_LOAD_TABLES_CLEANING_ARGS, STAR_EXTRACT_TABLES_ARGS
from plugins.operators.data_quality import DataQualityOperator
from utils.io import process_config
from utils.spark import create_spark_session

user_config, dl_config = (
    process_config(Path(__file__).parents[2].joinpath("_user.cfg")),
    process_config(Path(__file__).parents[2].joinpath("dl.cfg")),
)
spark = create_spark_session(user_config, dl_config)

s3_bucket_prefix = dl_config.get("S3", "BUCKET_NAME")

default_args = {
    "owner": "DE Capstone",
    "depends_on_past": False,
    "start_date": datetime(2022, 12, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
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
        s3_bucket_prefix=s3_bucket_prefix + "/clean",
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
        table_parquet=f"s3a://{s3_bucket_prefix}/clean/{table_name}",
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

# # 3. Intermediate step - Clean ready
clean_ready_op = EmptyOperator(task_id="clean_ready", dag=dag)
for task in clean_tables_quality_check_tasks.values():
    task.set_downstream(clean_ready_op)

# 4. Extract dimensional tables
# 4.1. Extract `dim_cities`
extract_dim_cities_task = PythonOperator(
    task_id=f"extract_dim_cities",
    dag=dag,
    python_callable=STAR_EXTRACT_TABLES_ARGS["dim_cities"]["python_callable"],
    op_kwargs={"spark": spark, **STAR_EXTRACT_TABLES_ARGS["dim_cities"]["op_kwargs"]},
)
extract_dim_cities_task.set_upstream(clean_ready_op)

check_dim_cities_task = DataQualityOperator(
    task_id=f"check_dim_cities",
    dag=dag,
    table_parquet=STAR_EXTRACT_TABLES_ARGS["dim_cities"]["op_kwargs"]["s3_save_path"],
    spark=spark,
    table_name="dim_cities",
    checks=[
        {
            "sql": f"SELECT COUNT(*) FROM dim_cities",
            "reference_value": 0,
            "func": (lambda x, y: x > y),
        }
    ],
)
check_dim_cities_task.set_upstream(extract_dim_cities_task)

# 4.2. Extract all the other dimensional tables and perform data quality
extract_tables_tasks = {
    table_name: PythonOperator(
        task_id=f"extract_{table_name}",
        dag=dag,
        python_callable=table_kwargs["python_callable"],
        op_kwargs={"spark": spark, **table_kwargs["op_kwargs"]},
    )
    for table_name, table_kwargs in STAR_EXTRACT_TABLES_ARGS.items()
    if table_name is not "dim_cities"
}
for task in extract_tables_tasks.values():
    check_dim_cities_task.set_downstream(task)

check_extract_tables_tasks = {
    table_name: DataQualityOperator(
        task_id=f"check_{table_name}",
        dag=dag,
        table_parquet=table_kwargs["op_kwargs"]["s3_save_path"],
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
    for table_name, table_kwargs in STAR_EXTRACT_TABLES_ARGS.items()
    if table_name is not "dim_cities"
}
for table_name, extract_task in extract_tables_tasks.items():
    extract_task.set_downstream(check_extract_tables_tasks[table_name])

# 5. All STAR dimensional tables extracted
extraction_ready_op = EmptyOperator(task_id="extraction_ready", dag=dag)
for check_extract_task in check_extract_tables_tasks.values():
    check_extract_task.set_downstream(extraction_ready_op)
