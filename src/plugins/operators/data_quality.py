import logging
from typing import Dict, Iterable

from airflow.models import BaseOperator
from airflow.utils.context import Context
from pyspark.sql import SparkSession


class DataQualityOperator(BaseOperator):
    """Run data quality checks on S3 tables, loaded by Spark.

    Args:
        spark: Spark session.
        table_parquet: Parquet file to load.
        table_name: Name of the table to run SQL checks against.
        checks: List of checks to perform. Each element is a dictionary with the
            following keys:
            sql -> SQL statement representing the check.
            reference_value -> A value to compare the SQL output against.
            func -> A function that encodes the condition necessary for the data quality
                check to pass. Takes the result of the SQL statement and the reference
                value as arguments and returns whether the condition is satisfied.
    """

    ui_color = "#4A708B"

    def __init__(
        self,
        spark: SparkSession,
        table_parquet: str,
        table_name: str,
        checks: Iterable[Dict[str, str]],
        *args,
        **kwargs,
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.spark = spark
        self.table_parquet = table_parquet
        self.table_name = table_name
        self.checks = checks

    def execute(self, context: Context):
        # 1. Load table
        table_df = self.spark.read.parquet(self.table_parquet)
        table_df.createOrReplaceTempView(self.table_name)

        # 2. Run checks against table
        for i, check in enumerate(self.checks):
            sql_output = self.spark.sql(check["sql"])

            # check if any results
            if sql_output.count() < 1:
                raise ValueError(
                    f"[{i}] Data quality check failed. \"{check['sql']}\" returned no"
                    " results."
                )
            elif sql_output.count() > 1:
                raise NotImplementedError(
                    "This operator only works for checks that return a single value."
                )

            # check if condition is satisfied
            logging.info(sql_output.take(1)[0][0])
            if not check["func"](sql_output.take(1)[0][0], check["reference_value"]):
                raise ValueError(
                    f"[{i}] Data quality check failed. Condition not satisfied."
                )
            logging.info(f"[{i}] Data quality check passed.")
