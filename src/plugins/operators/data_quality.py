import logging
from typing import Dict, Iterable

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.utils.context import Context


class DataQualityOperator(BaseOperator):
    """Data quality check

    Args:
        conn_id: Amazon Redshift connection ID.
        checks: List of checks to perform. Each element is a dictionary with the
            following keys:
            sql -> SQL statement representing the check.
            reference_value -> A value to compare the SQL output against.
            func -> A function that encodes the condition necessary for the data quality
                check to pass. Takes the result of the SQL statement and the reference
                value as arguments and returns whether the condition is satisfied.
    """

    ui_color = "#89DA59"

    def __init__(self, conn_id: str, checks: Iterable[Dict[str, str]], *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.checks = checks

    def execute(self, context: Context):
        redshift_hook = RedshiftSQLHook(self.conn_id)
        for i, check in enumerate(self.checks):
            sql_output = redshift_hook.get_records(check["sql"])

            # check if any results
            if len(sql_output) < 1 or len(sql_output[0]) < 1:
                raise ValueError(
                    f"[{i}] Data quality check failed. \"{check['sql']}\" returned no"
                    " results."
                )

            # check if condition is satisfied
            num_output = sql_output[0][0]
            if not check["func"](num_output, check["reference_value"]):
                raise ValueError(
                    f"[{i}] Data quality check failed. Condition not satisfied."
                )
            logging.info(f"[{i}] Data quality check passed.")
