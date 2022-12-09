from typing import Optional

from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.utils.context import Context


class LoadDimensionOperator(RedshiftSQLOperator):
    """Load dimension table.

    Args:
        delete_load: Name of the table to delete before load, or None.
    """

    ui_color = "#80BD9E"

    def __init__(self, delete_load: Optional[str] = None, *args, **kwargs):
        self.delete_load = delete_load
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:
        if self.delete_load is not None:
            self.get_hook().run(
                f"DELETE FROM {self.delete_load}",
                autocommit=self.autocommit,
                parameters=self.parameters,
            )
        super(LoadDimensionOperator, self).execute(context)
