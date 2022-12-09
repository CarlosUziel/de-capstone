from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator


class LoadFactOperator(RedshiftSQLOperator):
    ui_color = "#F98866"

    def __init__(self, *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
