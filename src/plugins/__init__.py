from __future__ import absolute_import, division, print_function

import operators
from airflow.plugins_manager import AirflowPlugin


# Defining the plugin class
class SparkifyPlugin(AirflowPlugin):
    name = "sparkify_plugin"
    operators = [
        operators.DataQualityOperator,
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
    ]
