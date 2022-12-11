from __future__ import absolute_import, division, print_function

import operators
from airflow.plugins_manager import AirflowPlugin


# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.DataCleaningOperator,
        operators.DataQualityOperator,
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
    ]
