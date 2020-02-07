from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class DecapstonePlugin(AirflowPlugin):
    name = "decapstone_plugin"
    operators = [
        operators.CreateEMRClusterOperator,
        operators.ClusterCheckSensor,
        operators.TerminateEMRClusterOperator,
        operators.SubmitSparkJobToEmrOperator
]
