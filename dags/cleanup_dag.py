from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from operators import (TerminateEMRClusterOperator)
import boto3
from airflow import AirflowException
import logging

region_name="us-west-2"
emr_conn=None
try:
    emr_conn = boto3.client('emr', region_name=region_name)
except Exception as e:
    logging.info(emr_conn)
    raise AirflowException("emr_connection fail!")

default_args = {
    'owner': 'decapstone-immigration',
    'start_date': datetime(2018,1,1),
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False,
    'provide_context': True
}
#Initializing the Dag, create EMR cluster and then wait for the ETL dag to complete
dag = DAG('cleanup_dag',
          default_args=default_args,
          concurrency=3,
          schedule_interval=None,
          description='Terminates the cluster',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# check_cluster = ExternalTaskSensor(task_id='check_cluster_available',
#                                    external_dag_id = 'cluster_dag',
#                                    external_task_id='check_cluster_waiting',
#                                    dag=dag,
#                                    mode='reschedule')

terminate_cluster = TerminateEMRClusterOperator(
    task_id="terminate_cluster",
    dag=dag,
    emr_connection=emr_conn
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> terminate_cluster >> end_operator