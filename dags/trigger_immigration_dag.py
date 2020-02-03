from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from operators import (ClusterCheckSensor)
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
dag = DAG('immigration_trigger_dag',
          default_args=default_args,
          concurrency=3,
          schedule_interval=None,
          description='Check if cluster is up, trigger immigration dag',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

check_cluster = ClusterCheckSensor(
    task_id="is_cluster_available",
    dag=dag,
    poke=60,
    emr=emr_conn,
)
def trigger_imigration_dag_with_context(context,dag_run_obj):
    return dag_run_obj

trigger_dag = TriggerDagRunOperator(
    task_id = "trigger_immigration_dag",
    trigger_dag_id = "immigration_etl_dag",
    python_callable=trigger_imigration_dag_with_context,
    dag= dag,
    provide_context=True
)

