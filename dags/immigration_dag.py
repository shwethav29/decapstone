from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from operators import (CreateEMRClusterOperator,ClusterCheckSensor)
import boto3
from airflow import AirflowException
import logging

region_name="us-west2"
emr_connection=None
try:
    emr_connection = boto3.client('emr', region_name=region_name)
except Exception as e:
    logging.info(emr_connection)
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
#Initializing the Dag, to transform the data from the S3 using spark and create normalized datasets
dag = DAG('immigration_dag',
          default_args=default_args,
          concurrency=3,
          schedule_interval=None,
          description='Load and transform data for immigration project',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_cluster=CreateEMRClusterOperator(
    task_id = "create_emr_cluster",
    dag = dag,
    region_name=region_name,
    emr_connection=emr_connection,
    cluster_name="immigration_cluster",
    release_label='emr-5.9.0',
    master_instance_type='m3.xlarge',
    num_core_nodes=2,
    core_node_instance_type='m3.2xlarge'
)

check_cluster= ClusterCheckSensor(
    task_id="check_cluster_waiting",
    dag=dag,
    poke=60,
    emr=emr_connection,
    cluster_id="{{task_instance.xcom_pull(task_ids='previous_task_id')}}"
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> create_cluster >> check_cluster >> end_operator