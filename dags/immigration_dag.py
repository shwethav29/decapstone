from airflow import DAG
from datetime import datetime, timedelta
from plugins.operators import (CreateEMRClusterOperator)
import boto3
default_args = {
    'owner': 'decapstone-immigration',
    'start_date': datetime(2018,1,1),
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False,
    'provide_context': True
}
region_name="us-west2"
#Initializing the Dag, to transform the data from the S3 using spark and create normalized datasets
dag = DAG('immigration_dag',
          default_args=default_args,
          concurrency=3,
          schedule_interval=None,
          description='Load and transform data for immigration project',
        )
emr_connection = boto3.client('emr', region_name=region_name)

create_cluster=CreateEMRClusterOperator(
    task_id = "create_emr_cluster",
    dag = dag,
    region_name=region_name,
    cluster_name="immigration_cluster_"+datetime.utcnow(),
    release_label='emr-5.9.0',
    master_instance_type='m3.xlarge',
    num_core_nodes=2,
    core_node_instance_type='m3.2xlarge'
)