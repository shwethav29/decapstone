from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from operators import (CreateEMRClusterOperator,ClusterCheckSensor,CustomExternalTaskSensor,SubmitSparkJobToEmrOperator)
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
dag = DAG('cluster_dag',
          default_args=default_args,
          concurrency=3,
          schedule_interval=None,
          description='Create EMR cluster, wait for ETL to complete immigration transformation. Terminate cluster',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_cluster=CreateEMRClusterOperator(
    task_id = "create_emr_cluster",
    dag = dag,
    region_name=region_name,
    emr_connection=emr_conn,
    cluster_name="immigration_cluster",
    release_label='emr-5.9.0',
    master_instance_type='m3.xlarge',
    num_core_nodes=2,
    core_node_instance_type='m3.2xlarge'
)

check_cluster = ClusterCheckSensor(
    task_id="check_cluster_waiting",
    dag=dag,
    poke=60,
    emr=emr_conn,
)

transform_weather_data = SubmitSparkJobToEmrOperator(
    task_id="transform_weather_data",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/weather_data.py",
    kind="pyspark",
    logs=True
)

transform_i94codes_data = SubmitSparkJobToEmrOperator(
    task_id="transform_i94codes_data",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/i94_data_dictionary.py",
    kind="pyspark",
    logs=True
)

transform_airport_code = SubmitSparkJobToEmrOperator(
    task_id="transform_airport_code",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/airport_codes.py",
    kind="pyspark",
    logs=True
)

transform_demographics = SubmitSparkJobToEmrOperator(
    task_id="transform_demographics",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/demographics.py",
    kind="pyspark",
    logs=True
)


check_etl_complete = CustomExternalTaskSensor(task_id='check_etl_dag_sensor',
                                        external_dag_id = 'immigration_etl_dag',
                                        external_task_id = None,
                                        dag=dag,
                                        execution_delta=timedelta(hours=-5)
)


end_operator = DummyOperator(task_id='End_execution',  dag=dag)


start_operator >> create_cluster >> check_cluster >> transform_i94codes_data
transform_i94codes_data >> [transform_weather_data,transform_airport_code, transform_demographics] >> end_operator

