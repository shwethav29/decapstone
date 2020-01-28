from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from operators import (SubmitSparkJobToEmrOperator)
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
#Initializing the Dag, to transform the data from the S3 using spark and create normalized datasets
dag = DAG('immigration_etl_dag',
          default_args=default_args,
          concurrency=3,
          schedule_interval=None,
          description='Load and transform data for immigration project',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

check_cluster = ExternalTaskSensor(task_id='check_cluster_ready_dag_sensor',
                                   external_dag_id = 'cluster_dag',
                                   external_task_id = 'check_cluster_waiting',
                                   dag=dag,
                                   execution_delta=timedelta(minutes=-30))

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

end_operator = DummyOperator(task_id='End_execution',  dag=dag)


start_operator >>  check_cluster >> transform_weather_data >> transform_i94codes_data >> end_operator

