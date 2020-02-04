from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from operators import (SubmitSparkJobToEmrOperator,ClusterCheckSensor)
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
    'start_date': datetime(2016,1,1),
    'end_date':datetime(2016,4,1),
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'schedule_interval': '@monthly',
    'email_on_retry':False,
    'provide_context': True
}
#Initializing the Dag, to transform the data from the S3 using spark and create normalized datasets
dag = DAG('immigration_etl_dag',
          default_args=default_args,
          concurrency=3,
          catchup=True,
          description='Load and transform data for immigration project',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

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

transform_immigration_data = SubmitSparkJobToEmrOperator(
    task_id="transform_immigration",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/immigration_data.py",
    kind="pyspark",
    logs=True
)
transform_immig_demo_weather_data = SubmitSparkJobToEmrOperator(
    task_id="transform_immigration",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/immigration_by_city.py",
    kind="pyspark",
    logs=True
)

run_quality_checks = SubmitSparkJobToEmrOperator(
    task_id="transform_immigration",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/check_data_quality.py",
    kind="pyspark",
    logs=True
)


end_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> check_cluster >> transform_i94codes_data
transform_i94codes_data >> [transform_weather_data,transform_airport_code, transform_demographics] >> transform_immigration_data
transform_immigration_data >> transform_immig_demo_weather_data >> run_quality_checks >> end_operator
