from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from operators import (SubmitSparkJobToEmrOperator,ClusterCheckSensor,CreateEMRClusterOperator,TerminateEMRClusterOperator)
from airflow.operators.python_operator import PythonOperator

import boto3
from airflow import AirflowException
import logging

region_name="us-west-2"

emr_conn=None

s3 = boto3.resource('s3')

try:
    emr_conn = boto3.client('emr', region_name=region_name)
except Exception as e:
    logging.info(emr_conn)
    raise AirflowException("emr_connection fail!")

default_args = {
    'owner': 'decapstone-immigration',
    'start_date': datetime(2016,1,1,0,0,0,0),
    'end_date':datetime(2016,4,1,0,0,0,0),
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
          catchup=True,
          description='Load and transform data for immigration project',
          max_active_runs=1,
          schedule_interval="@monthly"
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
    num_core_nodes=3,
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


transform_immigration_data = SubmitSparkJobToEmrOperator(
    task_id="transform_immigration",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/immigration_data.py",
    kind="pyspark",
    logs=True
)
transform_immigration_city = SubmitSparkJobToEmrOperator(
    task_id="transform_immigration_city",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/immigration_by_city.py",
    kind="pyspark",
    logs=True
)

run_quality_checks = SubmitSparkJobToEmrOperator(
    task_id="run_quality_checks",
    dag=dag,
    emr_connection=emr_conn,
    file="/root/airflow/dags/transform/check_data_quality.py",
    kind="pyspark",
    logs=True
)

def check_s3_list_key(keys,bucket,**kwargs):
    capstone_bucket = s3.Bucket(bucket)

    for key in keys:
        objs = list(capstone_bucket.objects.filter(Prefix=key+"_SUCCESS"))
        print(objs)
        print(objs[0])
        if len(objs) == 0:
            raise ValueError("key {0} does not exist".format(key))


test_s3_hook = PythonOperator(
    task_id="s3_hook_list",
    python_callable=check_s3_list_key,
    provide_context=True,
    op_kwargs={
        'keys':["data/processed/weather/","data/processed/airports/","data/processed/city/","data/processed/immigration/","data/processed/immigrant/"],
        'bucket':"shwes3udacapstone"
    },
    dag=dag
)

terminate_cluster = TerminateEMRClusterOperator(
    task_id="terminate_cluster",
    dag=dag,
    trigger_rule="all_done",
    emr_connection=emr_conn
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> create_cluster >> check_cluster >> transform_i94codes_data
transform_i94codes_data >> [transform_weather_data,transform_airport_code, transform_demographics] >> transform_immigration_data
transform_immigration_data >> transform_immigration_city >> run_quality_checks >> test_s3_hook >> terminate_cluster >> end_operator
