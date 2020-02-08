from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import boto3
from airflow import AirflowException
import logging

region_name="us-west-2"
s3 = boto3.resource('s3')
default_args = {
    'owner': 'decapstone-immigration',
    'start_date': datetime(2018,1,1),
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False
}
#Initializing the Dag, to transform the data from the S3 using spark and create normalized datasets
dag = DAG('test_s3_hook',
          default_args=default_args,
          concurrency=3,
          catchup=False,
          description='check s3 hook',
          max_active_runs=1,
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

def check_s3_list_key(keys,bucket):
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
    op_kwargs={
        'keys':["data/processed/weather/","data/processed/airports/","data/processed/city/","data/processed/immigration/","data/processed/immigrant/"],
        'bucket':"shwes3udacapstone"
    },
    dag=dag
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> test_s3_hook >> end_operator