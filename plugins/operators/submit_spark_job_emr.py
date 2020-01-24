from airflow.models import BaseOperator
import datetime
import boto3
import requests
import json
import time
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException

class SubmitSparkJobToEmrOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 emr_connection,
                 file,
                 logs=False,
                 kind='spark',
                 *args, **kwargs):

        super(SubmitSparkJobToEmrOperator, self).__init__(*args, **kwargs)
        self.emr_connection = emr_connection
        self.file = file
        self.kind = kind
        self.logs= logs

    def get_cluster_dns(self,cluster_id):
        response = self.emr_connection.describe_cluster(ClusterId=cluster_id)
        return response['Cluster']['MasterPublicDnsName']

    def create_spark_session(self,master_dns):
        # 8998 is the port on which the Livy server runs
        host = 'http://' + master_dns + ':8998'
        data = {'kind': self.kind,
                "conf": {"spark.jars.packages": "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                         "spark.driver.extraJavaOptions": "-Dlog4jspark.root.logger=WARN,console"
                         }
                }
        headers = {'Content-Type': 'application/json'}
        response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
        self.log.info(response.json())
        return response.headers

    def wait_for_idle_session(self,master_dns, response_headers):
        # wait for the session to be idle or ready for job submission
        status = ''
        host = 'http://' + master_dns + ':8998'
        self.log.info(response_headers)
        session_url = host + response_headers['location']
        while status != 'idle':
            time.sleep(3)
            status_response = requests.get(session_url, headers=response_headers)
            status = status_response.json()['state']
            self.log.info('Session status: ' + status)
        return session_url

    def submit_statement(self, session_url, statement_path, args=''):
        statements_url = session_url + '/statements'
        with open(statement_path, 'r') as f:
            code = f.read()
        code = args + code
        data = {'code': code}
        response = requests.post(statements_url, data=json.dumps(data),
                                 headers={'Content-Type': 'application/json'})
        self.log.info(response.json())
        return response

    def track_statement_progress(self,master_dns, response_headers):
        statement_status = ''
        host = 'http://' + master_dns + ':8998'
        session_url = host + response_headers['location'].split('/statements', 1)[0]
        # Poll the status of the submitted scala code
        while statement_status != 'available':
            # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
            statement_url = host + response_headers['location']
            statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
            statement_status = statement_response.json()['state']
            self.log.info('Statement status: ' + statement_status)
            if 'progress' in statement_response.json():
                self.log.info('Progress: ' + str(statement_response.json()['progress']))
            time.sleep(10)
        final_statement_status = statement_response.json()['output']['status']
        if final_statement_status == 'error':
            self.log.info('Statement exception: ' + statement_response.json()['output']['evalue'])
            for trace in statement_response.json()['output']['traceback']:
                self.log.info(trace)
            raise ValueError('Final Statement Status: ' + final_statement_status)

        # Get the logs
        lines = requests.get(session_url + '/log',
                             headers={'Content-Type': 'application/json'}).json()['log']
        self.log.info('Final Statement Status: ' + final_statement_status)
        return lines

    def kill_spark_session(session_url):
        requests.delete(session_url, headers={'Content-Type': 'application/json'})

    def execute(self, context):
        self.log("Submitting the spark job file = {0}".format(self.file))
        task_instance = context['task_instance']
        clusterId = task_instance.xcom_pull('create_emr_cluster', key='cluster_id')
        cluster_dns = self.get_cluster_dns(clusterId)
        headers = self.create_spark_session(cluster_dns)
        session_url = self.wait_for_idle_session(cluster_dns,headers)
        statement_response = self.submit_statement(session_url,self.file)
        logs = self.track_statement_progress(cluster_dns,statement_response.headers)
        self.kill_spark_session(session_url)
        for line in logs:
            if 'FAIL' in str(line):
                self.logging.error(line)
                raise AirflowException("Failure is spark job for file {0}".format(self.file))
            else:
                self.log.info(line)

        self.log.info("complete the spark job for file {0}".format(self.file))
