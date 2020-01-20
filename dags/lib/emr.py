import boto3, json, pprint, requests, textwrap, time, logging, requests
from datetime import datetime

def get_cluster_dns(emr,cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']


def wait_for_cluster_creation(emr,cluster_id):
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


def terminate_cluster(emr,cluster_id):
    emr.terminate_job_flows(JobFlowIds=[cluster_id])

def get_public_ip(emr,cluster_id):
    instances = emr.list_instances(ClusterId=cluster_id, InstanceGroupTypes=['MASTER'])
    return instances['Instances'][0]['PublicIpAddress']
