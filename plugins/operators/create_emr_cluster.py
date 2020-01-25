from airflow.models import BaseOperator
import datetime
import boto3
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException

class CreateEMRClusterOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 region_name,
                 emr_connection,
                 cluster_name,
                 release_label='emr-5.9.0',
                 master_instance_type='m3.xlarge',
                 num_core_nodes=2,
                 core_node_instance_type='m3.2xlarge',
                 *args, **kwargs):

        super(CreateEMRClusterOperator, self).__init__(*args, **kwargs)
        self.region_name=region_name
        self.emr_connection = emr_connection
        self.num_core_nodes=num_core_nodes
        self.cluster_name = cluster_name
        self.master_instance_type=master_instance_type
        self.release_label=release_label
        self.core_node_instance_type=core_node_instance_type

    def get_security_group_id(self,group_name):
        ec2 = boto3.client('ec2', region_name=self.region_name)
        response = ec2.describe_security_groups(GroupNames=[group_name])
        return response['SecurityGroups'][0]['GroupId']

    def create_cluster(self):
        emr_master_security_group_id = self.get_security_group_id('AirflowEMRMasterSG')
        emr_slave_security_group_id = self.get_security_group_id('AirflowEMRSlaveSG')
        response = ""
        try:
            cluster_response = self.emr_connection.run_job_flow(
                Name='Airflow-' + self.cluster_name + "-" + str(datetime.datetime.utcnow()),
                ReleaseLabel=self.release_label,
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': "Master nodes",
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': self.master_instance_type,
                            'InstanceCount': 1
                        },
                        {
                            'Name': "Slave nodes",
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            'InstanceType': self.core_node_instance_type,
                            'InstanceCount': self.num_core_nodes
                        }
                    ],
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'Ec2KeyName': 'spark-cluster',
                    'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
                    'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
                },
                VisibleToAllUsers=True,
                JobFlowRole='EmrEc2InstanceProfile',
                ServiceRole='EmrRole',
                Applications=[
                    {'Name': 'hadoop'},
                    {'Name': 'spark'},
                    {'Name': 'hive'},
                    {'Name': 'livy'},
                    {'Name': 'zeppelin'}
                ]
            )
            response = cluster_response['JobFlowId']
        except Exception as e:
            self.logger.error("Could not create cluster",exc_info=True)
            raise AirflowException("Create cluster exception!")
        return response

    def execute(self, context):
        self.log.info("Creating EMR cluster cluster={0} at region={1}".format(self.cluster_name,self.region_name))
        self.log.info("EMR cluster number_of_nodes={0}".format(self.num_core_nodes))
        task_instance = context['task_instance']
        #commenting out create cluster calls in testing
        # cluster_id = self.create_cluster();
        cluster_id = "j-2PKDSSJM7ZNSG"
        task_instance.xcom_push('cluster_id', cluster_id)
        self.log.info("The newly create_cluster_id = {0}".format(cluster_id))
        return cluster_id

