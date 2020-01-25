from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException

class TerminateEMRClusterOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 emr_connection,
                 *args, **kwargs):

        super(TerminateEMRClusterOperator, self).__init__(*args, **kwargs)
        self.emr_connection = emr_connection

    def terminate_cluster(self,clusterId):
        try:
            self.emr_connection.terminate_job_flows(JobFlowIds=[clusterId])
        except Exception as e:
            self.logger.error("Error deleting the cluster",exc_info=True)
            raise AirflowException("Failed to terminate the EMR cluster")

    def execute(self, context):
        task_instance = context['task_instance']
        clusterId = task_instance.xcom_pull('create_emr_cluster', key='cluster_id')
        self.log.info("Deleting the cluster EMR cluster cluster id={0}".format(clusterId))
        self.terminate_cluster(clusterId)
