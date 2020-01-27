from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

def get_cluster_status(emr, cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

class ClusterCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self,emr, *args, **kwargs):
        self.emr = emr
        return super(ClusterCheckSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        ti = context['ti']
        try:
            task_instance = context['task_instance']
            #clusterId = task_instance.xcom_pull('create_emr_cluster', key='cluster_id')
            clusterId = Variable.get("cluster_id")
            self.log.info("The cluster id from create_emr_cluster {0}".format(clusterId))
            status = get_cluster_status(self.emr, clusterId)
            self.log.info(status)
            if status in ['STARTING','RUNNING','BOOTSTRAPPING']:
                return False
            else:
                return True
        except Exception as e:
            self.log.info(e)
            return False
