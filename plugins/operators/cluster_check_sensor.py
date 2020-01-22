from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


def get_cluster_status(emr, cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

class ClusterCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self,emr,cluster_id, *args, **kwargs):
        self.emr = emr
        self.cluster_id = cluster_id
        return super(ClusterCheckSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        ti = context['ti']
        try:
            #cluster_id = Variable.get("cluster_id")
            #task_instance = self.kwargs[self.task_id]
            status = get_cluster_status(self.emr, self.cluster_id)
            self.log.info(status)
            if status == 'WAITING':
                return True
            else:
                return False
        except Exception as e:
            self.log.info(e)
            return False
