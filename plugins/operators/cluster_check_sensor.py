from airflow.operators.sensors import BaseSensorOperator
from airflow.models import Variable

def get_cluster_status(emr, cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

class ClusterCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        return super(ClusterCheckSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        ti = context['ti']
        try:
            cluster_id = Variable.get("cluster_id")
            status = get_cluster_status(emr, cluster_id)
            self.log.info(status)
            if status == 'WAITING':
                return True
            else:
                return False
        except Exception as e:
            logging.info(e)
            return False
