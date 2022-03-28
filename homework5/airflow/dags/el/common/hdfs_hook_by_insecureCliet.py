from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from hdfs import InsecureClient

class HDFSHookWithInsecureClientException(AirflowException):
    pass

class HDFSHookWithInsecureClient(BaseHook):
    def __init__(self, hdfs_conn_id='hdfs_default', proxy_user=None):
        self.hdfs_conn_id = hdfs_conn_id
        self.proxy_user = proxy_user
    
    def get_conn(self):
        effective_user = self.proxy_user
        connections = self.get_connections(self.hdfs_conn_id)

        if not effective_user:
            effective_user = connections[0].login
        
        url = f'{connections[0].host}:{connections[0].port}'

        if len(connections) == 1:
            client = InsecureClient(url=url, user=effective_user)
        else:
            raise HDFSHookWithInsecureClientException(
                "conn_id doesn't exist in the repository")

        return client
