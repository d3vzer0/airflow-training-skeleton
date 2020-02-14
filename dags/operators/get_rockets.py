# from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import json

class LaunchLibraryOperator(BaseOperator):
    template_fields = ['params', 'result_key']
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            launch_conn_id: str,
            endpoint: str,
            params: dict,
            result_key: str,
            result_bucket: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.launch_conn_id = launch_conn_id
        self.endpoint = endpoint
        self.params = params
        self.result_key = result_key
        self.result_bucket = result_bucket
        self.args = args
        self.kwargs = kwargs

    def execute(self, context):
        http_object = HttpHook('GET', http_conn_id=self.launch_conn_id)
        http_response = http_object.run(self.endpoint, data=self.params)
        json_response = http_response.json()
        with open('/tmp/rockets.json', 'w') as rocketsfile:
            rocketsfile.write(json.dumps(json_response))
    
        gcs_hook = GoogleCloudStorageHook()
        gcs_hook.upload(bucket=self.result_bucket, object=self.result_key, filename='/tmp/rockets.json')
        return http_response

# class LaunchLibraryPlugin(AirflowPlugin):
#     name = "launchlibrary_plugin"
#     operators = [LaunchLibraryOperator]
