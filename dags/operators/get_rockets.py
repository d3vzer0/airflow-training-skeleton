# from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

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

        gcs_hook = GoogleCloudStorageHook()
        gcs_hook.upload(bucket=self.result_bucket, filename=self.result_key, object=json_response)
        print(self.params)
        print(self.args)
        print(self.kwargs)
        print(self.result_key)
        print(self.result_bucket)
        # with open('')
        print(http_response)

        return http_response

# class LaunchLibraryPlugin(AirflowPlugin):
#     name = "launchlibrary_plugin"
#     operators = [LaunchLibraryOperator]
