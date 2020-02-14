from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults

class LaunchLibraryOperator(BaseOperator):
    # template_fields = ['created_gt', 'created_lt', ]
    # template_ext = ()

    @apply_defaults
    def __init__(
            self,
            launch_conn_id: str,
            endpoint: str,
            params: dict,
            result_path: str,
            result_filename,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.launch_conn_id = launch_conn_id
        self.endpoint = endpoint
        self.params = params
        self.result_path = result_path
        self.result_filename = result_filename
        self.args = args
        self.kwargs = kwargs

    def execute(self, context):
        http_object = HttpHook('GET', http_conn_id=self.launch_conn_id)
        http_response = http_object.run(self.endpoint, data=self.params)
        return http_response

class LaunchLibraryPlugin(AirflowPlugin):
    name = "launchlibrary_plugin"
    operators = [LaunchLibraryOperator]
