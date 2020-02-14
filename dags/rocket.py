# -*- coding: utf-8 -*-

import datetime
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.get_rockets import LaunchLibraryOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import BranchPythonOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

# from airflow.operators.postgres_to_gcs_operator import 
# from airflow.providers.google.cloud.operators.postgres_to_gcs_operator import Postg


args = {
    'owner': 'JDreijer',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise5',
    default_args=args,
    schedule_interval= None,
    dagrun_timeout=timedelta(minutes=60),
)

t1 = LaunchLibraryOperator(
    task_id='get_rockets',
    launch_conn_id='launchlibrary',
    endpoint='/launch',
    params={"startdate":"{{ ds }}", "enddate": "{{ tomorrow_ds }}"},
    result_path='postgres_dump.csv',
    result_filename='google_cloud_storage_default',
    dag=dag
)

t1
