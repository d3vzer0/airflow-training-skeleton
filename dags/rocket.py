# -*- coding: utf-8 -*-

import datetime
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators import LaunchLibraryOperator

args = {
    'owner': 'JDreijer',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='launchlibdag',
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
