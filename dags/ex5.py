# -*- coding: utf-8 -*-

import datetime
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
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

t1 = PostgresToGoogleCloudStorageOperator(
    task_id='from_pg_to_gcs',
    postgres_conn_id='postgres_ex',
    sql='select * from land_registry_price_paid_uk LIMIT 5',
    bucket='test_bucket_airflow',
    filename='postgres_dump.csv',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    dag=dag
)

t1

# task_start = DummyOperator(task_id='start_task', dag=dag)
# task_start >> t1
