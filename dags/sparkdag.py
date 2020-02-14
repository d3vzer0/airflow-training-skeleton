# -*- coding: utf-8 -*-
import airflow
import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from operators.http_to_gcs import HttpToGcsOperator
import pendulum
from datetime import datetime

args = {
    'owner': 'JDreijer',
    'start_date': datetime(2019-11-26),
    'end_date': datetime(2019-11-28),
    'schedule_interval': '@daily'
}

dag = DAG(
    dag_id='sparkdag',
    default_args=args,
    schedule_interval= None,
    dagrun_timeout=timedelta(minutes=60),
)

t1 = PostgresToGoogleCloudStorageOperator(
    task_id='from_pg_to_gcs',
    postgres_conn_id='postgres_default',
    sql='select * from land_registry_price_paid_uk WHERE transfer_date = \'{{ ds }}\'',
    bucket='spark_bucket_jd',
    filename='postgres-transfers-{{ ds }}.csv',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    dag=dag
)

t2 = HttpToGcsOperator(
    task_id='from_api_to_gcs',
    endpoint='history?start_at={{ yesterday_ds }}&end_at={{ ds }}&symbols=EUR&base=GBP',
    gcs_path='api-exchangerate-{{ ds }}.json',
    gcs_bucket='spark_bucket_jd',
    http_conn_id='exchange_rates_api',
    dag=dag
)

t1 
t2 
