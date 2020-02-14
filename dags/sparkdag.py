# -*- coding: utf-8 -*-
import airflow
import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator


args = {
    'owner': 'JDreijer',
    'start_date': airflow.utils.dates.days_ago(2),
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
    sql='select * from land_registry_price_paid_uk WHERE transfer_date="{{ ds }}"',
    bucket='spark_bucket_jd',
    filename='postgres-transfers-{{ ds }}.csv',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    dag=dag
)

t1
