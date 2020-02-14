# -*- coding: utf-8 -*-
import airflow
import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

from operators.http_to_gcs import HttpToGcsOperator
import pendulum
from datetime import datetime

args = {
    'owner': 'JDreijer',
    'start_date': datetime(2019, 11, 26),
    'end_date': datetime(2019, 11, 28),
    'schedule_interval': '@daily',
    'depends_on_past': False
}

dag = DAG(
    dag_id='sparkdag',
    default_args=args,
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

t3 = DataprocClusterCreateOperator(
    task_id='create_dataproc',
    cluster_name='analyse-pricing-{{ ds }}',
    project_id='afspfeb3-07be9fd3ffa2687ea1891',
    num_workers=2,
    zone='europe-west4-a',
    dag=dag
)

t4 = DataProcPySparkOperator(
    dag=dag,
    task_id='run_spark_job',
    main='gs://spark_bucket_jd/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    job_name='analyse-pricing'
)

tx = DataprocClusterDeleteOperator(
    dag=dag,
    task_id='delete_dataproc',
    cluster_name='analyse-pricing-{{ ds }}',
    project_id='afspfeb3-07be9fd3ffa2687ea1891'
)

[t1, t2] >> t3 >> t4 >> tx

