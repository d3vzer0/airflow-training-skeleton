# -*- coding: utf-8 -*-


from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'JDreijer',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise1',
    default_args=args,
    schedule_interval= None,
    dagrun_timeout=timedelta(minutes=60),
)


t1 = DummyOperator(task_id='task1', dag=dag)
t2 = DummyOperator(task_id='task2', dag=dag)
t3 = DummyOperator(task_id='task3', dag=dag)
t4 = DummyOperator(task_id='task4', dag=dag)
t5 = DummyOperator(task_id='task5', dag=dag)

t1 >> t2 >> [t3,t4] >> t5
