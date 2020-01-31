# -*- coding: utf-8 -*-

import datetime
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator


args = {
    'owner': 'JDreijer',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise4',
    default_args=args,
    schedule_interval= None,
    dagrun_timeout=timedelta(minutes=60),
)

def print_date(**context):
    print(context['execution_date'])

task_start = PythonOperator(
    task_id='print_execution_date',
    provide_context=True,
    python_callable=print_date,
    dag=dag
)

weekday_persons = {0: 'email_bob', 1: 'email_joe', 2:'email_alice', 3:'email_joey',
    4:'email_alice', 5:'email_alice', 6:'email_alice'}

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: weekday_persons[int(datetime.datetime.today().weekday())],
    dag=dag,
)

person_tasks = []
for person in weekday_persons.values():
     person_tasks.append(DummyOperator(task_id=person, dag=dag))

final_task = BashOperator(task_id='final_task', dag=dag, trigger_rule='one_success',
    bash_command='echo "Hoi"')

task_start >> branching >> person_tasks >> final_task

