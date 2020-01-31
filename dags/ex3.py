# -*- coding: utf-8 -*-


from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


args = {
    'owner': 'JDreijer',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise3',
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

timer_tasks = []
for timer in [1,5,10]:
    timer_tasks.append(
        BashOperator(
            task_id=f'wait_{timer}',
            bash_command=f'sleep {timer}',
            dag=dag
        )
    )

task_end = DummyOperator(task_id='task4', dag=dag)

task_start >> timer_tasks >> task_end

