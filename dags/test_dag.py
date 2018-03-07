from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 6),
    'email': ['fstecconi@starflownetworks.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 1, 1),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    schedule_interval=timedelta(1)
)

fast_task = BashOperator(
    dag=dag,
    task_id='fast_task',
    bash_command='echo fast task',
)

slow_task_4 = BashOperator(
    dag=dag,
    task_id='slow_task_4',
    bash_command='echo -n sloo; sleep 4; echo oow task',
)

slow_task_6 = BashOperator(
    dag=dag,
    task_id='slow_task_6',
    bash_command='echo -n slooo; sleep 6; echo ooower task',
)

slow_task_4.set_downstream(fast_task)
slow_task_4.set_downstream(slow_task_6)