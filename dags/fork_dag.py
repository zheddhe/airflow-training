import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

def successful_task():
    print('success')


def failed_task():
    raise Exception('This task did not work!')


def random_fail_task():
    random.seed()
    a = random.random() 
    if a < .5:
        raise Exception('This task randomly failed half of the time')

with DAG(
    dag_id='my_fork_dag',
    tags=['tutorial', 'datascientest'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1)
    },
    catchup=False
) as my_dag:

    init_task = PythonOperator(
        task_id='init_task',
        python_callable=random_fail_task
    )

    task1 = PythonOperator(
        task_id='task1',
        python_callable=random_fail_task
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=successful_task,
        trigger_rule='all_failed'
    )

    task3 = PythonOperator(
        task_id='task3',
        python_callable=successful_task,
        trigger_rule='all_success'
    )

    task4 = PythonOperator(
        task_id='task4',
        python_callable=random_fail_task,
        trigger_rule='all_done'
    )

    # bonne pratique : teardown doit inclure du code idempotent (si re-exécuté : exemple rm -f, DROP IF EXIST)
    cleanup_task = PythonOperator(
        task_id='cleanup_task',
        python_callable=successful_task
    )

    # bonne pratique : code metier pas dans setup/teardown (a réserver pour initialisation et nettoyage)
    init_task.as_setup() >> task1 >> [task2, task3] >> task4 >> cleanup_task.as_teardown()
