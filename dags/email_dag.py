import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

def successful_task():
    print('success')


def failed_task():
    raise Exception('This task did not work!')


with DAG(
    dag_id='email_dag',
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
        python_callable=successful_task
    )

    mail_task = PythonOperator(
        task_id='mail_task',
        python_callable=failed_task,
        retries=5,
		retry_delay=datetime.timedelta(seconds=10),
		email_on_retry=True,
		email=['airflow-email-dag@example.com']
)
    # bonne pratique : teardown doit inclure du code idempotent (si re-exécuté : exemple rm -f, DROP IF EXIST)
    cleanup_task = PythonOperator(
        task_id='cleanup_task',
        python_callable=successful_task
    )

    # bonne pratique : code metier pas dans setup/teardown (a réserver pour initialisation et nettoyage)
    init_task.as_setup() >> mail_task >> cleanup_task.as_teardown()
