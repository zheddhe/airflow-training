import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import datetime

def successful_task():
    print('success')


def failed_task():
    raise Exception('This task did not work!')


def notify_retry(context):
    subject = f"[Custom] Retry alert: {context['task_instance']}"
    html_content = f"<p>Task {context['task_instance']} failed on try #{context['ti'].try_number}</p>"
    send_email(to=["airflow-email-dag@example.com"], subject=subject, html_content=html_content)


def notify_failure(context):
    subject = f"[Custom] Failure alert: {context['task_instance']}"
    html_content = f"<p>Task {context['task_instance']} finally failed after retries</p>"
    send_email(to=["airflow-email-dag@example.com"], subject=subject, html_content=html_content)


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
        # email_on_failure=True,
        email=['airflow-email-dag@example.com'],
        # on peut ajouter des callback mail additionnels spécifiques en plus des mail génériques (declenché presence "email=[],")
        # on_failure_callback=notify_failure,
        # on_retry_callback=notify_retry,
)
    # bonne pratique : teardown doit inclure du code idempotent (si re-exécuté : exemple rm -f, DROP IF EXIST)
    cleanup_task = PythonOperator(
        task_id='cleanup_task',
        python_callable=successful_task
    )

    # bonne pratique : code metier pas dans setup/teardown (a réserver pour initialisation et nettoyage)
    init_task.as_setup() >> mail_task >> cleanup_task.as_teardown()
