from airflow import DAG
from airflow.operators.bash import BashOperator

# DEPRECATED
def create_sub_dag(dag_id, schedule_interval, start_date):
    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        tags=['tutorial', 'datascientest'],
        default_args={
            'start_date': start_date
        }
    ) as my_sub_dag:

        task1 = BashOperator(
            bash_command="echo subdag task 1",
            task_id="my_sub_dag_task1",
        )

        task2 = BashOperator(
            bash_command="echo subdag task 2",
            task_id="my_sub_dag_task2",
        )

        task1 >> task2

        return my_sub_dag