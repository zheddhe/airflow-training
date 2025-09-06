from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.subdag import SubDagOperator
from airflow.operators.bash import BashOperator
# importing DAG generating function
from sub_dag import create_sub_dag

with DAG(
    dag_id="my_parent_dag",
    schedule_interval=None,
    tags=['tutorial', 'datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, 1)
    }
) as my_parent_dag:

    # NB : taskgroup à preferer au subdag qui sont DEPRECATED
    task1 = SubDagOperator(
        task_id="my_subdag",
        subdag=create_sub_dag(
            dag_id=my_parent_dag.dag_id + '.' + 'my_subdag',
            schedule_interval=my_parent_dag.schedule_interval,
            start_date=my_parent_dag.start_date),
    )


    task2 = BashOperator(
        task_id="bash_task",
        bash_command="echo hello world from parent",
    )

    task1 >> task2