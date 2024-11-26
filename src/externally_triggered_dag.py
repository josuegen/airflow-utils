from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'google',
    'depends_on_past': True,
    'email': ['josuegen@google.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    dag_id='externally_triggered_dag',
    start_date=None,
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["external", "triggered"]
)

some_task = BashOperator(
    task_id="some_task",
    bash_command="echo \"here is the conf passed: '$conf'\"",
    env={"conf": '{{ dag_run.conf if dag_run.conf else "" }}'},
    dag=dag
)

some_other_task = BashOperator(
    task_id="some_other_task",
    bash_command="sleep 60",
    dag=dag
)

some_task >> some_other_task
