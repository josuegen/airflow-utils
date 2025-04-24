from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def get_job_info_fn(**kwargs):
    bq_hook = BigQueryHook(location="us")
    records = bq_hook.get_records(
        sql="select body from cvs_poc_us.job_scan_config where job_id='abc'"
    )

    return [
        {
            "op_kwargs": {"query": record[0]},
            "show_return_value_in_logs": True
        } for record in list(records)
    ]


def run_bq_validations(query: str):
    bq_hook = BigQueryHook()
    bq_client = bq_hook.get_client()

    query_job = bq_client.query(query)
    # If the number of rows returned is long (>10 rows),
    # the preferred way is to store the result into a table

    # If the number of rows returned is small,
    # you can retrieve the values and pass them to the next task as a XCOM
    rows = query_job.result()
    print(rows)


default_args = {
    'owner': 'google',
    'depends_on_past': False,
    'email': ['josuegen@google.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
    dag_id='dynamic_task_mapping_1',
    start_date=None,
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["dynamic_task", "approach1"]
) as dag:

    get_job_info = PythonOperator(
        task_id='get_job_info',
        python_callable=get_job_info_fn
    )

    run_data_scan = PythonOperator.partial(
        task_id='run_validation',
        python_callable=run_bq_validations
    ).expand_kwargs(
        get_job_info.output
    )

    get_job_info >> run_data_scan
