from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from typing import Any
from datetime import datetime, timedelta


default_args = {
    'owner': 'Google',
    'depends_on_past': False,
    'email': ['josuegen@google.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    dag_id="insert_job_override",
    start_date=None,
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["BigQuery", "Override"]
)

class BigQueryInsertJobOperatorCVS(BigQueryInsertJobOperator):
    ui_color = "#fbb8bf"
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Any) -> None:
        state = "success"
        error_message = ""
        try:
            job_id = super().execute(context)
        except Exception as e:
            state = "failed"
            error_message = e
            raise AirflowException("Wrapped BigQuery Job failed.", e)
        finally:
            self.log.info("Producing adhoc XCOMs")
            task_instance = context["ti"]
            # State Xcom push
            task_instance.xcom_push(key="status", value=state)
            # End date Xcom push
            # This approach will capture the exit time (after retries, etc)
            task_instance.xcom_push(key="end_timestamp", value=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S%Z"))
            # Start date Xcom push
            task_instance.xcom_push(key="start_timestamp", value=task_instance.start_date.strftime("%Y-%m-%d %H:%M:%S%Z"))
            # Error message Xcom push
            task_instance.xcom_push(key="error_message", value=error_message)


insert_query_job = BigQueryInsertJobOperatorCVS(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": "select * from josuegen-gsd.cvs_poc.queries_dry_run_log",
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    dag=dag,
    retries=1,
    retry_delay=timedelta(seconds=10)
    
)

insert_query_job
