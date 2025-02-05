from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.alloy_db import AlloyDbHook

from google.cloud import alloydb_v1

from datetime import datetime, timedelta

default_args = {
    'owner': 'google',
    'depends_on_past': True,
    'email': ['josuegen@google.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'sla': timedelta(minutes=1)
}

dag = DAG(
    dag_id='alloydb_hook_dag',
    start_date=datetime(2025, 1, 1, 0, 0),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["alloydb", "hook"]
)

@task(task_id='alloydb_hook', dag=dag)
def perform_ops_with_hook():
    alloydb_hook = AlloyDbHook(
        gcp_conn_id = '',
        impersonation_chain = ''
    )
    
    alloydb_client = get_alloy_db_admin_client()

    """
    From here, you can execute whatever method is available for the client 
    https://cloud.google.com/python/docs/reference/alloydb/latest/google.cloud.alloydb_v1.services.alloy_db_admin.AlloyDBAdminClient#methods
    """

    # For example
    request = alloydb_v1.ExecuteSqlRequest(
        instance="instance_value",
        database="database_value",
        sql_statement="sql_statement_value",
    )

    alloydb_client.execute_sql(request=request)

    # Etc

perform_ops_with_hook()
